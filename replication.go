package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/secmask/go-redisproto"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) walProgress(shard int) (total uint64, err error) {
	f := func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("wal"))
		if bk != nil {
			k, _ := bk.Cursor().Last()
			if len(k) == 8 {
				total += binary.BigEndian.Uint64(k)
			}
		}
		return nil
	}
	if shard == -1 {
		for i := range s.db {
			if err := s.db[i].View(f); err != nil {
				return 0, err
			}
		}
	} else {
		if err := s.db[shard].View(f); err != nil {
			return 0, err
		}
	}
	return
}

func (s *Server) requestLogPuller(shard int) {
	ctx := context.TODO()
	buf := &bytes.Buffer{}
	dummy := redisproto.NewWriter(buf, log.StandardLogger())

	defer func() {
		if r := recover(); r != nil {
			log.Error(r, string(debug.Stack()))
			go s.requestLogPuller(shard)
		}
	}()

	for pinger := 0; !s.closed; pinger++ {
		if pinger%10 == 0 {
			cmd := redis.NewStringCmd(ctx, "PING", "FROM", s.ln.Addr().String(), s.ServerName, Version)
			s.rdb.Process(ctx, cmd)
			s.master = serverInfo{}
			parts := strings.Split(cmd.Val(), " ")
			if len(parts) == 3 {
				s.master.ServerName = parts[1]
				s.master.Version = parts[2]
			}
		}

		myWalIndex, err := s.walProgress(shard)
		if err != nil {
			log.Error("#", shard, " read local wal index: ", err)
			break
		}

		cmd := redis.NewStringSliceCmd(ctx, "REQUESTLOG", shard, myWalIndex+1)
		if err := s.rdb.Process(ctx, cmd); err != nil {
			if strings.Contains(err.Error(), "refused") {
				if shard == 0 {
					log.Error("#", shard, " master not alive")
				}
			} else if err != redis.Nil {
				log.Error("#", shard, " request log from master: ", err)
			}
			time.Sleep(time.Second * 2)
			continue
		}

		cmds := cmd.Val()
		if len(cmds) == 0 {
			time.Sleep(time.Second)
			continue
		}

		for _, x := range cmds {
			cmd, err := splitCommand(x)
			if err != nil {
				log.Error("bulkload: invalid payload: ", x)
				break
			}

			buf.Reset()
			s.runCommand(dummy, cmd, true)
			if buf.Len() > 0 && buf.Bytes()[0] == '-' {
				log.Error("bulkload: ", strings.TrimSpace(buf.String()[1:]))
				break
			}
		}

		time.Sleep(time.Second / 2)
	}

	log.Info("#", shard, " log replayer exited")
	s.db[shard].pullerCloseSignal <- true
}

func (s *Server) responseLog(shard int, start uint64) (logs []string, err error) {
	sz := 0
	masterWalIndex, err := s.walProgress(shard)
	if err != nil {
		return nil, err
	}
	if start == masterWalIndex+1 {
		return nil, nil
	}
	if start > masterWalIndex {
		return nil, fmt.Errorf("slave log (%d) surpass master log (%d)", start, masterWalIndex)
	}
	err = s.db[shard].View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("wal"))
		if bk == nil {
			return nil
		}

		k, _ := bk.Cursor().First()
		if len(k) == 8 {
			first := binary.BigEndian.Uint64(k)
			if first > start {
				return fmt.Errorf("master log (%d) truncated as slave request older log (%d)", first, start)
			}
		}

		for i := start; i <= masterWalIndex; i++ {
			data := bk.Get(intToBytes(uint64(i)))
			logs = append(logs, string(data))
			sz += len(data)
			if len(logs) >= s.ResponseLogRun || sz > s.ResponseLogSize*1024 {
				break
			}
		}
		return nil
	})
	return
}

func (s *Server) purgeLog(shard int, head uint64) (int, error) {
	if head <= 0 {
		return 0, fmt.Errorf("head is zero")
	}
	count := 0
	if err := s.db[shard].Update(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("wal"))
		if bk == nil {
			return nil
		}
		c := bk.Cursor()
		last, _ := c.Last()
		if len(last) != 8 {
			if bk.Stats().KeyN == 0 {
				return fmt.Errorf("nothing to purge")
			}
			return fmt.Errorf("invalid last key, fatal error")
		}
		tail := binary.BigEndian.Uint64(last)
		if head >= tail {
			return fmt.Errorf("truncate head over tail")
		}

		if tail-head > 10000 {
			return fmt.Errorf("too much gap, purging aborted")
		}

		keepLogs := [][2][]byte{}
		for i := head; i <= tail; i++ {
			k := intToBytes(i)
			v := append([]byte{}, bk.Get(k)...)
			keepLogs = append(keepLogs, [2][]byte{k, v})
			count++
		}
		if len(keepLogs) == 0 {
			return fmt.Errorf("keep zero logs, fatal error")
		}
		if err := tx.DeleteBucket([]byte("wal")); err != nil {
			return err
		}
		bk, err := tx.CreateBucket([]byte("wal"))
		if err != nil {
			return err
		}
		for _, p := range keepLogs {
			if err := bk.Put(p[0], p[1]); err != nil {
				return err
			}
		}
		return bk.SetSequence(tail)
	}); err != nil {
		return 0, err
	}
	return count, nil
}

type slaves struct {
	sync.Mutex
	Slaves []Pair
}

type serverInfo struct {
	KnownLogTails [ShardNum]uint64 `json:"logtails"`
	ListenAddr    string           `json:"listen"`
	ServerName    string           `json:"servername"`
	Version       string           `json:"version"`
}

func (s *slaves) Take(t time.Duration) []Pair {
	s.Lock()
	defer s.Unlock()

	now := time.Now()
	for i, sv := range s.Slaves {
		if now.Sub(time.Unix(int64(sv.Score), 0)) > t {
			return append([]Pair{}, s.Slaves[:i]...)
		}
	}
	return append([]Pair{}, s.Slaves...)
}

func (s *slaves) Update(ip string, update func(*serverInfo)) {
	p := Pair{Key: ip, Score: float64(time.Now().Unix())}

	s.Lock()
	defer s.Unlock()

	found := false
	for i, sv := range s.Slaves {
		if sv.Key == p.Key {
			info := &serverInfo{}
			json.Unmarshal(sv.Data, info)
			update(info)
			p.Data, _ = json.Marshal(info)
			s.Slaves[i] = p
			found = true
			break
		}
	}
	if !found {
		info := &serverInfo{}
		update(info)
		p.Data, _ = json.Marshal(info)
		s.Slaves = append(s.Slaves, p)
	}
	sort.Slice(s.Slaves, func(i, j int) bool {
		return s.Slaves[i].Score > s.Slaves[j].Score
	})
}

func (s *Server) compactShard(shard int) error {
	x := &s.db[shard]

	// Turn the shard into read only mode
	x.readonly = true
	for len(x.deferAdd) > 0 { // wait as-many-as-possible deferred ZAdds to finish
		runtime.Gosched()
	}

	old := x.DB.Path()
	path := old + ".bak"

	of, err := os.Create(path)
	if err != nil {
		return err // keep shard readonly until we find the cause
	}
	err = x.DB.Update(func(tx *bbolt.Tx) error {
		_, err = tx.WriteTo(of)
		return err
	})
	of.Close()
	if err != nil {
		return err // keep ...
	}

	if err := x.DB.Close(); err != nil {
		return err // keep ...
	}

	// Replace the database file on disk
	if err := os.Rename(old, old+time.Now().UTC().Format(".060102150405")); err != nil {
		return err // keep ...
	}
	if err := os.Rename(path, old); err != nil {
		return err // keep ...
	}

	// Reload new database
	db, err := bbolt.Open(old, 0666, bboltOptions)
	if err != nil {
		return err // keep ...
	}

	x.DB, x.readonly = db, false
	return nil
}
