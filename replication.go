package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) myLogTail(shard int) (total uint64, err error) {
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

func (s *Server) logDiff() (diff string, err error) {
	my, err := s.myLogTail(-1)
	if err != nil {
		return "", err
	}
	for _, p := range s.slaves.Take(time.Minute) {
		si := &serverInfo{}
		json.Unmarshal(p.Data, si)
		lt := int64(0)
		for _, t := range si.LogTails {
			lt += int64(t)
		}
		diff += fmt.Sprintf("%s:%d\r\n", p.Key, int64(my)-lt)
	}
	return
}

func (s *Server) requestLogPuller(shard int) {
	ctx := context.TODO()
	log := log.WithField("shard", strconv.Itoa(shard))

	defer func() {
		if r := recover(); r != nil {
			log.Error(r, " ", string(debug.Stack()))
			time.Sleep(time.Second)
			go s.requestLogPuller(shard)
		}
	}()

	for pinger := 0; !s.closed; pinger++ {
		if pinger%10 == 0 {
			cmd := redis.NewStringCmd(ctx, "PING", "FROM", s.ln.Addr().String(), s.ServerName, Version)
			s.rdb.Process(ctx, cmd)
			s.master = serverInfo{}
			parts := strings.Split(cmd.Val(), " ")
			if len(parts) != 3 {
				if cmd.Err() != nil && strings.Contains(cmd.Err().Error(), "refused") {
					if shard == 0 {
						log.Error("ping: master not alive")
					}
				} else {
					log.Error("ping: invalid response: ", cmd.Val(), cmd.Err())
				}
				pinger += 9
				time.Sleep(10 * time.Second)
				continue
			}
			s.master.ServerName = parts[1]
			s.master.Version = parts[2]
			if s.master.Version > Version {
				log.Error("ping: master version too high: ", s.master.Version, ">", Version)
				pinger += 9
				time.Sleep(10 * time.Second)
				continue
			}
		}

		myWalIndex, err := s.myLogTail(shard)
		if err != nil {
			log.Error("read local log index: ", err)
			break
		}

		cmd := redis.NewStringSliceCmd(ctx, "REQUESTLOG", shard, myWalIndex+1)
		if err := s.rdb.Process(ctx, cmd); err != nil {
			if strings.Contains(err.Error(), "refused") {
				if shard == 0 {
					log.Error("master not alive")
				}
			} else if err != redis.Nil {
				log.Error("request log from master: ", err)
			}
			time.Sleep(time.Second * 2)
			continue
		}

		cmds := cmd.Val()
		if len(cmds) == 0 {
			time.Sleep(time.Second)
			continue
		}

		var names []string
		start := time.Now()
		err = s.db[shard].Update(func(tx *bbolt.Tx) error {
			for _, x := range cmds {
				command, err := splitCommand(x)
				if err != nil {
					return fmt.Errorf("fatal: invalid payload: %q", x)
				}
				cmd := strings.ToUpper(command.Get(0))
				name := command.Get(1)
				switch cmd {
				case "DEL", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
					_, err = s.parseDel(cmd, name, command)(tx)
				case "ZADD":
					_, err = s.parseZAdd(cmd, name, command)(tx)
				case "ZINCRBY":
					_, err = s.parseZIncrBy(cmd, name, command)(tx)
				default:
					return fmt.Errorf("fatal: not a write command: %q", cmd)
				}
				if err != nil {
					log.Error("bulkload, error ocurred: ", cmd, " ", name)
					return err
				}
				names = append(names, name)
			}
			return nil
		})
		if err != nil {
			log.Error("bulkload: ", err)
		} else {
			for _, n := range names {
				s.removeCache(n)
			}
			s.survey.batchLatSv.Incr(time.Since(start).Milliseconds())
			s.survey.batchSizeSv.Incr(int64(len(names)))
		}
		time.Sleep(time.Second / 2)
	}

	log.Info("log replayer exited")
	s.db[shard].pullerCloseSignal <- true
}

func (s *Server) responseLog(shard int, start uint64) (logs []string, err error) {
	sz := 0
	masterWalIndex, err := s.myLogTail(shard)
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
	LogTails   [ShardNum]uint64 `json:"logtails"`
	ListenAddr string           `json:"listen"`
	ServerName string           `json:"servername"`
	Version    string           `json:"version"`
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
	for len(x.batchTx) > 0 { // wait batch worker to clear the queue
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
