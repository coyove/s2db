package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
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

func (s *Server) requestLogWorker(shard int) {
	ctx := context.TODO()
	buf := &bytes.Buffer{}
	dummy := redisproto.NewWriter(buf)

	defer func() {
		if r := recover(); r != nil {
			log.Error(r, string(debug.Stack()))
			go s.requestLogWorker(shard)
		}
	}()

	for !s.closed {
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
	s.db[shard].rdCloseSignal <- true
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
			if len(logs) == 200 || sz > 16*1024 {
				break
			}
		}
		return nil
	})
	return
}

func (s *Server) purgeLog(shard int, until uint64) (int, error) {
	start := time.Now()
	count := 0
	exit := false
AGAIN:
	if err := s.db[shard].Update(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("wal"))
		if bk == nil {
			exit = true
			return nil
		}
		if bk.Stats().KeyN == 0 {
			exit = true
			return nil
		}

		c := bk.Cursor()

		last, _ := c.Last()
		if until > 0 {
			last = intToBytes(uint64(until))
		}

		keys := [][]byte{}
		for k, _ := c.First(); len(k) == 8 && bytes.Compare(k, last) == -1; k, _ = c.Next() {
			keys = append(keys, k)
			if len(keys) == s.PurgeLogRun {
				break
			}
		}
		if len(keys) == 0 {
			exit = true
			return nil
		}
		for _, k := range keys {
			if err := bk.Delete(k); err != nil {
				return err
			}
			count++
		}
		return nil
	}); err != nil {
		return 0, err
	}
	if !exit && time.Since(start) < time.Duration(s.PurgeLogMaxRunTime)*time.Second {
		goto AGAIN
	}
	return count, nil
}

type slaves struct {
	sync.Mutex
	Slaves []Pair
}

type slaveInfo struct {
	KnownLogOffsets [ShardNum]uint64
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

func (s *slaves) Update(p Pair, shard int, logOffset uint64) {
	s.Lock()
	defer s.Unlock()

	found := false
	for i, sv := range s.Slaves {
		if sv.Key == p.Key {
			info := &slaveInfo{}
			json.Unmarshal(sv.Data, info)
			info.KnownLogOffsets[shard] = logOffset
			p.Data, _ = json.Marshal(info)
			s.Slaves[i] = p
			found = true
			break
		}
	}
	if !found {
		info := &slaveInfo{}
		info.KnownLogOffsets[shard] = logOffset
		p.Data, _ = json.Marshal(info)
		s.Slaves = append(s.Slaves, p)
	}
	sort.Slice(s.Slaves, func(i, j int) bool {
		return s.Slaves[i].Score > s.Slaves[j].Score
	})
}
