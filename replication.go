package main

import (
	"bytes"
	"context"
	"fmt"
	"runtime/debug"
	"strings"
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
			s.runCommand(dummy, "", cmd, true)
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
	err = s.db[shard].View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("wal"))
		if bk == nil {
			return nil
		}
		masterWalIndex := uint64(bk.Stats().KeyN)
		if start == masterWalIndex+1 {
			return nil
		}
		if start > masterWalIndex {
			return fmt.Errorf("slave log (%d) surpass master log (%d)", start, masterWalIndex)
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
