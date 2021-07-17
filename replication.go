package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) readWalCommand(shard int, slaveAddr string) {
	ctx := context.TODO()

	for !s.closed {
		cmd := redis.NewIntCmd(ctx, "WALLAST", shard)
		err := s.rdb.Process(ctx, cmd)
		if err != nil && cmd.Err() != nil {
			log.Error("#", shard, " getting wal index from slave: ", slaveAddr, " err=", err)
			time.Sleep(time.Second * 5)
			continue
		}

		slaveWalIndex := uint64(cmd.Val())
		masterWalIndex, err := s.walProgress(shard)
		if err != nil {
			log.Error("#", shard, " read local wal index: ", err)
			goto EXIT
		}

		if slaveWalIndex == masterWalIndex {
			time.Sleep(time.Second)
			continue
		}

		if slaveWalIndex > masterWalIndex {
			log.Error("#", shard, " fatal: slave index surpass master index: ", slaveWalIndex, masterWalIndex)
			goto EXIT
		}

		cmds := []interface{}{"BULK", shard, slaveWalIndex + 1}
		sz := 0
		s.db[shard].View(func(tx *bbolt.Tx) error {
			bk := tx.Bucket([]byte("wal"))
			for i := slaveWalIndex + 1; i <= masterWalIndex; i++ {
				data := bk.Get(intToBytes(uint64(i)))
				cmds = append(cmds, string(data))
				sz += len(data)
				if len(cmds) == 200 || sz > 16*1024 {
					break
				}
			}
			return nil
		})

		cmd = redis.NewIntCmd(ctx, cmds...)
		if err := s.rdb.Process(ctx, cmd); err != nil || cmd.Err() != nil {
			if !strings.Contains(fmt.Sprint(err), "concurrent bulk write") {
				log.Error("#", shard, " slave bulk returned: ", err, " current master index: ", masterWalIndex)
			}
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second / 2)
	}

EXIT:
	log.Info("#", shard, " wal replayer exited")
	s.db[shard].rdCloseSignal <- true
}
