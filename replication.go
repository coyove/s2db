package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/wal"
)

func (s *Server) writeWalCommand(i int) {
	for cmd := range s.db[i].walIn {
		last, err := s.db[i].wal.LastIndex()
		if err != nil {
			log.Error("wal: ", err)
			continue
		}
		ctr := last + 1
		if err := s.db[i].wal.Write(ctr, joinCommand(cmd...)); err != nil {
			// Basically we won't reach here as long as the filesystem is okay
			// otherwise we are totally screwed up
			log.Error("wal fatal: ", err)
		}
	}
	log.Info("#", i, " wal worker exited")
	s.db[i].walCloseSignal <- true
}

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
		masterWalIndex, err := s.db[shard].wal.LastIndex()
		if err != nil {
			if err != wal.ErrClosed {
				log.Error("#", shard, "read local wal index: ", err)
			}
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
		for i := slaveWalIndex + 1; i <= masterWalIndex; i++ {
			data, err := s.db[shard].wal.Read(i)
			if err != nil {
				log.Error("#", shard, " wal read #", i, ":", err)
				goto EXIT
			}
			cmds = append(cmds, string(data))
			sz += len(data)
			if len(cmds) == 200 || sz > 10*1024 {
				break
			}
		}

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
