package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"gitlab.litatom.com/zhangzezhong/zset/calc"
	"go.etcd.io/bbolt"
)

func (s *Server) compactShard(shard int) error {
	log := log.WithField("shard", strconv.Itoa(shard))
	log.Info("STAGE 0: begin compaction")

	x := &s.db[shard]
	path := x.DB.Path()

	// STAGE 1: open a temp database for compaction
	os.Remove(path + ".compact")
	compactDB, err := bbolt.Open(path+".compact", 0666, bboltOptions)
	if err != nil {
		return err
	}
	if err := bbolt.Compact(compactDB, x.DB, int64(s.CompactTxSize)); err != nil {
		compactDB.Close()
		return err
	}
	log.Info("STAGE 1: point-in-time compaction finished, size: ", compactDB.Size())

	// STAGE 2: for any changes happened during the compaction, write them into compactDB
	compactTail := func() (tail uint64, err error) {
		err = compactDB.View(func(tx *bbolt.Tx) error {
			bk := tx.Bucket([]byte("wal"))
			if bk != nil {
				if k, _ := bk.Cursor().Last(); len(k) == 8 {
					tail = binary.BigEndian.Uint64(k)
				}
			}
			return nil
		})
		return
	}
	var ct, mt uint64
	for {
		ct, err = compactTail()
		if err != nil {
			return err
		}
		mt, err = s.myLogTail(shard)
		if err != nil {
			return err
		}
		if ct > mt {
			return fmt.Errorf("fatal error: compact tail exceeds shard tail: %d>%d", ct, mt)
		}
		if mt-ct <= uint64(s.CompactTxSize*2) {
			break // the gap is close enough, it is time to move on to the next stage
		}
		logs, err := s.responseLog(shard, ct+1)
		if err != nil {
			return err
		}
		if _, err := runLog(logs, compactDB); err != nil {
			return err
		}
	}
	log.Infof("STAGE 2: incremental logs replayed, ct=%d, mt=%d, diff=%d, size: %d", ct, mt, mt-ct, compactDB.Size())

	// STAGE 3: now compactDB almost (or already) catch up with onlineDB, we make onlineDB readonly so no more new changes can be made
	x.DB.Close()
	roDB, err := bbolt.Open(path, 0666, bboltReadonlyOptions)
	if err != nil {
		// Worst case, this shard goes offline completely
		return err
	}
	x.DB = roDB
	log.Info("STAGE 3: make online database rw -> ro")

	// STAGE 4: for any changes happened during STAGE 2+3 before readonly, write them to compactDB (should be few)
	logs, err := s.responseLog(shard, ct+1)
	if err != nil {
		return err
	}
	if _, err := runLog(logs, compactDB); err != nil {
		return err
	}
	log.Infof("STAGE 4: final logs replayed, count=%d, size: %d>%d", len(logs), roDB.Size(), compactDB.Size())

	// STAGE 5: now compactDB and onlineDB are identical, swap them to make compactDB officially online
	compactDB.Close()
	roDB.Close()

	if err := os.Remove(path); err != nil {
		return err
	}
	if err := os.Rename(path+".compact", path); err != nil {
		return err
	}

	db, err := bbolt.Open(path, 0666, bboltOptions)
	if err != nil {
		return err
	}
	x.DB = db

	log.Info("STAGE 5: swap compacted database to online")
	return nil
}

func (s *Server) schedPurge() {
	for !s.closed {
		if s.SchedPurgeJob == "" {
			time.Sleep(time.Minute)
			continue
		}

		oks := [ShardNum]bool{}
		for i := 0; i < ShardNum; i++ {
			ok, err := calc.Eval(s.SchedPurgeJob, 's', float64(i))
			if err != nil {
				log.Error("scheduled purgelog invalid job string: ", err)
			} else if ok != 0 {
				oks[i] = true
			}
		}
		for i, ok := range oks {
			if ok {
				log.Info("begin scheduled shard #", i, " purging")
				remains, oldCount, err := s.purgeLog(i, -int64(s.SchedPurgeHead))
				log.Info("scheduled purgelog shard ", i, " ", oldCount, ">", remains, " err=", err)
				log.Info("scheduled compact shard ", i, " err=", s.compactShard(i))
			}
		}
		time.Sleep(time.Minute)
	}
}
