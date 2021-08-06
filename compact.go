package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
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
	if err := s.defragdb(shard, x.DB, compactDB, s.CompactTxSize); err != nil {
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

func (s *Server) defragdb(shard int, odb, tmpdb *bbolt.DB, walSize int, limit int) error {
	// open a tx on tmpdb for writes
	tmptx, err := tmpdb.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tmptx.Rollback()
		}
	}()

	// open a tx on old db for read
	tx, err := odb.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	c := tx.Cursor()

	count := 0
	for next, _ := c.First(); next != nil; next, _ = c.Next() {
		b := tx.Bucket(next)
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %s", string(next))
		}

		tmpb, berr := tmptx.CreateBucketIfNotExists(next)
		if berr != nil {
			return berr
		}
		tmpb.FillPercent = 0.9 // for seq write in for each
		tmpb.SetSequence(b.Sequence())

		var walStartBuf []byte
		if string(next) == "wal" {
			var walStart uint64
			var min uint64 = math.MaxUint64
			for _, sv := range s.slaves.Take(time.Minute) {
				si := &serverInfo{}
				json.Unmarshal(sv.Data, si)
				if si.LogTails[shard] < min {
					min = si.LogTails[shard]
				}
			}
			if min != math.MaxUint64 {
				// If master have any slaves, it can't purge logs which slaves don't have yet
				// This is the best effort we can make because slaves maybe offline so it is still possible to over-purge
				walStart = decUint64(min, uint64(walSize))
			} else {
				walStart = decUint64(b.Sequence(), uint64(walSize))
			}
			log.Info("truncate logs using start: ", walStart)
			walStartBuf = make([]byte, 8)
			binary.BigEndian.PutUint64(walStartBuf, walStart)
		}

		if err = b.ForEach(func(k, v []byte) error {
			if len(walStartBuf) > 0 && bytes.Compare(k, walStartBuf) < 0 {
				return nil
			}
			count++
			if count > limit {
				err = tmptx.Commit()
				if err != nil {
					return err
				}
				tmptx, err = tmpdb.Begin(true)
				if err != nil {
					return err
				}
				tmpb = tmptx.Bucket(next)
				tmpb.FillPercent = 0.9 // for seq write in for each

				count = 0
			}
			return tmpb.Put(k, v)
		}); err != nil {
			return err
		}
	}

	return tmptx.Commit()
}

func decUint64(v uint64, d uint64) uint64 {
	if v > d {
		return v - d
	}
	return 0
}
