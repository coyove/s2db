package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/nj/typ"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) CompactShardAsync(shard int) error {
	out := make(chan int, 1)
	go s.compactShard(shard, out)
	if p := <-out; p != shard {
		return fmt.Errorf("wait previous compaction on shard%d", p)
	}
	return nil
}

func (s *Server) compactShard(shard int, out chan int) {
	log := log.WithField("shard", strconv.Itoa(shard))

	if v, ok := s.CompactLock.lock(int32(shard) + 1); !ok {
		out <- int(v - 1)
		log.Info("STAGE -1: previous compaction in the way #", v-1)
		return
	}
	out <- shard

	s.LocalStorage().Set("compact_lock", shard)
	defer func() {
		s.CompactLock.unlock()
		s.LocalStorage().Delete("compact_lock")
	}()

	x := &s.db[shard]
	s.runInspectFunc("compactonstart", shard)

	path := x.DB.Path()
	compactPath, dumpPath := path+".compact", path+".dump"
	if s.ServerConfig.CompactDumpTmpDir != "" {
		dumpPath = filepath.Join(s.CompactDumpTmpDir, "shard"+strconv.Itoa(shard)+".redir.dump")
	}

	log.Info("STAGE 0: begin compaction, compactDB=", compactPath, ", dumpDB=", dumpPath)

	// STAGE 1: dump the shard, open a temp database for compaction
	os.Remove(compactPath)
	os.Remove(dumpPath)

	dumpSize, err := x.DB.Dump(dumpPath)
	if err != nil {
		log.Error("dump DB: ", err)
		s.runInspectFunc("compactonerror", err)
		return
	}
	log.Info("STAGE 0: dump finished: ", dumpSize)

	compactDB, err := bbolt.Open(compactPath, 0666, bboltOptions)
	if err != nil {
		log.Error("open compactDB: ", err)
		s.runInspectFunc("compactonerror", err)
		return
	}
	dumpDB, err := bbolt.Open(dumpPath, 0666, bboltReadonlyOptions)
	if err != nil {
		log.Error("open dumpDB: ", err)
		s.runInspectFunc("compactonerror", err)
		return
	}
	if err := s.defragdb(shard, dumpDB, compactDB); err != nil {
		dumpDB.Close()
		compactDB.Close()
		log.Error("defragdb: ", err)
		s.runInspectFunc("compactonerror", err)
		return
	}
	dumpDB.Close()
	log.Infof("STAGE 1: point-in-time compaction finished, size=%d, removeDumpErr=%v", compactDB.Size(), os.Remove(dumpPath))

	// STAGE 2: for any changes happened during the compaction, write them into compactDB
	var ct, mt uint64
	for {
		if err = compactDB.View(func(tx *bbolt.Tx) error {
			if bk := tx.Bucket([]byte("wal")); bk != nil {
				if k, _ := bk.Cursor().Last(); len(k) == 8 {
					ct = binary.BigEndian.Uint64(k)
				}
			}
			return nil
		}); err != nil {
			log.Error("get compactDB tail: ", err)
			s.runInspectFunc("compactonerror", err)
			return
		}
		mt, err = s.myLogTail(shard)
		if err != nil {
			log.Error("get shard tail: ", err)
			s.runInspectFunc("compactonerror", err)
			return
		}
		if ct > mt {
			log.Errorf("fatal error: compactDB tail exceeds shard tail: %d>%d", ct, mt)
			s.runInspectFunc("compactonerror", err)
			return
		}
		if mt-ct <= uint64(s.CompactTxSize) {
			break // the gap is close enough, it is time to move on to the next stage
		}
		logs, err := s.responseLog(shard, ct+1, false)
		if err != nil {
			log.Error("responseLog: ", err)
			s.runInspectFunc("compactonerror", err)
			return
		}
		if _, err := runLog(logs, compactDB); err != nil {
			log.Error("runLog: ", err)
			s.runInspectFunc("compactonerror", err)
			return
		}
	}
	log.Infof("STAGE 2: incremental logs replayed, ct=%d, mt=%d, diff=%d, compactSize=%d", ct, mt, mt-ct, compactDB.Size())

	// STAGE 3: now compactDB almost (or already) catch up with onlineDB, we make onlineDB readonly so no more new changes can be made
	x.compactReplacing = true
	defer func() { x.compactReplacing = false }()

	x.DB.Close()
	roDB, err := bbolt.Open(path, 0666, bboltReadonlyOptions)
	if err != nil {
		// Worst case, this shard goes offline completely
		log.Error("CAUTION: open roDB: ", err)
		s.runInspectFunc("compactonerror", err)
		return
	}
	x.DB = roDB
	log.Info("STAGE 3: make online database rw -> ro")

	// STAGE 4: for any changes happened during STAGE 2+3 before readonly, write them to compactDB (should be few)
	logs, err := s.responseLog(shard, ct+1, true)
	if err != nil {
		log.Error("responseLog: ", err)
		s.runInspectFunc("compactonerror", err)
		return
	}
	if _, err := runLog(logs, compactDB); err != nil {
		log.Error("runLog: ", err)
		s.runInspectFunc("compactonerror", err)
		return
	}
	log.Infof("STAGE 4: final logs replayed, count=%d, size: %d>%d", len(logs), roDB.Size(), compactDB.Size())

	// STAGE 5: now compactDB and onlineDB are identical, swap them to make compactDB officially online
	roDB.Close()

	if s.CompactNoBackup == 1 {
		log.Info("CAUTION: compact no backup")
		if err := os.Remove(path); err != nil {
			log.Error("delete original (online) DB: ", err)
			s.runInspectFunc("compactonerror", err)
			return
		}
	} else {
		if err := os.Rename(path, path+".bak"); err != nil {
			log.Error("backup original (online) DB: ", err)
			s.runInspectFunc("compactonerror", err)
			return
		}
	}

	compactDB.Close()
	if err := os.Rename(compactPath, path); err != nil {
		log.Error("rename compactDB to online DB: ", err)
		s.runInspectFunc("compactonerror", err)
		return
	}

	db, err := bbolt.Open(path, 0666, bboltOptions)
	if err != nil {
		log.Error("open compactDB as online DB: ", err)
		s.runInspectFunc("compactonerror", err)
		return
	}
	x.DB = db

	log.Info("STAGE 5: swap compacted database to online")
	s.runInspectFunc("compactonfinish", shard)
}

func (s *Server) schedPurge() {
	out := make(chan int, 1)
	for !s.Closed {
		now := time.Now().UTC()
		if s.CompactJobType == 0 { // disabled
		} else if 100 <= s.CompactJobType && s.CompactJobType <= 123 { // start at exact hour per day
			if now.Hour() == s.CompactJobType-100 {
				key := "last_compact_1xx_ts"
				if last, _ := s.LocalStorage().GetInt64(key); now.Unix()-last < 86400 {
					log.Info("last_compact_1xx_ts: skipped")
				} else {
					for i := 0; i < ShardNum; i++ {
						log.Info("scheduleCompaction(", i, ")")
						s.compactShard(i, out)
						<-out
					}
					log.Info("update last_compact_1xx_ts: ", s.LocalStorage().Set(key, time.Now().Unix()))
				}
			}
		} else if 200 <= s.CompactJobType && s.CompactJobType <= 223 { // each shard starts at interval of 30min
			hr := s.CompactJobType - 200
			for idx, sh := range [16]int{
				(hr + 0) % 24, (hr + 1) % 24, (hr + 2) % 24, (hr + 3) % 24, (hr + 4) % 24, (hr + 5) % 24, (hr + 6) % 24, (hr + 7) % 24,
				(hr + 8) % 24, (hr + 9) % 24, (hr + 10) % 24, (hr + 11) % 24, (hr + 12) % 24, (hr + 13) % 24, (hr + 14) % 24, (hr + 15) % 24,
			} {
				if now.Hour() == sh {
					key := "last_compact_2xx_ts"
					if last, _ := s.LocalStorage().GetInt64(key); now.Unix()/1800-last < 1 {
						log.Info("last_compact_2xx_ts: skipped")
					} else {
						log.Info("update last_compact_2xx_ts: ", s.LocalStorage().Set(key, now.Unix()/1800))
						shardIdx := idx * 2
						if now.Minute() >= 30 {
							shardIdx++
						}
						log.Info("scheduleCompaction(", shardIdx, ")")
						s.compactShard(shardIdx, out)
						<-out
					}
				}
			}
		}

		// Do compaction on shard with large freelist
		maxFreelistSize := 0
		maxFreelistDB := -1
		for i := range s.db {
			sz := s.db[i].FreelistSize()
			if sz > maxFreelistSize {
				maxFreelistSize = sz
				maxFreelistDB = i
			}
		}
		if maxFreelistDB >= 0 && s.CompactFreelistLimit > 0 && maxFreelistSize > s.CompactFreelistLimit {
			log.Info("freelistCompaction(", maxFreelistDB, ")")
			s.compactShard(maxFreelistDB, out)
			<-out
		}

		time.Sleep(time.Minute)
	}
}

func (s *Server) startCronjobs() {
	run := func(d time.Duration) {
		for ; !s.Closed; time.Sleep(d - time.Second) {
			s.runInspectFunc("cronjob" + strconv.Itoa(int(d.Seconds())))
		}
	}
	go run(time.Second * 30)
	go run(time.Second * 60)
	go run(time.Second * 300)
}

func (s *Server) defragdb(shard int, odb, tmpdb *bbolt.DB) error {
	log := log.WithField("shard", strconv.Itoa(shard))

	tmp, err := getPendingUnlinks(odb)
	if err != nil {
		return err
	}
	unlinkp := make(map[string]bool, len(tmp))
	for _, n := range tmp {
		unlinkp[n] = true
	}

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

	var slaveMinWal uint64
	var useSlaveWal bool
	{
		var min uint64 = math.MaxUint64
		s.Slaves.Foreach(func(si *serverInfo) {
			if si.LogTails[shard] < min {
				min = si.LogTails[shard]
			}
		})
		if min != math.MaxUint64 {
			// If master have any slaves, it can't purge logs which slaves don't have yet
			// This is the best effort we can make because slaves maybe offline so it is still possible to over-purge
			slaveMinWal = min
			useSlaveWal = true
		} else if s.MasterMode {
			log.Info("STAGE 0.1: master mode: failed to collect info from slaves, no log compaction will be made")
			slaveMinWal = 0
			useSlaveWal = true
		}
	}

	count := 0
	total := 0
	queueDrops := 0
	for next, _ := c.First(); next != nil; next, _ = c.Next() {
		nextStr := string(next)
		if nextStr == "unlink" { // pending unlinks will be cleared during every compaction
			continue
		}
		isQueue := strings.HasPrefix(nextStr, "q.")
		if strings.HasPrefix(nextStr, "zset.score.") && unlinkp[string(next[11:])] ||
			strings.HasPrefix(nextStr, "zset.") && unlinkp[string(next[5:])] ||
			isQueue && unlinkp[string(next[2:])] {
			continue
		}

		queueTTL := 0
		if isQueue {
			res, err := s.runInspectFuncRet("queuettl", nextStr[2:])
			if err == nil && res.Type() == typ.Number {
				queueTTL = int(res.Int())
			}
		}

		b := tx.Bucket(next)
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %q", string(next))
		}

		tmpb, berr := tmptx.CreateBucketIfNotExists(next)
		if berr != nil {
			return berr
		}
		tmpb.FillPercent = 0.9 // for seq write in for each
		tmpb.SetSequence(b.Sequence())

		var walStartBuf []byte
		if nextStr == "wal" {
			walStart := decUint64(slaveMinWal, uint64(s.CompactLogHead))
			if !useSlaveWal {
				walStart = decUint64(b.Sequence(), uint64(s.CompactLogHead))
			} else if walStart >= b.Sequence() {
				log.Infof("STAGE 0.1: dumping took too long, slave logs surpass dumped logs: slave log: %d, log tail: %d", slaveMinWal, b.Sequence())
				walStart = decUint64(b.Sequence(), uint64(s.CompactLogHead))
			}
			log.Infof("STAGE 0.1: truncate logs using start: %d, slave tail: %d, log tail: %d", walStart, slaveMinWal, b.Sequence())
			walStartBuf = make([]byte, 8)
			binary.BigEndian.PutUint64(walStartBuf, walStart)
		}

		now := time.Now().UnixNano()
		if err = b.ForEach(func(k, v []byte) error {
			if len(walStartBuf) > 0 && bytes.Compare(k, walStartBuf) < 0 {
				return nil
			}
			if isQueue && len(k) == 16 && queueTTL > 0 {
				ts := int64(binary.BigEndian.Uint64(k[8:]))
				if (now-ts)/1e9 > int64(queueTTL) {
					queueDrops++
					return nil
				}
			}

			count++
			total++
			if count > s.CompactTxSize {
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

		if len(walStartBuf) > 0 {
			k, _ := tmpb.Cursor().Last()
			if len(k) != 8 {
				log.Infof("STAGE 0.2: truncate logs double check: buffer: %v, tail: %v, seq: %d, count: %d", walStartBuf, k, tmpb.Sequence(), total)
				return fmt.Errorf("FATAL")
			} else {
				log.Infof("STAGE 0.2: truncate logs double check: tail: %d, seq: %d, count: %d", binary.BigEndian.Uint64(k), tmpb.Sequence(), total)
			}
		}

		if isQueue {
			k, _ := tmpb.Cursor().Last()
			if len(k) == 0 {
				tmptx.DeleteBucket(next)
			}
		}
	}

	log.Infof("STAGE 0.3: queue drops: %d", queueDrops)
	return tmptx.Commit()
}

func decUint64(v uint64, d uint64) uint64 {
	if v > d {
		return v - d
	}
	return 0
}

func getPendingUnlinks(db *bbolt.DB) (names []string, err error) {
	if err := db.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("unlink"))
		if bk == nil {
			return nil
		}
		bk.ForEach(func(k, v []byte) error {
			if bytes.Equal(v, []byte("unlink")) {
				names = append(names, string(k))
			}
			return nil
		})
		return nil
	}); err != nil {
		return nil, err
	}
	return
}
