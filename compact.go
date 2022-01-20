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
	"sync"
	"sync/atomic"
	"time"

	"github.com/coyove/nj/typ"
	"github.com/coyove/s2db/internal"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) CompactShard(shard int, async bool) error {
	out := make(chan int, 1)
	if async {
		go s.compactShardImpl(shard, out)
	} else {
		s.compactShardImpl(shard, out)
	}
	if p := <-out; p != shard {
		return fmt.Errorf("wait previous compaction on shard%d", p)
	}
	return nil
}

func (s *Server) compactShardImpl(shard int, out chan int) {
	log := log.WithField("shard", strconv.Itoa(shard))

	if v, ok := s.CompactLock.Lock(shard); !ok {
		out <- v.(int)
		log.Info("STAGE -1: previous compaction in the way #", v)
		return
	}
	out <- shard

	s.LocalStorage().Set("compact_lock", shard)
	defer func() {
		s.CompactLock.Unlock()
		s.LocalStorage().Delete("compact_lock")
		internal.Recover()
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

	dumpSize, err := x.DB.Dump(dumpPath, s.DumpSafeMargin*1024*1024)
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
		log.Error("defragdb: ", err, " remove compactDB: ", os.Remove(compactPath))
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
	x.compactReplacing.Lock()
	defer x.compactReplacing.Unlock()

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

func (s *Server) schedCompactionJob() {
	out := make(chan int, 1)
	for !s.Closed {
		now := time.Now().UTC()
		if cjt := s.CompactJobType; cjt == 0 { // disabled
		} else if (100 <= cjt && cjt <= 123) || (10000 <= cjt && cjt <= 12359) { // start at exact time per day
			pass := cjt <= 123 && now.Hour() == cjt-100
			pass2 := cjt >= 10000 && now.Hour() == (cjt-10000)/100 && now.Minute() == (cjt-10000)%100
			if pass || pass2 {
				for i := 0; i < ShardNum; i++ {
					ts := now.Unix() / 86400
					key := fmt.Sprintf("last_compact_1xx_%d_ts", i)
					if last, _ := s.LocalStorage().GetInt64(key); ts-last < 1 {
						log.Info("last_compact_1xx_ts: skip #", i, " last=", time.Unix(last*86400, 0))
					} else {
						log.Info("scheduleCompaction(", i, ")")
						s.compactShardImpl(i, out)
						<-out
						log.Info("update last_compact_1xx_ts: ", i, " err=", s.LocalStorage().Set(key, ts))
					}
				}
			}
		} else if 200 <= cjt && cjt <= 223 { // even shards start at __:00, odd shards start at __:30
			hr := cjt - 200
			for idx := 0; idx < 16; idx++ {
				if now.Hour() == (hr+idx)%24 {
					shardIdx := idx * 2
					if now.Minute() >= 30 {
						shardIdx++
					}
					key := "last_compact_2xx_ts"
					ts := now.Unix() / 1800
					if last, _ := s.LocalStorage().GetInt64(key); ts-last < 1 {
						log.Info("last_compact_2xx_ts: skip #", shardIdx, " last=", time.Unix(last*1800, 0))
					} else {
						log.Info("scheduleCompaction(", shardIdx, ")")
						s.compactShardImpl(shardIdx, out)
						<-out
						log.Info("update last_compact_2xx_ts: ", shardIdx, " err=", s.LocalStorage().Set(key, ts))
					}
					break
				}
			}
		} else if cjt >= 600 && cjt <= 659 {
			offset := int64(cjt-600) * 60
			ts := (now.Unix() - offset) / 3600
			shardIdx := int(ts % ShardNum)
			key := "last_compact_6xx_ts"
			if last, _ := s.LocalStorage().GetInt64(key); ts-last < 1 {
				log.Info("last_compact_6xx_ts: skip #", shardIdx, " last=", time.Unix(last*3600+offset, 0))
			} else {
				log.Info("scheduleCompaction(", shardIdx, ")")
				s.compactShardImpl(shardIdx, out)
				<-out
				log.Info("update last_compact_6xx_ts: ", shardIdx, " err=", s.LocalStorage().Set(key, ts))
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
			s.compactShardImpl(maxFreelistDB, out)
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

	// open a tx on old db for read
	tx, err := odb.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	slaveMinLogtail, useSlaveLogtail := s.calcSlaveLogtail(shard)

	var total, queueDrops, queueDeletes int64

	tmptx, err := internal.CreateLimitedTx(tmpdb, s.CompactTxSize)
	if err != nil {
		return err
	}
	defer tmptx.Close()

	c := tx.Cursor()
	bucketIn := make(chan *internal.BucketWalker, 2*s.CompactTxWorkers)
	bucketWalkerWg := sync.WaitGroup{}
	bucketWalkerWg.Add(s.CompactTxWorkers)
	for i := 0; i < s.CompactTxWorkers; i++ {
		go func() {
			defer func() {
				bucketWalkerWg.Done()
				internal.Recover()
			}()
			for p := range bucketIn {
				s.compactionBucketWalker(p)
			}
		}()
	}

	for key, _ := c.First(); key != nil; key, _ = c.Next() {
		bucketName := string(key)
		isQueue := strings.HasPrefix(bucketName, "q.")
		isZSetScore := strings.HasPrefix(bucketName, "zset.score.")
		isZSet := !isZSetScore && strings.HasPrefix(bucketName, "zset.")

		// Drop unlinked buckets, "unlink" bucket itself will be dropped during every compaction
		if bucketName == "unlink" ||
			(isZSetScore && unlinkp[bucketName[11:]]) ||
			(isZSet && unlinkp[bucketName[5:]]) ||
			(isQueue && unlinkp[bucketName[2:]]) {
			continue
		}

		b := tx.Bucket(key)
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %q", string(key))
		}

		// Calculate queue TTL and WAL logs length if needed
		var queueTTL int
		var logtailStartBuf []byte
		if isQueue {
			res, err := s.runInspectFunc("queuettl", bucketName[2:])
			if err == nil && res.Type() == typ.Number {
				queueTTL = int(res.Int())
			}
		} else if bucketName == "wal" {
			logtailStart := decUint64(slaveMinLogtail, uint64(s.CompactLogHead))
			if !useSlaveLogtail {
				logtailStart = decUint64(b.Sequence(), uint64(s.CompactLogHead))
			} else if logtailStart >= b.Sequence() {
				log.Infof("STAGE 0.1: dumping took too long, slave logs surpass dumped logs: slave log: %d, log tail: %d", slaveMinLogtail, b.Sequence())
				logtailStart = decUint64(b.Sequence(), uint64(s.CompactLogHead))
			}
			log.Infof("STAGE 0.1: truncate logs using start: %d, slave tail: %d, log tail: %d", logtailStart, slaveMinLogtail, b.Sequence())
			logtailStartBuf = make([]byte, 8)
			binary.BigEndian.PutUint64(logtailStartBuf, logtailStart)
		}

		bucketIn <- &internal.BucketWalker{
			Bucket:          b,
			BucketName:      bucketName,
			Tx:              tmptx,
			QueueTTL:        queueTTL,
			LogtailStartBuf: logtailStartBuf,
			Total:           &total,
			QueueDrops:      &queueDrops,
			QueueDeletes:    &queueDeletes,
			Logger:          log,
		}
	}

	close(bucketIn)
	bucketWalkerWg.Wait()

	log.Infof("STAGE 0.3: queue drops: %d, queue deletes: %d, limited tx: %v", queueDrops, queueDeletes, tmptx.MapSize.MeanString())
	return tmptx.Finish()
}

func (s *Server) compactionBucketWalker(p *internal.BucketWalker) error {
	now := time.Now().UnixNano()
	isQueue := strings.HasPrefix(p.BucketName, "q.")
	if err := p.Bucket.ForEach(func(k, v []byte) error {
		// Truncate WAL logs
		if len(p.LogtailStartBuf) > 0 && bytes.Compare(k, p.LogtailStartBuf) < 0 {
			return nil
		}

		// Truncate queue
		if isQueue && len(k) == 16 && p.QueueTTL > 0 {
			ts := int64(binary.BigEndian.Uint64(k[8:]))
			if (now-ts)/1e9 > int64(p.QueueTTL) {
				atomic.AddInt64(p.QueueDrops, 1)
				return nil
			}
		}

		atomic.AddInt64(p.Total, 1)
		return p.Tx.Put(&internal.OnetimeLimitedTxPut{
			BkName: p.BucketName,
			Seq:    p.Bucket.Sequence(),
			Key:    k,
			Value:  v,
		})
	}); err != nil {
		return err
	}

	// Check compaction
	return p.Tx.Put(&internal.OnetimeLimitedTxPut{
		BkName: p.BucketName,
		Seq:    p.Bucket.Sequence(),
		Finishing: func(tx *bbolt.Tx, tmpb *bbolt.Bucket) error {
			if len(p.LogtailStartBuf) > 0 {
				k, _ := tmpb.Cursor().Last()
				if len(k) != 8 {
					p.Logger.Infof("STAGE 0.2: truncate logs double check: buffer: %v, tail: %v, seq: %d, count: %d", p.LogtailStartBuf, k, tmpb.Sequence(), p.Total)
				} else {
					p.Logger.Infof("STAGE 0.2: truncate logs double check: tail: %d, seq: %d, count: %d", binary.BigEndian.Uint64(k), tmpb.Sequence(), p.Total)
				}
			}
			if isQueue {
				if k, _ := tmpb.Cursor().Last(); len(k) == 0 {
					atomic.AddInt64(p.QueueDeletes, 1)
					tx.DeleteBucket([]byte(p.BucketName))
				}
			}
			// Done bucket compaction
			return nil
		},
	})
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

func (s *Server) calcSlaveLogtail(shard int) (slaveMinLogtail uint64, useSlaveLogtail bool) {
	var min uint64 = math.MaxUint64
	s.Slaves.Foreach(func(si *serverInfo) {
		if si.LogTails[shard] < min {
			min = si.LogTails[shard]
		}
	})
	if min != math.MaxUint64 {
		// If master have any slaves, it can't purge logs which slaves don't have yet
		// This is the best effort we can make because slaves maybe offline so it is still possible to over-purge
		slaveMinLogtail = min
		useSlaveLogtail = true
	} else if s.MasterMode {
		log.Infof("FATAL(master mode): failed to collect shard info #%d from slaves, no log compaction will be made", shard)
		slaveMinLogtail = 0
		useSlaveLogtail = true
	}
	return
}
