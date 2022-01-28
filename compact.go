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
	s2pkg "github.com/coyove/s2db/s2pkg"
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
	success := false

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
		s2pkg.Recover()
	}()

	x := &s.db[shard]
	s.runInspectFunc("compactonstart", shard)

	path := x.DB.Path()
	compactFilename := "shard" + strconv.Itoa(shard) + "." + fmt.Sprintf("%013d", time.Now().UnixNano()/1e6)
	compactPath := filepath.Join(s.DataPath, compactFilename)
	dumpPath := path + ".dump"
	if s.ServerConfig.CompactDumpTmpDir != "" {
		dumpPath = filepath.Join(s.CompactDumpTmpDir, "shard"+strconv.Itoa(shard)+".redir.dump")
	}
	defer func() {
		if !success {
			log.Infof("compaction failed, removeCompactErr=%v", s2pkg.RemoveFile(compactPath))
		}
	}()

	// STAGE 1: dump the shard, open a temp database for compaction
	log.Infof("STAGE 0: begin compaction, compactDB=%s, dumpDB=%s, removeOldDumpErr=%v",
		compactPath, dumpPath, s2pkg.RemoveFile(dumpPath))

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
		log.Errorf("open dumpDB: %v, closeCompactErr=%v", err, compactDB.Close())
		s.runInspectFunc("compactonerror", err)
		return
	}
	if err := s.defragdb(shard, dumpDB, compactDB); err != nil {
		log.Errorf("defragdb error: %v, closeDumpErr=%v, closeCompactErr=%v", err, dumpDB.Close(), compactDB.Close())
		s.runInspectFunc("compactonerror", err)
		return
	}
	log.Infof("STAGE 1: point-in-time compaction finished, size=%d, closeDumpErr=%v, removeDumpErr=%v",
		compactDB.Size(), dumpDB.Close(), s2pkg.RemoveFile(dumpPath))

	// STAGE 2: for any changes happened during the compaction, write them into compactDB
	var ct, mt uint64
	for first := 0; ; first++ {
		if err = compactDB.View(func(tx *bbolt.Tx) error {
			if bk := tx.Bucket([]byte("wal")); bk != nil {
				if k, _ := bk.Cursor().Last(); len(k) == 8 {
					ct = binary.BigEndian.Uint64(k)
				}
			}
			return nil
		}); err != nil {
			log.Errorf("get compactDB tail: %v, closeCompactErr=%v", err, compactDB.Close())
			s.runInspectFunc("compactonerror", err)
			return
		}
		mt, err = s.myLogTail(shard)
		if err != nil {
			log.Errorf("get shard tail: %v, closeCompactErr=%v", err, compactDB.Close())
			s.runInspectFunc("compactonerror", err)
			return
		}
		if ct > mt {
			log.Errorf("fatal error: compactDB tail exceeds shard tail: %d>%d, closeCompactErr=%v", ct, mt, compactDB.Close())
			s.runInspectFunc("compactonerror", err)
			return
		}
		if first%1000 == 0 {
			log.Infof("STAGE 1.5: chasing online (% 7d) ct=% 16d, mt=% 16d, diff=%d", first/1000, ct, mt, mt-ct)
		}
		if mt-ct <= uint64(s.ResponseLogRun) {
			break // the gap is close enough, it is time to move on to the next stage
		}

		logs, err := s.respondLog(shard, ct+1, false)
		if err != nil {
			log.Errorf("responseLog: %v, closeCompactErr=%v", err, compactDB.Close())
			s.runInspectFunc("compactonerror", err)
			return
		}
		if _, err := runLog(logs, compactDB); err != nil {
			log.Errorf("runLog: %v, closeCompactErr=%v", err, compactDB.Close())
			s.runInspectFunc("compactonerror", err)
			return
		}
	}
	log.Infof("STAGE 2: incremental logs replayed, ct=% 16d, mt=% 16d, diff=%d, compactSize=%d", ct, mt, mt-ct, compactDB.Size())

	finalStageReached := func() {}
	// STAGE 3: now compactDB almost (or already) catch up with onlineDB, we make onlineDB readonly so no more new changes can be made
	x.compactLocker.Lock(func() { log.Info("compactor is waiting for runner/bulkload") })
	defer func() {
		x.compactLocker.Unlock()
		finalStageReached()
	}()
	log.Info("STAGE 3: onlineDB write lock acquired")

	// STAGE 4: for any changes happened during STAGE 2+3 before readonly, write them to compactDB (should be few)
	logs, err := s.respondLog(shard, ct+1, true)
	if err != nil {
		log.Errorf("responseLog: %v, closeCompactErr=%v", err, compactDB.Close())
		s.runInspectFunc("compactonerror", err)
		return
	}
	if _, err := runLog(logs, compactDB); err != nil {
		log.Errorf("runLog: %v, closeCompactErr=%v", err, compactDB.Close())
		s.runInspectFunc("compactonerror", err)
		return
	}
	log.Infof("STAGE 4: final logs replayed, count=%d, size: %d>%d", len(logs), x.DB.Size(), compactDB.Size())

	// STAGE 5: now compactDB and onlineDB are identical, time to make compactDB officially online
	if err := s.UpdateShardFilename(shard, compactFilename); err != nil {
		log.Errorf("update shard filename: %v, closeCompactErr=%v", err, compactDB.Close())
		s.runInspectFunc("compactonerror", err)
		return
	}

	old := x.DB
	x.DB = compactDB
	finalStageReached = func() {
		bakPath := filepath.Join(s.DataPath, "shard"+strconv.Itoa(shard)+".bak")
		log.Infof("STAGE 5: swap compacted database to online, closeOldErr=%v, removeBakErr=%v, renameOldErr=%v",
			old.Close(), s2pkg.RemoveFile(bakPath), os.Rename(path, bakPath))
		if s.CompactNoBackup == 1 {
			log.Infof("STAGE 5.1: CAUTION delete previous backup file: %v", s2pkg.RemoveFile(bakPath))
		}
		s.runInspectFunc("compactonfinish", shard)
	}
	success = true
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
						log.Info("update last_compact_1xx_ts: #", i, " err=", s.LocalStorage().Set(key, ts))
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
						log.Info("update last_compact_2xx_ts: #", shardIdx, " err=", s.LocalStorage().Set(key, ts))
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
				log.Info("update last_compact_6xx_ts: #", shardIdx, " err=", s.LocalStorage().Set(key, ts))
			}
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

	tmptx, err := s2pkg.CreateLimitedTx(tmpdb, s.CompactTxSize)
	if err != nil {
		return err
	}
	defer tmptx.Close()

	c := tx.Cursor()
	bucketIn := make(chan *s2pkg.BucketWalker, 2*s.CompactTxWorkers)
	bucketWalkerWg := sync.WaitGroup{}
	bucketWalkerWg.Add(s.CompactTxWorkers)
	for i := 0; i < s.CompactTxWorkers; i++ {
		go func() {
			defer func() {
				bucketWalkerWg.Done()
				s2pkg.Recover()
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

		bucketIn <- &s2pkg.BucketWalker{
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

func (s *Server) compactionBucketWalker(p *s2pkg.BucketWalker) error {
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
		return p.Tx.Put(&s2pkg.OnetimeLimitedTxPut{
			BkName: p.BucketName,
			Seq:    p.Bucket.Sequence(),
			Key:    k,
			Value:  v,
		})
	}); err != nil {
		return err
	}

	// Check compaction
	return p.Tx.Put(&s2pkg.OnetimeLimitedTxPut{
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
