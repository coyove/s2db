package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

const (
	QueueTTLKey = "_queuettl_"
	ZSetTTLKey  = "_zsetttl_"
)

func (s *Server) DumpShard(shard int, path string) (int64, error) {
	return s.db[shard].DB.Dump(path, s.DumpSafeMargin*1024*1024)
}

func (s *Server) CompactShard(shard int, remapDir string, async bool) error {
	out := make(chan int, 1)
	if async {
		go s.compactShardImpl(shard, remapDir, out)
	} else {
		s.compactShardImpl(shard, remapDir, out)
	}
	if p := <-out; p != shard {
		return fmt.Errorf("wait previous compaction on shard%d", p)
	}
	return nil
}

func (s *Server) compactShardImpl(shard int, remapShardDir string, out chan int) {
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
		s2pkg.Recover(nil)
	}()

	x := &s.db[shard]
	s.runScriptFunc("compactonstart", shard)

	path := x.DB.Path()
	shardDir, _, err := s.GetShardFilename(shard)
	if err != nil {
		log.Error("get shard dir: ", err)
		s.runScriptFunc("compactonerror", shard, err)
		return
	}
	if remapShardDir != "" && remapShardDir != shardDir {
		log.Infof("STAGE X: compaction remapping: from %q to %q, mkdir=%v", shardDir, remapShardDir, os.MkdirAll(remapShardDir, 0777))
		shardDir = remapShardDir
	}

	compactFilename := makeShardFilename(shard)
	compactPath := filepath.Join(shardDir, compactFilename)
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
		s.runScriptFunc("compactonerror", shard, err)
		return
	}
	log.Info("STAGE 0: dump finished: ", dumpSize)

	compactDB, err := bbolt.Open(compactPath, 0666, DBOptions)
	if err != nil {
		log.Error("open compactDB: ", err)
		s.runScriptFunc("compactonerror", shard, err)
		return
	}
	dumpDB, err := bbolt.Open(dumpPath, 0666, DBReadonlyOptions)
	if err != nil {
		log.Errorf("open dumpDB: %v, closeCompactErr=%v", err, compactDB.Close())
		s.runScriptFunc("compactonerror", shard, err)
		return
	}
	if err := s.defragdb(shard, dumpDB, compactDB); err != nil {
		log.Errorf("defragdb error: %v, closeDumpErr=%v, closeCompactErr=%v", err, dumpDB.Close(), compactDB.Close())
		s.runScriptFunc("compactonerror", shard, err)
		return
	}
	log.Infof("STAGE 1: point-in-time compaction finished, size=%d, closeDumpErr=%v, removeDumpErr=%v",
		compactDB.Size(), dumpDB.Close(), s2pkg.RemoveFile(dumpPath))

	// STAGE 2: for any changes happened during the compaction, write them into compactDB
	var ct, mt uint64
	for first := 0; ; first++ {
		if err = compactDB.View(func(tx *bbolt.Tx) error {
			if bk := tx.Bucket([]byte("wal")); bk != nil {
				ct = bk.Sequence()
			}
			return nil
		}); err != nil {
			log.Errorf("get compactDB tail: %v, closeCompactErr=%v", err, compactDB.Close())
			s.runScriptFunc("compactonerror", shard, err)
			return
		}
		mt, err = s.ShardLogtail(shard)
		if err != nil {
			log.Errorf("get shard tail: %v, closeCompactErr=%v", err, compactDB.Close())
			s.runScriptFunc("compactonerror", shard, err)
			return
		}
		if ct > mt {
			log.Errorf("fatal error: compactDB tail exceeds shard tail: %d>%d, closeCompactErr=%v", ct, mt, compactDB.Close())
			s.runScriptFunc("compactonerror", shard, err)
			return
		}
		if first%1000 == 0 {
			log.Infof("STAGE 1.5: chasing online (% 7d) ct=% 16d, mt=% 16d, diff=%d", first/1000, ct, mt, mt-ct)
		}
		if mt-ct <= uint64(s.ResponseLogRun/2+1) {
			break // the gap is close enough, it is time to move on to the next stage
		}

		logs, logprevHash, err := s.respondLog(shard, ct+1, false)
		if err != nil {
			log.Errorf("respondLog: %v, closeCompactErr=%v", err, compactDB.Close())
			s.runScriptFunc("compactonerror", shard, err)
			return
		}
		if len(logs) == 0 {
			log.Errorf("fatal: respondLog returned empty logs, closeCompactErr=%v", compactDB.Close())
			return
		}
		if _, _, err := runLog(ct+1, logprevHash, logs, compactDB); err != nil {
			log.Errorf("runLog: %v, closeCompactErr=%v", err, compactDB.Close())
			s.runScriptFunc("compactonerror", shard, err)
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
	logs, logprevHash, err := s.respondLog(shard, ct+1, true)
	if err != nil {
		log.Errorf("responseLog: %v, closeCompactErr=%v", err, compactDB.Close())
		s.runScriptFunc("compactonerror", shard, err)
		return
	}
	if len(logs) > 0 {
		if _, _, err := runLog(ct+1, logprevHash, logs, compactDB); err != nil {
			log.Errorf("runLog: %v, closeCompactErr=%v", err, compactDB.Close())
			s.runScriptFunc("compactonerror", shard, err)
			return
		}
	}
	log.Infof("STAGE 4: final logs replayed, count=%d, size: %d>%d", len(logs), x.DB.Size(), compactDB.Size())

	// STAGE 5: now compactDB and onlineDB are identical, time to make compactDB officially online
	if err := s.UpdateShardFilename(shard, shardDir, compactFilename); err != nil {
		log.Errorf("update shard filename: %v, closeCompactErr=%v", err, compactDB.Close())
		s.runScriptFunc("compactonerror", shard, err)
		return
	}

	old := x.DB
	x.DB = compactDB
	finalStageReached = func() {
		go func() {
			for i := 0; i < ShardNum; i++ {
				if i == (shard-1+ShardNum)%ShardNum { // preserve last 2 shard backups
					continue
				}
				oldBakPath := filepath.Join(shardDir, "shard"+strconv.Itoa(i)+".bak")
				s.runScriptFunc("compactondeletebackup", shard, oldBakPath)
				if err := s2pkg.RemoveFile(oldBakPath); err != nil {
					log.Errorf("STAGE 6: delete old backup file: %v, err=%v", oldBakPath, err)
				}
			}
			bakPath := filepath.Join(shardDir, "shard"+strconv.Itoa(shard)+".bak")
			log.Infof("STAGE 5: swap compacted database to online, closeOldErr=%v, renameOldErr=%v",
				old.Close(), os.Rename(path, bakPath))
		}()
		s.runScriptFunc("compactonfinish", shard)
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
						s.compactShardImpl(i, "", out)
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
						s.compactShardImpl(shardIdx, "", out)
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
				s.compactShardImpl(shardIdx, "", out)
				<-out
				log.Info("update last_compact_6xx_ts: #", shardIdx, " err=", s.LocalStorage().Set(key, ts))
			}
		} else {
			log.Info("compactor: no job found")
		}

		time.Sleep(time.Minute)
	}
}

func (s *Server) startCronjobs() {
	var run func(time.Duration, bool)
	run = func(d time.Duration, m bool) {
		if s.Closed {
			return
		}
		time.AfterFunc(d, func() { run(d, m) })
		if m {
			if s.DisableMetrics != 1 {
				if err := s.appendMetricsPairs(time.Hour * 24 * 30); err != nil {
					log.Error("AppendMetricsPairs: ", err)
				}
			}
		} else {
			s.runScriptFunc("cronjob" + strconv.Itoa(int(d.Seconds())))
		}
	}
	run(time.Second*30, false)
	run(time.Second*60, false)
	run(time.Second*60, true)
	run(time.Second*300, false)
}

func (s *Server) defragdb(shard int, odb, tmpdb *bbolt.DB) error {
	log := log.WithField("shard", strconv.Itoa(shard))
	nanots := time.Now().UnixNano()

	unlinksKey := getPendingUnlinksKey(shard)
	unlinks, _ := s.ZRange(true, unlinksKey, 0, -1, redisproto.Flags{LIMIT: s2pkg.RangeHardLimit})
	unlinkp := make(map[string]bool, len(unlinks))
	for _, n := range unlinks {
		unlinkp[n.Member] = true
	}
	unlinkp[unlinksKey] = true // "unlinks" key itself will also be unlinked

	queueTTLs, _ := s.ZRange(false, QueueTTLKey, 0, -1, redisproto.Flags{LIMIT: s2pkg.RangeHardLimit})
	zsetTTLs, _ := s.ZRange(false, ZSetTTLKey, 0, -1, redisproto.Flags{LIMIT: s2pkg.RangeHardLimit})

	tx, err := odb.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// As being master, server can't purge logs which slave doesn't have yet.
	// This is not foolproof because slave maybe temporarily offline, so it is still possible to over-purge.
	hasSlave := s.Slave.Redis() != nil && s.Slave.IsAcked(s)
	slaveLogtail := s.Slave.Logtails[shard]

	var total, totalBuckets, unlinksDrops, queueDrops, queueDeletes int64
	var zsetDrops, zsetDeletes, zsetScoreDrops, zsetScoreDeletes, zsetCardFix int64
	var keysDistribution s2pkg.LogSurvey

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
				s2pkg.Recover(nil)
			}()
			for p := range bucketIn {
				s.compactionBucketWalker(p)
			}
		}()
	}

	for key, _ := c.First(); key != nil; key, _ = c.Next() {
		bucketName := string(key)
		keyName := bucketName
		isQueue := strings.HasPrefix(bucketName, "q.")
		isZSetScore := strings.HasPrefix(bucketName, "zset.score.")
		isZSet := !isZSetScore && strings.HasPrefix(bucketName, "zset.")

		if isQueue {
			keyName = keyName[2:]
		} else if isZSetScore {
			keyName = keyName[11:]
		} else if isZSet {
			keyName = keyName[5:]
		}

		// Drop unlinked buckets
		if unlinkp[keyName] {
			if !isZSetScore { // zsets have 2 buckets, we count only one of them
				s.removeCache(keyName)
				unlinksDrops++
			}
			continue
		}

		b := tx.Bucket(key)
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %q", string(key))
		}

		// Calculate queue TTL and WAL logs length if needed
		var queueTTL int = -1
		var zsetTTL int = -1
		var logtailStartBuf []byte
		if isQueue {
			queueTTL = getTTLByName(queueTTLs, keyName)
		} else if isZSet || isZSetScore {
			zsetTTL = getTTLByName(zsetTTLs, keyName)
		} else if bucketName == "wal" {
			logtailStart := decUint64(slaveLogtail, uint64(s.CompactLogHead))
			if !hasSlave {
				logtailStart = decUint64(b.Sequence(), uint64(s.CompactLogHead))
			} else if logtailStart >= b.Sequence() {
				log.Infof("STAGE 0.1: dumping took too long, slave log (%d) surpass local log (%d)", slaveLogtail, b.Sequence())
				logtailStart = decUint64(b.Sequence(), uint64(s.CompactLogHead))
			}
			log.Infof("STAGE 0.1: truncate logs before %d, fact check: slave log: %d(%v), local log: %d, log head: %d",
				logtailStart, slaveLogtail, hasSlave, b.Sequence(), s.CompactLogHead)
			logtailStartBuf = s2pkg.Uint64ToBytes(logtailStart)
		}

		bucketIn <- &s2pkg.BucketWalker{
			UnixNano:        nanots,
			Bucket:          b,
			BucketName:      bucketName,
			KeyName:         keyName,
			Tx:              tmptx,
			QueueTTL:        queueTTL,
			ZSetTTL:         zsetTTL,
			LogtailStartBuf: logtailStartBuf,
			// Metrics
			Total: &total, QueueDrops: &queueDrops, QueueDeletes: &queueDeletes,
			ZSetDrops: &zsetDrops, ZSetDeletes: &zsetDeletes,
			ZSetScoreDrops: &zsetScoreDrops, ZSetScoreDeletes: &zsetScoreDeletes,
			ZSetCardFix: &zsetCardFix,
			KeysDist:    &keysDistribution,
			Logger:      log,
		}
		totalBuckets++
	}

	close(bucketIn)
	bucketWalkerWg.Wait()

	log.Infof("STAGE 0.3: buckets: %d, unlinks: %d/%d, limited tx: %v", totalBuckets, unlinksDrops, len(unlinkp), tmptx.MapSize.MeanString())
	log.Infof("STAGE 0.4: qttls:%d, qdrops: %d, qdeletes: %d", len(queueTTLs), queueDrops, queueDeletes)
	log.Infof("STAGE 0.5: zttls:%d, zdrops: %d/%d, zdeletes: %d/%d, zfixs: %d",
		len(zsetTTLs), zsetDrops, zsetScoreDrops, zsetDeletes, zsetScoreDeletes, zsetCardFix)

	if zsetDrops != zsetScoreDrops || zsetDeletes != zsetScoreDeletes {
		return fmt.Errorf("zset ttl: numbers mismatched")
	}

	for i, s := range keysDistribution {
		if s[0] > 0 && s[1] > 0 {
			log.Infof("STAGE 0.6: bucket %d: count=%d, keys=%d, avg=%.2f", int(math.Pow10(i)), s[0], s[1], float64(s[1])/float64(s[0]))
		}
	}
	return tmptx.Finish()
}

func (s *Server) compactionBucketWalker(p *s2pkg.BucketWalker) error {
	isQueue := strings.HasPrefix(p.BucketName, "q.")
	isZSetScore := strings.HasPrefix(p.BucketName, "zset.score.")
	isZSet := !isZSetScore && strings.HasPrefix(p.BucketName, "zset.")
	keyCount := uint64(0)
	keyChanged := false
	if err := p.Bucket.ForEach(func(k, v []byte) error {
		// Truncate WAL logs
		if len(p.LogtailStartBuf) > 0 && bytes.Compare(k, p.LogtailStartBuf) < 0 {
			return nil
		}

		// Truncate queue
		if isQueue && len(k) == 16 && p.QueueTTL >= 0 {
			ts := int64(binary.BigEndian.Uint64(k[8:]))
			if (p.UnixNano-ts)/1e9 > int64(p.QueueTTL) {
				atomic.AddInt64(p.QueueDrops, 1)
				keyChanged = true
				return nil
			}
		}
		if isZSet && len(v) == 8 && p.ZSetTTL >= 0 {
			if int(p.UnixNano/1e9)-int(s2pkg.BytesToFloat(v)) > p.ZSetTTL {
				atomic.AddInt64(p.ZSetDrops, 1)
				keyChanged = true
				return nil
			}
		}
		if isZSetScore && len(k) >= 8 && p.ZSetTTL >= 0 {
			if int(p.UnixNano/1e9)-int(s2pkg.BytesToFloat(k[:8])) > p.ZSetTTL {
				atomic.AddInt64(p.ZSetScoreDrops, 1)
				keyChanged = true
				return nil
			}
		}

		atomic.AddInt64(p.Total, 1)
		keyCount++
		return p.Tx.Put(&s2pkg.OnetimeLimitedTxPut{
			BkName: p.BucketName,
			Seq:    p.Bucket.Sequence(),
			Key:    k,
			Value:  v,
		})
	}); err != nil {
		return err
	}

	if keyChanged {
		s.removeCache(p.KeyName)
	}

	// Check compaction
	return p.Tx.Put(&s2pkg.OnetimeLimitedTxPut{
		BkName: p.BucketName,
		Seq:    p.Bucket.Sequence(),
		Finishing: func(tx *bbolt.Tx, tmpb *bbolt.Bucket) error {
			seq := tmpb.Sequence()
			if len(p.LogtailStartBuf) > 0 {
				k, _ := tmpb.Cursor().Last()
				p.Logger.Infof("STAGE 0.2: truncate logs check, buffer=%v, last=%d, tail=%d, count=%d",
					p.LogtailStartBuf, s2pkg.BytesToUint64(k), seq, *p.Total)
			}
			if isZSetScore && keyCount != seq {
				tmpb.SetSequence(keyCount)
				atomic.AddInt64(p.ZSetCardFix, 1)
			}

			if k, _ := tmpb.Cursor().Last(); len(k) == 0 {
				if isQueue {
					atomic.AddInt64(p.QueueDeletes, 1)
					tx.DeleteBucket([]byte(p.BucketName))
				}
				if isZSet {
					atomic.AddInt64(p.ZSetDeletes, 1)
					tx.DeleteBucket([]byte(p.BucketName))
				}
				if isZSetScore {
					atomic.AddInt64(p.ZSetScoreDeletes, 1)
					tx.DeleteBucket([]byte(p.BucketName))
				}
			}
			// Done bucket compaction
			p.KeysDist.Incr(int64(keyCount))
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

func getPendingUnlinksKey(shard int) string {
	return "_unlinks_\t" + strconv.Itoa(shard)
}

func getTTLByName(ttls []s2pkg.Pair, name string) int {
	idx := sort.Search(len(ttls), func(i int) bool { return ttls[i].Member >= name })
	if idx < len(ttls) && name == ttls[idx].Member {
		return int(ttls[idx].Score)
	}
	if idx > 0 && idx <= len(ttls) && strings.HasPrefix(name, ttls[idx-1].Member) {
		return int(ttls[idx-1].Score)
	}
	return -1
}
