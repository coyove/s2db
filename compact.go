package main

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	s2pkg "github.com/coyove/s2db/s2pkg"
	log "github.com/sirupsen/logrus"
)

func (s *Server) DumpShard(shard int, path string) (int64, error) {
	// return s.shards[shard].DB.Dump(path, s.DumpSafeMargin*1024*1024)
	return 0, nil
}

func (s *Server) CompactShard(shard int) {
	s.compactShardImpl(shard)
}

func (s *Server) compactShardImpl(shard int) {
	log := log.WithField("shard", strconv.Itoa(shard))

	if v, ok := s.CompactLock.Lock(shard); !ok {
		log.Info("previous compaction in the way #", v)
		return
	}

	defer func() {
		s.CompactLock.Unlock()
		s2pkg.Recover(nil)
	}()

	logPrefix := getShardLogKey(int16(shard))
	c := s.db.NewIter(&pebble.IterOptions{
		LowerBound: logPrefix,
		UpperBound: incrBytes(logPrefix),
	})
	defer c.Close()

	c.Last()
	if !c.Valid() || !bytes.HasPrefix(c.Key(), logPrefix) {
		log.Errorf("invalid log head: %v", c.Key())
		return
	}

	id := int64(s2pkg.BytesToUint64(c.Key()[len(logPrefix):]))
	if id == 0 {
		log.Info("no log to compact")
		return
	}
	if id <= int64(s.ServerConfig.CompactLogHead)*2 {
		log.Info("small logs, no need to compact")
		return
	}

	logx := uint64(id - int64(s.ServerConfig.CompactLogHead))
	if s.Slave.IsAcked(s) {
		if s.Slave.Logtails[shard] == 0 || !s.Slave.LogtailOK[shard] {
			log.Info("slave is not replication-ready, can't compact logs")
			return
		}
		if logx < s.Slave.Logtails[shard] {
			log.Infof("log compaction: adjust for slave: %d->%d", logx, s.Slave.Logtails[shard])
			logx = s.Slave.Logtails[shard]
		}
	}

	b := s.db.NewBatch()
	if err := b.DeleteRange(append(dupBytes(logPrefix), s2pkg.Uint64ToBytes(2)...),
		append(dupBytes(logPrefix), s2pkg.Uint64ToBytes(logx)...),
		pebble.Sync); err != nil {
		log.Errorf("log compaction: delete range: %v", err)
		return
	}
	if err := b.Set(append(dupBytes(logPrefix), s2pkg.Uint64ToBytes(1)...), joinCommandEmpty(), pebble.Sync); err != nil {
		log.Errorf("log compaction: set log #1: %v", err)
		return
	}
	if err := b.Commit(pebble.Sync); err != nil {
		log.Errorf("log compaction: final commit: %v", err)
		return
	}
	a := time.Now().Unix()
	log.Infof("log comssy: %d, %d %d", logx, id, a*a+a+1)
}

func (s *Server) schedCompactionJob() {
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
						s.compactShardImpl(i)
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
						s.compactShardImpl(shardIdx)
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
				s.compactShardImpl(shardIdx)
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
