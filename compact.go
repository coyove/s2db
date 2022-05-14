package main

import (
	"bytes"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/clock"
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

	defer func() {
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

	id := s2pkg.BytesToUint64(c.Key()[len(logPrefix):])
	if id == 0 {
		log.Info("no log to compact")
		return
	}
	if id == 1 {
		log.Error("invalid log tail: no log after compaction checkpoint")
		return
	}

	logx := clock.IdBeforeSeconds(id, s.ServerConfig.CompactLogsTTL)
	if s.Slave.IsAcked(s) {
		if s.Slave.Logtails[shard] == 0 || !s.Slave.LogtailOK[shard] {
			log.Info("slave is not replication-ready, can't compact logs")
			return
		}
		if logx > s.Slave.Logtails[shard] {
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
	log.Infof("log compaction: %d, %d", logx, id)
}

func (s *Server) schedCompactionJob() {
	// shard := 0
	for !s.Closed {
		// for i := 0; i < LogShardNum/16; i++ {
		// 	shard = (shard + 1) % LogShardNum
		// 	s.CompactShard(shard)
		// }
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
