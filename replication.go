package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/ranges"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

func writeLog(tx extdb.LogTx, dd []byte) error {
	var id uint64
	if tx.InLogtail == nil {
		id = clock.Id()
	} else {
		id = *tx.InLogtail
	}
	if tx.OutLogtail != nil {
		*tx.OutLogtail = id
	}
	return tx.Set(appendUint(tx.LogPrefix, id), dd, pebble.Sync)
}

func (s *Server) ShardLogtail(shard int) uint64 {
	key := ranges.GetShardLogKey(int16(shard))
	iter := s.DB.NewIter(&pebble.IterOptions{
		LowerBound: key,
		UpperBound: s2pkg.IncBytes(key),
	})
	defer iter.Close()
	if iter.Last() && bytes.HasPrefix(iter.Key(), key) {
		return s2pkg.BytesToUint64(iter.Key()[len(key):])
	}
	return 0
}

func (s *Server) ShardLogInfo(shard int) (logtail uint64, logSpan int64, compacted bool) {
	key := ranges.GetShardLogKey(int16(shard))
	iter := s.DB.NewIter(&pebble.IterOptions{
		LowerBound: key,
		UpperBound: s2pkg.IncBytes(key),
	})
	defer iter.Close()
	if iter.Last() {
		last := s2pkg.BytesToUint64(iter.Key()[len(key):])
		if last == 0 {
			return 0, 0, false
		}
		if iter.First() {
			if iter.Next() {
				first := s2pkg.BytesToUint64(iter.Key()[len(key):])
				if first == 1 {
					if iter.Next() {
						first = s2pkg.BytesToUint64(iter.Key()[len(key):])
						return last, clock.IdNano(last) - clock.IdNano(first), true
					}
					panic("fatal: no log after compaction checkpoint")
				}
				return last, clock.IdNano(last) - clock.IdNano(first), false
			}
		}
	}
	panic("fatal: invalid shard logs")
}

func (s *Server) checkLogtail(shard int) error {
	logtail := s.ShardLogtail(shard)
	if logtail == 0 {
		return nil
	}
	now := clock.UnixNano()
	if now < clock.IdNano(logtail) {
		return fmt.Errorf("monotonic clock skew: %d<%d", now, clock.IdNano(logtail))
	}
	return nil
}

func (s *Server) compactLogs(shard int, first bool) {
	logtail := s.ShardLogtail(shard)
	if logtail == 0 {
		return
	}

	if !first {
		if clock.Rand() >= 1/float64(s.ServerConfig.CompactLogsDice) {
			return
		}
	}

	s.Survey.LogCompaction.Incr(1)

	logPrefix := ranges.GetShardLogKey(int16(shard))
	logx := clock.IdBeforeSeconds(logtail, s.ServerConfig.CompactLogsTTL)

	if err := func() error {
		b := s.DB.NewBatch()
		defer b.Close()
		if err := b.DeleteRange(appendUint(logPrefix, 2), appendUint(logPrefix, logx), pebble.Sync); err != nil {
			return err
		}
		if err := b.Set(appendUint(logPrefix, 1), joinMultiBytesEmptyNoSig(), pebble.Sync); err != nil {
			return err
		}
		return b.Commit(pebble.Sync)
	}(); err != nil {
		log.Errorf("compact log shard #%d, logx=%d: %v", shard, logx, err)
	}
}

func (s *Server) logPusher(shard int) {
	defer s2pkg.Recover(func() { time.Sleep(time.Second); go s.logPusher(shard) })
	ctx := context.TODO()
	log := log.WithField("shard", "#"+strconv.Itoa(shard))
	ticker := time.NewTicker(time.Second)
	logtailChanged := false

	s.compactLogs(shard, true)

	for !s.Closed {
		if logtailChanged {
			// Continue pushing logs until slave catches up with master
		} else {
			select {
			case <-s.shards[shard].pusherTrigger:
			case <-ticker.C:
			}
		}
		logtailChanged = false

		rdb := s.Slave.Redis()
		if rdb == nil {
			s.compactLogs(shard, false)
			continue
		}

		var cmd *redis.IntCmd
		if !s.Slave.LogtailOK[shard] {
			// First PUSHLOGS to slave will be an empty one, to get slave's current logtail
			cmd = redis.NewIntCmd(ctx, "PUSHLOGS", shard, (&s2pkg.Logs{}).MarshalBytes())
		} else {
			loghead := s.Slave.Logtails[shard]
			logs, err := s.respondLog(shard, loghead)
			if err != nil {
				log.Error("local pusher: ", err)
				continue
			}
			if len(logs.Logs) == 0 {
				if err := rdb.Ping(ctx).Err(); err != nil {
					if s2pkg.IsRemoteOfflineError(err) {
						log.Error("[M] slave offline")
					} else {
						log.Error("failed to ping slave: ", err)
					}
				} else {
					s.Slave.LastUpdate = clock.UnixNano()
				}
				continue
			}
			cmd = redis.NewIntCmd(ctx, "PUSHLOGS", shard, logs.MarshalBytes())
		}
		rdb.Process(ctx, cmd)
		if err := cmd.Err(); err != nil {
			if err != redis.ErrClosed {
				if s2pkg.IsRemoteOfflineError(err) {
					log.Error("[M] slave offline")
				} else if err != redis.Nil {
					if err.Error() == rejectedByMasterMsg {
						_, err := s.UpdateConfig("slave", "", false)
						log.Info("[M] endpoint is master and rejected PUSHLOGS, clearing slave config: ", err)
						continue
					}
					log.Error("push logs to slave: ", err)
				}
			}
			continue
		}
		logtail := uint64(cmd.Val())
		s.Slave.LogtailOK[shard] = true
		logtailChanged = s.Slave.Logtails[shard] != logtail
		s.Slave.Logtails[shard] = logtail
		s.Slave.LastUpdate = clock.UnixNano()
		s.shards[shard].syncWaiter.RaiseTo(logtail)
		s.compactLogs(shard, false)
	}

	ticker.Stop()
	s.shards[shard].pusherCloseSignal <- true
}

func (s *Server) runLog(shard int, logs *s2pkg.Logs) (names map[string]bool, logtail uint64, err error) {
	s.shards[shard].runLogLock.Lock(func() {
		log.Errorf("slow runLog(%d), logs size: %d", shard, len(logs.Logs))
	})
	defer s.shards[shard].runLogLock.Unlock()

	tx := s.DB.NewIndexedBatch()
	defer tx.Close()

	logPrefix := ranges.GetShardLogKey(int16(shard))
	ltx := extdb.LogTx{
		Storage:   tx,
		LogPrefix: logPrefix,
		InLogtail: new(uint64),
	}
	c := tx.NewIter(&pebble.IterOptions{
		LowerBound: logPrefix,
		UpperBound: s2pkg.IncBytes(logPrefix),
	})
	defer c.Close()

	c.Last()
	if !c.Valid() || !bytes.HasPrefix(c.Key(), logPrefix) {
		return nil, 0, fmt.Errorf("fatal: no log found")
	}

	currentLogtail := s2pkg.BytesToUint64(c.Key()[len(logPrefix):])
	if len(logs.Logs) == 0 {
		return nil, currentLogtail, nil
	}
	if currentSig := binary.BigEndian.Uint32(c.Value()[1:]); logs.PrevSig != currentSig {
		log.Errorf("running unrelated logs at %v, got %x, expects %x", c.Key(), currentSig, logs.PrevSig)
		return nil, currentLogtail, nil
	}

	names = map[string]bool{}
	sumCheck := crc32.NewIEEE()
	sumBuf := make([]byte, 4)
	for _, l := range logs.Logs {
		data := l.Data
		dd := data
		if len(data) >= 9 && data[0] == 0x95 {
			sum32 := data[len(data)-4:]
			data = data[5 : len(data)-4]
			sumCheck.Reset()
			sumCheck.Write(data)
			if !bytes.Equal(sum32, sumCheck.Sum(sumBuf[:0])) {
				return nil, 0, fmt.Errorf("corrupted log checksum: %v", data)
			}
		} else {
			return nil, 0, fmt.Errorf("invalid log data: %v", data)
		}
		command, err := splitRawMultiBytesNoHeader(data)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid payload: %v", data)
		}

		cmd := strings.ToUpper(command.Get(0))
		name := command.Get(1)

		*ltx.InLogtail = l.Id
		switch cmd {
		case "DEL", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
			_, err = s.parseDel(cmd, name, command, dd).f(ltx)
		case "ZADD":
			_, err = s.parseZAdd(cmd, name, command, dd).f(ltx)
		case "ZINCRBY":
			_, err = parseZIncrBy(cmd, name, command, dd).f(ltx)
		default:
			return nil, 0, fmt.Errorf("not a write command: %q", cmd)
		}
		if err != nil {
			return nil, 0, fmt.Errorf("failed to run %s %s: %v", cmd, name, err)
		}
		names[name] = true
	}

	c.Last()
	if !c.Valid() || !bytes.HasPrefix(c.Key(), logPrefix) {
		return nil, 0, fmt.Errorf("fatal: %v", c.Key())
	}
	return names, s2pkg.BytesToUint64(c.Key()[len(logPrefix):]), tx.Commit(pebble.Sync)
}

func (s *Server) respondLog(shard int, startLogId uint64) (logs *s2pkg.Logs, err error) {
	if startLogId == 1 {
		return nil, fmt.Errorf("can't respond logs at #1 (compaction checkpoint)")
	}

	logPrefix := ranges.GetShardLogKey(int16(shard))
	c := s.DB.NewIter(&pebble.IterOptions{
		LowerBound: logPrefix,
		UpperBound: s2pkg.IncBytes(logPrefix),
	})
	defer c.Close()

	startBuf := append(logPrefix, s2pkg.Uint64ToBytes(startLogId)...)
	c.SeekGE(startBuf)
	if !c.Valid() {
		return nil, fmt.Errorf("log %d not found", startLogId)
	}
	if !bytes.Equal(startBuf, c.Key()) {
		if bytes.HasPrefix(c.Key(), logPrefix) {
			closestLogId := s2pkg.BytesToUint64(c.Key()[len(logPrefix):])
			if startLogId < closestLogId {
				return nil, fmt.Errorf("log %d not found, oldest log is %d (local compacted)", startLogId, closestLogId)
			}
			return nil, fmt.Errorf("log %d not found, got %d at its closest", startLogId, closestLogId)
		}
		return nil, fmt.Errorf("log %d not found, got %v", startLogId, c.Key())
	}

	logs = &s2pkg.Logs{}
	logs.PrevSig = binary.BigEndian.Uint32(c.Value()[1:])
	resSize := 0
	for c.Next(); c.Valid() && bytes.HasPrefix(c.Key(), logPrefix); c.Next() {
		data := s2pkg.Bytes(c.Value())
		if len(data) == 0 || len(c.Key()) != 8+len(logPrefix) {
			return nil, fmt.Errorf("fatal: invalid log entry: (%v, %v)", c.Key(), data)
		}

		id := s2pkg.BytesToUint64(c.Key()[len(logPrefix):])
		if id == 1 {
			return nil, fmt.Errorf("logs are compacted")
		}

		logs.Logs = append(logs.Logs, &s2pkg.Log{Id: id, Data: data})
		resSize += len(data)
		if resSize > s.ResponseLogSize*1024 {
			break
		}
	}
	return
}

type endpoint struct {
	mu     sync.RWMutex
	client *redis.Client
	config wire.RedisConfig

	RemoteIP   string
	LogtailOK  [ShardLogNum]bool
	Logtails   [ShardLogNum]uint64
	LastUpdate int64
}

func (e *endpoint) IsAcked(s *Server) bool {
	return e.AckBefore() < time.Duration(s.PingTimeout)*time.Millisecond
}

func (e *endpoint) AckBefore() time.Duration {
	return time.Since(time.Unix(0, e.LastUpdate))
}

func (e *endpoint) CreateRedis(connString string) (changed bool, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if connString != e.config.Raw {
		if connString != "" {
			cfg, err := wire.ParseConnString(connString)
			if err != nil {
				return false, err
			}
			if cfg.Name == "" {
				return false, fmt.Errorf("sevrer name must be set")
			}
			old := e.client
			e.config, e.client = cfg, cfg.GetClient()
			if old != nil {
				old.Close()
			}
		} else {
			e.client.Close()
			e.client = nil
			e.config = wire.RedisConfig{}
		}
		for i := range e.LogtailOK {
			e.LogtailOK[i] = false
		}
		changed = true
	}
	return
}

func (e *endpoint) Redis() *redis.Client {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.client
}

func (e *endpoint) Config() wire.RedisConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.config
}

func (e *endpoint) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}
