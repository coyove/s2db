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
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

func (s *Server) ShardLogtail(shard int) uint64 {
	key := getShardLogKey(int16(shard))
	iter := s.db.NewIter(&pebble.IterOptions{
		LowerBound: key,
		UpperBound: incrBytes(key),
	})
	if iter.Last() && bytes.HasPrefix(iter.Key(), key) {
		return s2pkg.BytesToUint64(iter.Key()[len(key):])
	}
	return 0
}

func (s *Server) ShardLogsRoughCount(shard int) int64 {
	key := getShardLogKey(int16(shard))
	iter := s.db.NewIter(&pebble.IterOptions{
		LowerBound: key,
		UpperBound: incrBytes(key),
	})
	if iter.Last() && bytes.HasPrefix(iter.Key(), key) {
		last := s2pkg.BytesToUint64(iter.Key()[len(key):])
		if iter.First() && bytes.HasPrefix(iter.Key(), key) {
			if iter.Next() && bytes.HasPrefix(iter.Key(), key) {
				first := s2pkg.BytesToUint64(iter.Key()[len(key):])
				return int64(last) - int64(first) + 1
			}
		}
	}
	return -1
}

func (s *Server) logPusher(shard int) {
	defer s2pkg.Recover(func() { time.Sleep(time.Second); go s.logPusher(shard) })
	ctx := context.TODO()
	log := log.WithField("shard", strconv.Itoa(shard))
	ticker := time.NewTicker(time.Second)
	logtailChanged := false

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
			continue
		}

		var cmd *redis.IntCmd
		if !s.Slave.LogtailOK[shard] {
			// First PUSHLOGS to slave will be an empty one, to get slave's current logtail
			cmd = redis.NewIntCmd(ctx, "PUSHLOGS", shard, (&s2pkg.Logs{}).Marshal())
		} else {
			loghead := s.Slave.Logtails[shard]
			logs, err := s.respondLog(shard, loghead)
			if err != nil {
				log.Error("logPusher get local log: ", err)
				continue
			}
			if len(logs.Logs) == 0 {
				if err := rdb.Ping(ctx).Err(); err != nil {
					if strings.Contains(err.Error(), "refused") {
						log.Error("[M] slave not alive")
					} else {
						log.Error("logPusher ping error: ", err)
					}
				} else {
					s.Slave.LastUpdate = time.Now().UnixNano()
				}
				continue
			}
			cmd = redis.NewIntCmd(ctx, "PUSHLOGS", shard, logs.Marshal())
		}
		rdb.Process(ctx, cmd)
		if err := cmd.Err(); err != nil {
			if err != redis.ErrClosed {
				if strings.Contains(err.Error(), "refused") {
					log.Error("[M] slave not alive")
				} else if err != redis.Nil {
					if err.Error() == rejectedByMasterMsg {
						_, err := s.UpdateConfig("slave", "", false)
						log.Info("[M] endpoint rejected PUSHLOGS because it is master, clear slave config: ", err)
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
		s.Slave.LastUpdate = time.Now().UnixNano()
		s.shards[shard].syncWaiter.RaiseTo(logtail)
	}

	log.Info("log pusher exited")
	ticker.Stop()
	s.shards[shard].pusherCloseSignal <- true
}

func runLog(db *pebble.DB, shard int, logs *s2pkg.Logs) (names map[string]bool, logtail uint64, err error) {
	tx := db.NewIndexedBatch()
	defer tx.Close()

	logPrefix := getShardLogKey(int16(shard))
	ltx := s2pkg.LogTx{
		Storage:   tx,
		LogPrefix: logPrefix,
		InLogtail: new(uint64),
	}
	c := tx.NewIter(&pebble.IterOptions{
		LowerBound: logPrefix,
		UpperBound: incrBytes(logPrefix),
	})
	defer c.Close()

	c.Last()
	if !c.Valid() || !bytes.HasPrefix(c.Key(), logPrefix) {
		return nil, 0, fmt.Errorf("fatal: no log found")
	}
	if len(logs.Logs) == 0 {
		return nil, s2pkg.BytesToUint64(c.Key()[len(logPrefix):]), nil
	}
	if currentSig := binary.BigEndian.Uint32(c.Value()[1:]); logs.PrevSig != currentSig {
		return nil, 0, fmt.Errorf("running unrelated logs at %v, got %x, expects %x", c.Key(), currentSig, logs.PrevSig)
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
			return nil, 0, fmt.Errorf("invalid log version: %x and data: %v", data[0], data)
		}
		command, err := splitCommand(data)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid payload: %v", data)
		}

		cmd := strings.ToUpper(command.Get(0))
		name := command.Get(1)

		*ltx.InLogtail = l.Id
		switch cmd {
		case "DEL", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
			_, err = parseDel(cmd, name, command, dd).f(ltx)
		case "ZADD":
			_, err = parseZAdd(cmd, name, command, dd).f(ltx)
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

	logPrefix := getShardLogKey(int16(shard))
	c := s.db.NewIter(&pebble.IterOptions{
		LowerBound: logPrefix,
		UpperBound: incrBytes(logPrefix),
	})
	defer c.Close()

	startBuf := append(logPrefix, s2pkg.Uint64ToBytes(startLogId)...)
	c.SeekGE(startBuf)
	if !c.Valid() || !bytes.Equal(startBuf, c.Key()) {
		currentKey := dupBytes(c.Key())
		c.Last()
		return nil, fmt.Errorf("log #%d request not found, got %v, log tail: %v(%d)",
			startLogId, currentKey, c.Key(), bytes.Compare(startBuf, c.Key()))
	}

	logs = &s2pkg.Logs{}
	logs.PrevSig = binary.BigEndian.Uint32(c.Value()[1:])
	resSize := 0
	for c.Next(); c.Valid() && bytes.HasPrefix(c.Key(), logPrefix); c.Next() {
		data := dupBytes(c.Value())
		if len(data) == 0 || len(c.Key()) != 8+len(logPrefix) {
			return nil, fmt.Errorf("fatal: invalid log entry: %v=>%v", c.Key(), data)
		}
		id := s2pkg.BytesToUint64(c.Key()[len(logPrefix):])
		logs.Logs = append(logs.Logs, &s2pkg.Log{Id: id, Data: data})
		resSize += len(data)
		if len(logs.Logs) >= s.ResponseLogRun || resSize > s.ResponseLogSize*1024 {
			break
		}
	}
	return
}

type endpoint struct {
	mu     sync.RWMutex
	client *redis.Client
	config redisproto.RedisConfig

	RemoteIP   string
	LogtailOK  [ShardNum]bool
	Logtails   [ShardNum]uint64
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
			cfg, err := redisproto.ParseConnString(connString)
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
			e.config = redisproto.RedisConfig{}
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

func (e *endpoint) Config() redisproto.RedisConfig {
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
