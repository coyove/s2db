package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) ShardLogtail(shard int) (tail uint64, err error) {
	err = s.db[shard].View(func(tx *bbolt.Tx) error {
		if bk := tx.Bucket([]byte("wal")); bk != nil {
			tail = bk.Sequence()
		}
		return nil
	})
	return
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
			case <-s.db[shard].pusherTrigger:
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
			cmd = redis.NewIntCmd(ctx, "PUSHLOGS", shard, 0, 0)
		} else {
			loghead := s.Slave.Logtails[shard] + 1
			logs, logprevSig, err := s.respondLog(shard, loghead, false)
			if err != nil {
				log.Error("logPusher get local log: ", err)
				continue
			}
			if len(logs) == 0 {
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
			args := append(make([]interface{}, 0, len(logs)+4), "PUSHLOGS", shard, loghead, logprevSig)
			for _, l := range logs {
				args = append(args, l)
			}
			cmd = redis.NewIntCmd(ctx, args...)
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
		s.db[shard].syncWaiter.RaiseTo(logtail)
	}

	log.Info("log pusher exited")
	ticker.Stop()
	s.db[shard].pusherCloseSignal <- true
}

func runLog(loghead uint64, logprevSig uint32, logs [][]byte, db *bbolt.DB) (names map[string]bool, logtail uint64, err error) {
	names = map[string]bool{}
	err = db.Update(func(tx *bbolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte("wal"))
		if err != nil {
			return err
		}

		sumCheck := crc32.NewIEEE()
		sumBuf := make([]byte, 4)
		ltx := s2pkg.LogTx{Tx: tx}

		if loghead != bk.Sequence()+1 {
			goto REPORT
		}

		if bk.Sequence() > 0 {
			if h := binary.BigEndian.Uint32(bk.Get(s2pkg.Uint64ToBytes(bk.Sequence()))[1:]); h != logprevSig {
				return fmt.Errorf("running unrelated logs at %d, got %x, expects %x", loghead, logprevSig, h)
			}
		}

		for i, data := range logs {
			dd := data
			if data[0] == 0x95 {
				sum32 := data[len(data)-4:]
				data = data[5 : len(data)-4]
				sumCheck.Reset()
				sumCheck.Write(data)
				if !bytes.Equal(sum32, sumCheck.Sum(sumBuf[:0])) {
					return fmt.Errorf("corrupted log checksum at %d", i)
				}
			} else if data[0] == 0x94 {
				sum32 := data[len(data)-4:]
				data = data[1 : len(data)-4]
				sumCheck.Reset()
				sumCheck.Write(data)
				if !bytes.Equal(sum32, sumCheck.Sum(sumBuf[:0])) {
					return fmt.Errorf("corrupted log checksum at %d", i)
				}
			} else if data[0] == 0x93 {
				data = data[1:]
			} else {
				return fmt.Errorf("invalid log entry: %v", data)
			}
			command, err := splitCommand(data)
			if err != nil {
				return fmt.Errorf("invalid payload: %q", data)
			}
			cmd := strings.ToUpper(command.Get(0))
			name := command.Get(1)
			switch cmd {
			case "DEL", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
				_, err = parseDel(cmd, name, command, dd).f(ltx)
			case "ZADD":
				_, err = parseZAdd(cmd, name, command, dd).f(ltx)
			case "ZINCRBY":
				_, err = parseZIncrBy(cmd, name, command, dd).f(ltx)
			case "QAPPEND":
				_, err = parseQAppend(cmd, name, command, dd).f(ltx)
			default:
				return fmt.Errorf("not a write command: %q", cmd)
			}
			if err != nil {
				log.Error("bulkload, error ocurred: ", cmd, " ", name)
				return err
			}
			names[name] = true
		}

	REPORT:
		logtail = bk.Sequence()
		return nil
	})
	return
}

func (s *Server) respondLog(shard int, start uint64, full bool) (logs [][]byte, logprevSig uint32, err error) {
	err = s.db[shard].View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("wal"))
		if bk == nil {
			return nil
		}

		if k, _ := bk.Cursor().First(); len(k) == 8 {
			if head := binary.BigEndian.Uint64(k); head > start {
				return fmt.Errorf("master log (head=%d) has been compacted, slave can't request older log (%d)", head, start)
			}
		}

		myLogtail := bk.Sequence()
		if start == myLogtail+1 {
			return nil
		}
		if start > myLogtail {
			return fmt.Errorf("slave log (req=%d) surpass master log (tail=%d)", start, myLogtail)
		}
		if start > 1 {
			logprevSig = binary.BigEndian.Uint32(bk.Get(s2pkg.Uint64ToBytes(start - 1))[1:])
		}

		resSize := 0
		for i := start; i <= myLogtail; i++ {
			data := append([]byte{}, bk.Get(s2pkg.Uint64ToBytes(uint64(i)))...)
			if len(data) == 0 {
				return fmt.Errorf("fatal: empty log entry")
			}
			logs = append(logs, data)
			resSize += len(data)
			if full {
				continue
			}
			if len(logs) >= s.ResponseLogRun || resSize > s.ResponseLogSize*1024 {
				break
			}
		}
		return nil
	})
	return
}

func (s *Server) requestFullShard(shard int, cfg redisproto.RedisConfig) bool {
	log := log.WithField("shard", strconv.Itoa(shard))
	client := &http.Client{}
	resp, err := client.Get("http://" + cfg.Addr + "/?dump=" + strconv.Itoa(shard) + "&p=" + cfg.Password)
	if err != nil {
		log.Error("requestShard: http error: ", err)
		return false
	}
	defer resp.Body.Close()

	log.Info("requestShard: response received, start dumping")
	sz := s2pkg.ParseInt(resp.Header.Get("X-Size"))
	if sz == 0 {
		log.Error("requestShard: invalid size")
		return false
	}

	fn := makeShardFilename(shard)
	of, err := os.Create(filepath.Join(s.DataPath, fn))
	if err != nil {
		log.Error("requestShard: create shard: ", err)
		return false
	}
	defer of.Close()

	lastProgress := 0
	start := time.Now()
	_, ok, err := s2pkg.CopyCrc32(of, resp.Body, func(progress int) {
		if cp := int(float64(progress) / float64(sz) * 20); cp > lastProgress {
			lastProgress = cp
			log.Info("requestShard: progress ", cp*5, "%")
		}
	})
	if err != nil {
		log.Error("requestShard: copy: ", err)
		return false
	}
	if !ok {
		log.Error("requestShard: crc32 checksum failed")
		return false
	}
	log.Info("requestShard: progress 100% in ", time.Since(start))
	if err := s.UpdateShardFilename(shard, fn); err != nil {
		log.Error("requestShard: update shard filename failed: ", err)
		return false
	}
	return true
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
