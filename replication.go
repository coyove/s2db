package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
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

func (s *Server) SlaveLogtailsInfo() (data []string) {
	if s.Slave.ServerName != "" {
		diffs, diffSum := [ShardNum]int64{}, int64(0)
		for i := range s.db {
			tail, err := s.ShardLogtail(i)
			if err != nil {
				return []string{fmt.Sprintf("shard #%d error: %v", i, err)}
			}
			diffs[i] = int64(tail) - int64(s.Slave.Logtails[i])
			diffSum += diffs[i]
		}
		data = append(data, "# slave_logtails",
			fmt.Sprintf("logtail:%v", joinArray(s.Slave.Logtails)),
			fmt.Sprintf("logtail_diff:%v", joinArray(diffs)),
			fmt.Sprintf("logtail_diff_sum:%d", diffSum),
			"",
		)
	}
	return append(data, "")
}

func (s *Server) logPuller(shard int) {
	ctx := context.TODO()
	log := log.WithField("shard", strconv.Itoa(shard))

	defer s2pkg.Recover(func() { time.Sleep(time.Second); go s.logPuller(shard) })

	for !s.Closed {
		wait := time.Millisecond * time.Duration(s.PingTimeout) / 2
		rdb := s.Master.Redis()
		masterName := s.Master.Config().Name
		if rdb == nil {
			time.Sleep(wait)
			continue
		}

		ping := redis.NewStringCmd(ctx, "PING", "FROM", s.ln.Addr().String(), s.ServerName, Version)
		rdb.Process(ctx, ping)
		parts := strings.Split(ping.Val(), " ")
		if len(parts) != 3 {
			if ping.Err() != redis.ErrClosed {
				if ping.Err() != nil && strings.Contains(ping.Err().Error(), "refused") {
					log.Error("[M] ping: master not alive")
				} else {
					log.Error("ping: invalid response: ", ping.Val(), ping.Err())
				}
			}
			time.Sleep(wait)
			continue
		}

		s.Master.ServerName = parts[1]
		s.Master.Version = parts[2]
		s.Master.LastUpdate = time.Now().UnixNano()
		if s.Master.ServerName != masterName {
			log.Errorf("[M] master responded un-matched server name: %q, asking the wrong master?", s.Master.ServerName)
			time.Sleep(wait)
			continue
		}

		myLogtail, err := s.ShardLogtail(shard)
		if err != nil {
			if err == bbolt.ErrDatabaseNotOpen {
				time.Sleep(wait)
				continue
			}
			log.Error("read local log index: ", err)
			break
		}

		cmd := redis.NewStringSliceCmd(ctx, "REQUESTLOG", shard, myLogtail+1)
		if err := rdb.Process(ctx, cmd); err != nil {
			if err != redis.ErrClosed {
				if strings.Contains(err.Error(), "refused") {
					log.Error("[M] master not alive")
				} else if err != redis.Nil {
					log.Error("request log from master: ", err)
				}
			}
			time.Sleep(wait)
			continue
		}

		cmds := cmd.Val()
		if len(cmds) == 0 {
			time.Sleep(time.Second)
			continue
		}

		start := time.Now()
		s.db[shard].compactLocker.Lock(func() { log.Info("bulkload is waiting for compactor") })
		names, err := runLog(cmds, s.db[shard].DB)
		s.db[shard].compactLocker.Unlock()
		if err != nil {
			log.Error("bulkload: ", err)
		} else {
			for n := range names {
				s.removeCache(n)
			}
			s.Survey.BatchLatSv.Incr(time.Since(start).Milliseconds())
			s.Survey.BatchSizeSv.Incr(int64(len(names)))
		}
	}

	log.Info("log replayer exited")
	s.db[shard].pullerCloseSignal <- true
}

func runLog(cmds []string, db *bbolt.DB) (names map[string]bool, err error) {
	names = map[string]bool{}
	err = db.Update(func(tx *bbolt.Tx) error {
		ltx := s2pkg.LogTx{Tx: tx}
		for _, x := range cmds {
			command, err := splitCommandBase64(x)
			if err != nil {
				return fmt.Errorf("fatal: invalid payload: %q", x)
			}
			cmd := strings.ToUpper(command.Get(0))
			name := command.Get(1)
			switch cmd {
			case "DEL", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
				_, err = parseDel(cmd, name, command).f(ltx)
			case "ZADD":
				_, err = parseZAdd(cmd, name, command).f(ltx)
			case "ZINCRBY":
				_, err = parseZIncrBy(cmd, name, command).f(ltx)
			case "QAPPEND":
				_, err = parseQAppend(cmd, name, command).f(ltx)
			default:
				return fmt.Errorf("fatal: not a write command: %q", cmd)
			}
			if err != nil {
				log.Error("bulkload, error ocurred: ", cmd, " ", name)
				return err
			}
			names[name] = true
		}
		return nil
	})
	return
}

func (s *Server) respondLog(shard int, start uint64, full bool) (logs []string, err error) {
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

		sumCheck := crc32.NewIEEE()
		sumBuf := make([]byte, 4)
		resSize := 0
		for i := start; i <= myLogtail; i++ {
			data := bk.Get(s2pkg.Uint64ToBytes(uint64(i)))
			if data[0] == 0x94 {
				sum32 := data[len(data)-4:]
				data = data[1 : len(data)-4]
				sumCheck.Reset()
				sumCheck.Write(data)
				if !bytes.Equal(sum32, sumCheck.Sum(sumBuf[:0])) {
					return fmt.Errorf("fatal error, corrupted log checksum at %d", i)
				}
				logs = append(logs, base64.URLEncoding.EncodeToString(data))
			} else if data[0] == 0x93 {
				logs = append(logs, base64.URLEncoding.EncodeToString(data[1:]))
			} else {
				logs = append(logs, string(data))
			}
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

func (s *Server) requestFullShard(shard int) bool {
	log := log.WithField("shard", strconv.Itoa(shard))
	client := &http.Client{}
	resp, err := client.Get("http://" + s.Master.Config().Addr +
		"/?dump=" + strconv.Itoa(shard) + "&p=" + s.Master.Config().Password)
	if err != nil {
		log.Error("requestShard: http error: ", err)
		return false
	}
	defer resp.Body.Close()

	sz := s2pkg.ParseInt(resp.Header.Get("X-Size"))
	if sz == 0 {
		log.Error("requestShard: invalid size")
		return false
	}

	fn := s.MakeShardFilename(shard)
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

	RemoteAddr string           `json:"remoteaddr"`
	ServerName string           `json:"servername"`
	ListenAddr string           `json:"listen"`
	Logtails   [ShardNum]uint64 `json:"logtails"`
	Version    string           `json:"version"`
	LastUpdate int64            `json:"lastupdate"`
}

func (e *endpoint) IsAcked(s *Server) bool {
	return e.AckBefore() < time.Duration(s.PingTimeout)*time.Millisecond
}

func (e *endpoint) AckBefore() time.Duration {
	return time.Since(time.Unix(0, e.LastUpdate))
}

func (e *endpoint) RemoteConnectAddr() string {
	_, port, _ := net.SplitHostPort(e.ListenAddr)
	return e.RemoteAddr + ":" + port
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
