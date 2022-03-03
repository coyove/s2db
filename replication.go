package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) myLogTails() (total [ShardNum]uint64, combined uint64, errors [ShardNum]error, err error) {
	var oneError error
	for i := range s.db {
		if err := s.db[i].View(func(tx *bbolt.Tx) error {
			bk := tx.Bucket([]byte("wal"))
			if bk != nil {
				k, _ := bk.Cursor().Last()
				if len(k) == 8 {
					total[i] = binary.BigEndian.Uint64(k)
					combined += total[i]
				}
			}

			return nil
		}); err != nil {
			errors[i] = err
			oneError = fmt.Errorf("%v -> %v", oneError, err)
		}
	}
	err = oneError
	return
}

func (s *Server) myLogTail(shard int) (total uint64, err error) {
	tails, combined, errors, err := s.myLogTails()
	if shard >= 0 {
		return tails[shard], errors[shard]
	}
	return combined, err
}

func (s *Server) SlaveInfo() (data []string) {
	if s.Slave.ServerName != "" {
		tails, combined, _, err := s.myLogTails()
		if err != nil {
			return []string{"error:" + err.Error()}
		}
		diffs := [ShardNum]int64{}
		lt := int64(0)
		si := s.Slave
		for i, t := range si.Logtails {
			lt += int64(t)
			diffs[i] = int64(tails[i]) - int64(t)
		}
		data = append(data, "# slave",
			"name:"+si.ServerName,
			"address:"+si.RemoteAddr,
			"version:"+si.Version,
			fmt.Sprintf("ack_before:%v(%v)", si.AckBefore(), s.IsAcked(si)),
			"listen:"+si.ListenAddr,
			"logtail:"+joinArray(si.Logtails),
			fmt.Sprintf("logtail_diff:%v", joinArray(diffs)),
			fmt.Sprintf("logtail_diff_sum:%d", int64(combined)-lt),
			"",
		)
	}
	return append(data, "")
}

func (s *Server) requestLogPuller(shard int) {
	ctx := context.TODO()
	log := log.WithField("shard", strconv.Itoa(shard))

	defer func() {
		if r := recover(); r != nil {
			log.Error(r, " ", string(debug.Stack()))
			time.Sleep(time.Second)
			go s.requestLogPuller(shard)
		}
	}()

	for !s.Closed {
		wait := time.Millisecond * time.Duration(s.PingTimeout) / 2
		ping := redis.NewStringCmd(ctx, "PING", "FROM", s.ln.Addr().String(), s.ServerName, Version)
		s.MasterRedis.Process(ctx, ping)
		parts := strings.Split(ping.Val(), " ")
		if len(parts) != 3 {
			if ping.Err() != nil && strings.Contains(ping.Err().Error(), "refused") {
				if shard == 0 {
					log.Error("ping: master not alive")
				}
			} else {
				log.Error("ping: invalid response: ", ping.Val(), ping.Err())
			}
			time.Sleep(wait)
			continue
		}
		s.Master.ServerName = parts[1]
		s.Master.Version = parts[2]
		s.Master.LastUpdate = time.Now().UnixNano()
		if s.Master.ServerName == "" {
			log.Error("master responded empty server name")
			time.Sleep(wait)
			continue
		}
		if s.Master.ServerName != s.MasterConfig.Name {
			log.Errorf("fatal: master responded un-matched server name: %q, asking the wrong master?", s.Master.ServerName)
			break
		}
		if s.StopLogPull == 1 {
			time.Sleep(time.Second)
			continue
		}
		// if s.master.Version > Version {
		// 	log.Error("ping: master version too high: ", s.master.Version, ">", Version)
		// 	time.Sleep(time.Second * 10)
		// 	continue
		// }

		myLogtail, err := s.myLogTail(shard)
		if err != nil {
			if err == bbolt.ErrDatabaseNotOpen {
				time.Sleep(wait)
				continue
			}
			log.Error("read local log index: ", err)
			break
		}

		cmd := redis.NewStringSliceCmd(ctx, "REQUESTLOG", shard, myLogtail+1)
		if err := s.MasterRedis.Process(ctx, cmd); err != nil {
			if strings.Contains(err.Error(), "refused") {
				if shard == 0 {
					log.Error("master not alive")
				}
			} else if err != redis.Nil {
				log.Error("request log from master: ", err)
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
		for _, x := range cmds {
			command, err := splitCommandBase64(x)
			if err != nil {
				return fmt.Errorf("fatal: invalid payload: %q", x)
			}
			cmd := strings.ToUpper(command.Get(0))
			name := command.Get(1)
			switch cmd {
			case "DEL", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
				_, err = parseDel(cmd, name, command)(tx)
			case "ZADD":
				_, err = parseZAdd(cmd, name, command)(tx)
			case "ZINCRBY":
				_, err = parseZIncrBy(cmd, name, command)(tx)
			case "QAPPEND":
				_, err = parseQAppend(cmd, name, command)(tx)
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
	sz := 0
	myLogtail, err := s.myLogTail(shard)
	if err != nil {
		return nil, err
	}
	if start == myLogtail+1 {
		return nil, nil
	}
	if start > myLogtail {
		return nil, fmt.Errorf("slave log (%d) surpass master log (%d)", start, myLogtail)
	}
	err = s.db[shard].View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("wal"))
		if bk == nil {
			return nil
		}

		k, _ := bk.Cursor().First()
		if len(k) == 8 {
			first := binary.BigEndian.Uint64(k)
			if first > start {
				return fmt.Errorf("master log (head=%d) has been compacted, slave failed to request older log (%d)", first, start)
			}
		}

		sumCheck := crc32.NewIEEE()
		sumBuf := make([]byte, 4)
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
			sz += len(data)
			if full {
				continue
			}
			if len(logs) >= s.ResponseLogRun || sz > s.ResponseLogSize*1024 {
				break
			}
		}
		return nil
	})
	return
}

type serverInfo struct {
	RemoteAddr string           `json:"remoteaddr"`
	ServerName string           `json:"servername"`
	ListenAddr string           `json:"listen"`
	Logtails   [ShardNum]uint64 `json:"logtails"`
	Version    string           `json:"version"`
	LastUpdate int64            `json:"lastupdate"`
}

func (s *Server) IsAcked(si serverInfo) bool {
	return si.AckBefore() < time.Duration(s.PingTimeout)*time.Millisecond
}

func (si *serverInfo) AckBefore() time.Duration {
	return time.Since(time.Unix(0, si.LastUpdate))
}

func (si *serverInfo) RemoteConnectAddr() string {
	_, port, _ := net.SplitHostPort(si.ListenAddr)
	return si.RemoteAddr + ":" + port
}
