package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) myLogTails() (total [ShardNum]uint64, combined uint64, err error) {
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
			return total, combined, err
		}
	}
	return
}

func (s *Server) myLogTail(shard int) (total uint64, err error) {
	tails, combined, err := s.myLogTails()
	if shard >= 0 {
		return tails[shard], err
	}
	return combined, err
}

func (s *Server) slavesSection() (data []string) {
	tails, combined, err := s.myLogTails()
	if err != nil {
		return []string{"error:" + err.Error()}
	}

	data = []string{"# slaves", ""}
	names := []string{}
	diffs := [ShardNum]int64{}
	s.slaves.Foreach(func(si *serverInfo) {
		lt := int64(0)
		for i, t := range si.LogTails {
			lt += int64(t)
			diffs[i] = int64(tails[i]) - int64(t)
		}
		data = append(data,
			"slave_"+si.RemoteAddr+"_name:"+si.ServerName,
			"slave_"+si.RemoteAddr+"_version:"+si.Version,
			"slave_"+si.RemoteAddr+"_ack_before:"+strconv.FormatInt(time.Now().Unix()-si.LastUpdateUnix, 10),
			"slave_"+si.RemoteAddr+"_listen:"+si.ListenAddr,
			"slave_"+si.RemoteAddr+"_logtail:"+joinArray(si.LogTails),
			fmt.Sprintf("slave_%s_logtail_diff_sum:%d", si.RemoteAddr, int64(combined)-lt),
			fmt.Sprintf("slave_%s_logtail_diff:%v", si.RemoteAddr, joinArray(diffs)),
		)
		names = append(names, si.RemoteAddr)
	})
	data[1] = "list:" + joinArray(names)
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

	for !s.closed {
		ping := redis.NewStringCmd(ctx, "PING", "FROM", s.ln.Addr().String(), s.ServerName, Version)
		s.rdb.Process(ctx, ping)
		parts := strings.Split(ping.Val(), " ")
		if len(parts) != 3 {
			if ping.Err() != nil && strings.Contains(ping.Err().Error(), "refused") {
				if shard == 0 {
					log.Error("ping: master not alive")
				}
			} else {
				log.Error("ping: invalid response: ", ping.Val(), ping.Err())
			}
			time.Sleep(time.Second * 10)
			continue
		}
		s.master = serverInfo{
			ServerName: parts[1],
			Version:    parts[2],
		}
		if s.master.ServerName == "" {
			log.Error("master responded empty server name")
			time.Sleep(time.Second * 10)
			continue
		}
		if s.master.ServerName != s.MasterNameAssert {
			log.Errorf("fatal: master responded un-matched server name: %q, asking the wrong master?", s.master.ServerName)
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

	AGAIN:
		myWalIndex, err := s.myLogTail(shard)
		if err != nil {
			if err == bbolt.ErrDatabaseNotOpen {
				time.Sleep(time.Second * 5)
				goto AGAIN
			}
			log.Error("read local log index: ", err)
			break
		}

		cmd := redis.NewStringSliceCmd(ctx, "REQUESTLOG", shard, myWalIndex+1)
		if err := s.rdb.Process(ctx, cmd); err != nil {
			if strings.Contains(err.Error(), "refused") {
				if shard == 0 {
					log.Error("master not alive")
				}
			} else if err != redis.Nil {
				log.Error("request log from master: ", err)
			}
			time.Sleep(time.Second * 2)
			continue
		}

		cmds := cmd.Val()
		if len(cmds) == 0 {
			time.Sleep(time.Second)
			continue
		}

		start := time.Now()
		names, err := runLog(cmds, s.db[shard].DB, s.FillPercent)
		if err != nil {
			log.Error("bulkload: ", err)
		} else {
			for n := range names {
				s.removeCache(n)
			}
			s.survey.batchLatSv.Incr(time.Since(start).Milliseconds())
			s.survey.batchSizeSv.Incr(int64(len(names)))
		}
	}

	log.Info("log replayer exited")
	s.db[shard].pullerCloseSignal <- true
}

func runLog(cmds []string, db *bbolt.DB, fillPercent int) (names map[string]bool, err error) {
	names = map[string]bool{}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, x := range cmds {
			command, err := splitCommand(x)
			if err != nil {
				return fmt.Errorf("fatal: invalid payload: %q", x)
			}
			cmd := strings.ToUpper(command.Get(0))
			name := command.Get(1)
			switch cmd {
			case "DEL", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
				_, err = parseDel(cmd, name, command)(tx)
			case "ZADD":
				_, err = parseZAdd(cmd, name, fillPercent, command)(tx)
			case "ZINCRBY":
				_, err = parseZIncrBy(cmd, name, command)(tx)
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

func (s *Server) responseLog(shard int, start uint64, full bool) (logs []string, err error) {
	sz := 0
	masterWalIndex, err := s.myLogTail(shard)
	if err != nil {
		return nil, err
	}
	if start == masterWalIndex+1 {
		return nil, nil
	}
	if start > masterWalIndex {
		return nil, fmt.Errorf("slave log (%d) surpass master log (%d)", start, masterWalIndex)
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
				return fmt.Errorf("master log (%d) truncated as slave request older log (%d)", first, start)
			}
		}

		for i := start; i <= masterWalIndex; i++ {
			data := bk.Get(intToBytes(uint64(i)))
			logs = append(logs, string(data))
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

type slaves struct {
	sync.RWMutex
	q map[string]*serverInfo
}

type serverInfo struct {
	// RemoteAddr is the definitive identifier of a server
	RemoteAddr string `json:"remoteaddr"`

	ServerName     string           `json:"servername"`
	ListenAddr     string           `json:"listen"`
	LogTails       [ShardNum]uint64 `json:"logtails"`
	Version        string           `json:"version"`
	LastUpdateUnix int64            `json:"lastupdate"`

	purgeTimer *time.Timer
}

func (s *slaves) Get(serverName string) *serverInfo {
	s.RLock()
	defer s.RUnlock()
	return s.q[serverName]
}

func (s *slaves) Foreach(cb func(*serverInfo)) {
	s.RLock()
	defer s.RUnlock()
	for _, sv := range s.q {
		cb(sv)
	}
}

func (s *slaves) Len() (l int) {
	s.RLock()
	l = len(s.q)
	s.RUnlock()
	return
}

func (s *slaves) Update(remoteAddr string, cb func(*serverInfo)) {
	s.Lock()
	defer s.Unlock()

	if s.q == nil {
		s.q = map[string]*serverInfo{}
	}

	si := s.q[remoteAddr]
	if si != nil {
		cb(si)
		si.purgeTimer.Stop()
		si.purgeTimer.Reset(time.Minute)
		si.LastUpdateUnix = time.Now().Unix()
		return
	}

	p := &serverInfo{}
	p.RemoteAddr = remoteAddr
	p.LastUpdateUnix = time.Now().Unix()
	p.purgeTimer = time.AfterFunc(time.Minute, func() {
		s.Lock()
		defer s.Unlock()
		si := s.q[remoteAddr]
		if si != nil && time.Now().Unix()-si.LastUpdateUnix > 50 {
			delete(s.q, remoteAddr)
		}
	})
	cb(p)
	s.q[remoteAddr] = p
}
