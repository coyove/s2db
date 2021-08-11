package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"sort"
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
	for _, p := range s.slaves.Take(time.Minute) {
		si := &serverInfo{}
		json.Unmarshal(p.Data, si)
		lt := int64(0)
		for i, t := range si.LogTails {
			lt += int64(t)
			diffs[i] = int64(tails[i]) - int64(t)
		}
		data = append(data,
			"slave_"+p.Key+"_name:"+si.ServerName,
			"slave_"+p.Key+"_version:"+si.Version,
			"slave_"+p.Key+"_listen:"+si.ListenAddr,
			"slave_"+p.Key+"_logtail:"+joinArray(si.LogTails),
			fmt.Sprintf("slave_%s_logtail_diff_sum:%d", p.Key, int64(combined)-lt),
			fmt.Sprintf("slave_%s_logtail_diff:%v", p.Key, joinArray(diffs)),
		)
		names = append(names, p.Key)
	}
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
		// if s.master.Version > Version {
		// 	log.Error("ping: master version too high: ", s.master.Version, ">", Version)
		// 	time.Sleep(time.Second * 10)
		// 	continue
		// }

	AGAIN:
		myWalIndex, err := s.myLogTail(shard)
		if err != nil {
			log.Error("read local log index: ", err)
			if err == bbolt.ErrDatabaseNotOpen {
				time.Sleep(time.Second * 5)
				goto AGAIN
			}
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

func (s *Server) responseLog(shard int, start uint64) (logs []string, err error) {
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
			if len(logs) >= s.ResponseLogRun || sz > s.ResponseLogSize*1024 {
				break
			}
		}
		return nil
	})
	return
}

// func (s *Server) purgeLog(shard int, head int64) (int, int, error) {
// 	if head == 0 {
// 		return 0, 0, fmt.Errorf("head is zero")
// 	}
// 	if head < 0 && -head > 10000 {
// 		return 0, 0, fmt.Errorf("neg-head is too large")
// 	}
// 	oldCount, count := 0, 0
// 	if err := s.db[shard].Update(func(tx *bbolt.Tx) error {
// 		bk := tx.Bucket([]byte("wal"))
// 		if bk == nil {
// 			return nil
// 		}
//
// 		oldCount = bk.Stats().KeyN
// 		if oldCount == 0 {
// 			return nil
// 		}
// 		c := bk.Cursor()
// 		last, _ := c.Last()
// 		if len(last) != 8 {
// 			return fmt.Errorf("invalid last key, fatal error")
// 		}
// 		tail := int64(binary.BigEndian.Uint64(last))
// 		if head < 0 {
// 			head = tail + head
// 			if head < 1 {
// 				head = 1
// 			}
// 		}
// 		if head >= tail {
// 			return fmt.Errorf("truncate head over tail")
// 		}
// 		if tail-head > 10000 {
// 			return fmt.Errorf("too much gap, purging aborted")
// 		}
// 		var min uint64 = math.MaxUint64
// 		for _, sv := range s.slaves.Take(time.Minute) {
// 			si := &serverInfo{}
// 			json.Unmarshal(sv.Data, si)
// 			if si.LogTails[shard] < min {
// 				min = si.LogTails[shard]
// 			}
// 		}
// 		if min != math.MaxUint64 {
// 			// If master have any slaves, it can't purge logs which slaves don't have yet
// 			// This is the best effort we can make because slaves maybe offline so it is still possible to over-purge
// 			if head > int64(min) {
// 				return fmt.Errorf("truncate too much: %d (slave rejection: %d)", head, min)
// 			}
// 		}
//
// 		keepLogs := [][2][]byte{}
// 		for i := head; i <= tail; i++ {
// 			k := intToBytes(uint64(i))
// 			v := append([]byte{}, bk.Get(k)...)
// 			keepLogs = append(keepLogs, [2][]byte{k, v})
// 			count++
// 		}
// 		if len(keepLogs) == 0 {
// 			return fmt.Errorf("keep zero logs, fatal error")
// 		}
// 		if err := tx.DeleteBucket([]byte("wal")); err != nil {
// 			return err
// 		}
// 		bk, err := tx.CreateBucket([]byte("wal"))
// 		if err != nil {
// 			return err
// 		}
// 		for _, p := range keepLogs {
// 			if err := bk.Put(p[0], p[1]); err != nil {
// 				return err
// 			}
// 		}
// 		return bk.SetSequence(uint64(tail))
// 	}); err != nil {
// 		return 0, 0, err
// 	}
// 	return count, oldCount, nil
// }

type slaves struct {
	sync.Mutex
	Slaves []Pair
}

type serverInfo struct {
	LogTails   [ShardNum]uint64 `json:"logtails"`
	ListenAddr string           `json:"listen"`
	ServerName string           `json:"servername"`
	Version    string           `json:"version"`
}

func (s *slaves) Take(t time.Duration) []Pair {
	s.Lock()
	defer s.Unlock()

	now := time.Now()
	for i, sv := range s.Slaves {
		if now.Sub(time.Unix(int64(sv.Score), 0)) > t {
			return append([]Pair{}, s.Slaves[:i]...)
		}
	}
	return append([]Pair{}, s.Slaves...)
}

func (s *slaves) Update(ip string, update func(*serverInfo)) {
	p := Pair{Key: ip, Score: float64(time.Now().Unix())}

	s.Lock()
	defer s.Unlock()

	found := false
	for i, sv := range s.Slaves {
		if sv.Key == p.Key {
			info := &serverInfo{}
			json.Unmarshal(sv.Data, info)
			update(info)
			p.Data, _ = json.Marshal(info)
			s.Slaves[i] = p
			found = true
			break
		}
	}
	if !found {
		info := &serverInfo{}
		update(info)
		p.Data, _ = json.Marshal(info)
		s.Slaves = append(s.Slaves, p)
	}
	sort.Slice(s.Slaves, func(i, j int) bool {
		return s.Slaves[i].Score > s.Slaves[j].Score
	})
}
