package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"html/template"
	"io/ioutil"
	"math"
	"net"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/redisproto"
	"github.com/coyove/s2db/s2pkg"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

var (
	isReadCommand = map[string]bool{
		"ZSCORE": true, "ZMSCORE": true, "ZDATA": true, "ZMDATA": true,
		"ZCARD": true, "ZCOUNT": true, "ZCOUNTBYLEX": true,
		"ZRANK": true, "ZREVRANK": true, "ZRANGE": true, "ZREVRANGE": true,
		"ZRANGEBYLEX": true, "ZREVRANGEBYLEX": true, "ZRANGEBYSCORE": true, "ZREVRANGEBYSCORE": true,
		"SCAN": true,
	}
	isWriteCommand = map[string]bool{
		"DEL":  true,
		"ZREM": true, "ZREMRANGEBYLEX": true, "ZREMRANGEBYSCORE": true, "ZREMRANGEBYRANK": true,
		"ZADD": true, "ZINCRBY": true,
	}
)

func init() {
	redisproto.MaxBulkSize = 1 << 20
	redisproto.MaxNumArg = 10000
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
}

func checkScore(s float64) error {
	if math.IsNaN(s) {
		return fmt.Errorf("score is NaN")
	}
	return nil
}

func shardIndex(key string) int {
	return int(s2pkg.HashStr(key) % ShardLogNum)
}

func restCommandsToKeys(i int, command *redisproto.Command) (keys []string) {
	for ; i < command.ArgCount(); i++ {
		keys = append(keys, string(command.At(i)))
	}
	return keys
}

func redisPairs(in []s2pkg.Pair, flags redisproto.Flags) []string {
	data := make([]string, 0, len(in))
	for _, p := range in {
		data = append(data, p.Member)
		if flags.WITHSCORES || flags.WITHDATA {
			data = append(data, s2pkg.FormatFloat(p.Score))
		}
		if flags.WITHDATA {
			data = append(data, string(p.Data))
		}
	}
	return data
}

func (s *Server) fillPairsData(key string, in []s2pkg.Pair) error {
	if len(in) == 0 {
		return nil
	}
	keys := make([]string, len(in))
	for i, el := range in {
		keys[i] = el.Member
	}
	data, err := s.ZMData(key, keys)
	if err != nil {
		return err
	}
	for i := range in {
		in[i].Data = data[i]
	}
	return nil
}

func dumpCommand(cmd *redisproto.Command) []byte {
	return joinCommand(cmd.Argv)
}

func splitCommand(buf []byte) (*redisproto.Command, error) {
	tmp := &s2pkg.BytesArray{}
	if err := tmp.UnmarshalBytes(buf); err != nil {
		return nil, err
	}
	return &redisproto.Command{Argv: tmp.Data}, nil
}

func joinCommand(cmd [][]byte) []byte {
	buf := []byte{0x95, 0, 0, 0, 0} // version: 0x95 + signature: 4b
	rand.Reader.Read(buf[1:])
	buf = (&s2pkg.BytesArray{Data: cmd}).MarshalAppend(buf)
	h := crc32.NewIEEE()
	h.Write(buf[5:])
	return h.Sum(buf) // crc32: 4b
}

func joinCommandEmpty() []byte {
	return crc32.NewIEEE().Sum([]byte{0x95, 0, 0, 0, 0})
}

func (s *Server) InfoCommand(section string) (data []string) {
	if section == "" || section == "server" {
		data = append(data, "# server",
			fmt.Sprintf("version:%v", Version),
			fmt.Sprintf("servername:%v", s.ServerName),
			fmt.Sprintf("listen:%v", s.ln.Addr()),
			fmt.Sprintf("listen_unix:%v", s.lnLocal.Addr()),
			fmt.Sprintf("uptime:%v", time.Since(s.Survey.StartAt)),
			fmt.Sprintf("readonly:%v", btoi(s.ReadOnly)),
			fmt.Sprintf("mark_master:%v", s.MarkMaster),
			fmt.Sprintf("passthrough:%v", s.Passthrough),
			fmt.Sprintf("connections:%v", s.Survey.Connections),
			"")
	}
	if section == "" || section == "server_misc" {
		dataSize := 0
		dataFiles, _ := ioutil.ReadDir(s.DBPath)
		for _, fi := range dataFiles {
			dataSize += int(fi.Size())
		}
		cwd, _ := os.Getwd()
		data = append(data, "# server_misc",
			fmt.Sprintf("cwd:%v", cwd),
			fmt.Sprintf("args:%v", strings.Join(os.Args, " ")),
			fmt.Sprintf("data_size:%d", dataSize),
			fmt.Sprintf("data_size_mb:%.2f", float64(dataSize)/1024/1024),
			"")
	}
	if section == "" || section == "replication" {
		data = append(data, "# replication")
		tails := [ShardLogNum]uint64{}
		for i := range s.shards {
			tails[i] = s.ShardLogtail(i)
		}
		data = append(data, fmt.Sprintf("logtail:%v", joinArray(tails)))
		if s.Slave.Redis() != nil {
			diffs := [ShardLogNum]string{}
			for i := range s.shards {
				diffs[i] = clock.IdDiff(s.ShardLogtail(i), s.Slave.Logtails[i])
			}
			data = append(data,
				fmt.Sprintf("slave_conn:%v", s.Slave.Config().Raw),
				fmt.Sprintf("slave_ack:%v", s.Slave.IsAcked(s)),
				fmt.Sprintf("slave_ack_before:%v", s.Slave.AckBefore()),
				fmt.Sprintf("slave_logtail:%v", joinArray(s.Slave.Logtails)),
				fmt.Sprintf("slave_logtail_diff:%v", joinArray(diffs)),
			)
		}
		if s.MasterIP != "" {
			data = append(data, fmt.Sprintf("master_ip:%v", s.MasterIP))
		}
		data = append(data, "")
	}
	if section == "" || section == "sys_rw_stats" {
		data = append(data, "# sys_rw_stats",
			fmt.Sprintf("sys_read_qps:%v", s.Survey.SysRead.String()),
			fmt.Sprintf("sys_read_avg_lat:%v", s.Survey.SysRead.MeanString()),
			fmt.Sprintf("sys_write_qps:%v", s.Survey.SysWrite.String()),
			fmt.Sprintf("sys_write_avg_lat:%v", s.Survey.SysWrite.MeanString()),
			fmt.Sprintf("sys_write_discards:%v", s.Survey.SysWriteDiscards.MeanString()),
			fmt.Sprintf("slow_logs_qps:%v", s.Survey.SlowLogs.QPSString()),
			fmt.Sprintf("slow_logs_avg_lat:%v", s.Survey.SlowLogs.MeanString()),
			fmt.Sprintf("sync_avg_lat:%v", s.Survey.Sync.MeanString()),
			"")
	}
	if section == "" || section == "command_qps" || section == "command_avg_lat" {
		var keys []string
		s.Survey.Command.Range(func(k, v interface{}) bool { keys = append(keys, k.(string)); return true })
		sort.Strings(keys)
		add := func(f func(*s2pkg.Survey) string) (res []string) {
			for _, k := range keys {
				v, _ := s.Survey.Command.Load(k)
				res = append(res, fmt.Sprintf("%v:%v", k, f(v.(*s2pkg.Survey))))
			}
			return append(res, "")
		}
		if section == "" || section == "command_avg_lat" {
			data = append(data, "# command_avg_lat")
			data = append(data, add(func(s *s2pkg.Survey) string { return s.MeanString() })...)
		}
		if section == "" || section == "command_qps" {
			data = append(data, "# command_qps")
			data = append(data, add(func(s *s2pkg.Survey) string { return s.QPSString() })...)
		}
	}
	if section == "" || section == "batch" {
		data = append(data, "# batch",
			fmt.Sprintf("batch_size:%v", s.Survey.BatchSize.MeanString()),
			fmt.Sprintf("batch_lat:%v", s.Survey.BatchLat.MeanString()),
			fmt.Sprintf("batch_size_slave:%v", s.Survey.BatchSizeSv.MeanString()),
			fmt.Sprintf("batch_lat_slave:%v", s.Survey.BatchLatSv.MeanString()),
			"")
	}
	if section == "" || section == "cache" {
		data = append(data, "# cache",
			fmt.Sprintf("cache_avg_size:%v", s.Survey.CacheSize.MeanString()),
			fmt.Sprintf("cache_req_qps:%v", s.Survey.CacheReq),
			fmt.Sprintf("cache_hit_qps:%v", s.Survey.CacheHit),
			fmt.Sprintf("weak_cache_hit_qps:%v", s.Survey.WeakCacheHit),
			fmt.Sprintf("cache_obj_count:%v/%v", s.Cache.Len(), s.Cache.Cap()),
			fmt.Sprintf("weak_cache_obj_count:%v/%v", s.WeakCache.Len(), s.WeakCache.Cap()),
			"")
	}
	return
}

func (s *Server) ShardLogInfoCommand(shard int) []string {
	x := &s.shards[shard]
	logtail, logSpan, compacted := s.ShardLogInfo(shard)
	tmp := []string{
		fmt.Sprintf("# log%d", shard),
		fmt.Sprintf("logtail:%d", logtail),
		fmt.Sprintf("batch_queue:%v", len(x.batchTx)),
		fmt.Sprintf("sync_waiter:%v", x.syncWaiter),
		fmt.Sprintf("timespan:%d", logSpan),
		fmt.Sprintf("compacted:%v", compacted),
	}
	if s.Slave.Redis() != nil {
		tail := s.Slave.Logtails[shard]
		tmp = append(tmp, fmt.Sprintf("slave_logtail:%d", tail))
		tmp = append(tmp, fmt.Sprintf("slave_logtail_diff:%v", clock.IdDiff(logtail, tail)))
	}
	tmp = append(tmp, "")
	return tmp //strings.Join(tmp, "\r\n") + "\r\n"
}

func (s *Server) waitSlave() {
	if !s.Slave.IsAcked(s) {
		log.Info("waitSlave: no need to wait")
		return
	}
	s.ReadOnly = true
	for start := time.Now(); time.Since(start).Seconds() < 0.5; {
		var ok int
		for i := 0; i < ShardLogNum; i++ {
			if s.ShardLogtail(i) == s.Slave.Logtails[i] {
				ok++
			}
		}
		if ok == ShardLogNum {
			return
		}
	}
	log.Error("waitSlave: timeout")
}

func ifZero(v *int, v2 int) {
	if *v <= 0 {
		*v = v2
	}
}

const (
	RunNormal = iota + 1
	RunDefer
	RunSync
)

func parseRunFlag(in *redisproto.Command) int {
	if len(in.Argv) > 2 {
		if bytes.EqualFold(in.Argv[2], []byte("--defer--")) {
			in.Argv = append(in.Argv[:2], in.Argv[3:]...)
			return RunDefer
		}
		if bytes.EqualFold(in.Argv[2], []byte("--sync--")) {
			in.Argv = append(in.Argv[:2], in.Argv[3:]...)
			return RunSync
		}
		if bytes.EqualFold(in.Argv[2], []byte("--normal--")) {
			in.Argv = append(in.Argv[:2], in.Argv[3:]...)
		}
	}
	return RunNormal
}

func parseWeakFlag(in *redisproto.Command) time.Duration {
	i := in.ArgCount() - 2
	if i >= 2 && in.EqualFold(i, "WEAK") {
		x := s2pkg.MustParseFloatBytes(in.Argv[i+1])
		in.Argv = in.Argv[:i]
		return time.Duration(int64(x*1e6) * 1e3)
	}
	return 0
}

func joinArray(v interface{}) string {
	rv := reflect.ValueOf(v)
	p := make([]string, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		p = append(p, fmt.Sprint(rv.Index(i).Interface()))
	}
	return strings.Join(p, " ")
}

func (s *Server) BigKeys(n, shard int) []s2pkg.Pair {
	return nil
	// if n <= 0 {
	// 	n = 10
	// }
	// h := &s2pkg.PairHeap{}
	// heap.Init(h)
	// for i := range s.shards {
	// 	if shard != -1 && i != shard {
	// 		continue
	// 	}
	// 	s.shards[i].View(func(tx *bbolt.Tx) error {
	// 		return tx.ForEach(func(name []byte, bk *bbolt.Bucket) error {
	// 			if bytes.HasPrefix(name, []byte("zset.score.")) {
	// 				return nil
	// 			}
	// 			if bytes.HasPrefix(name, []byte("zset.")) {
	// 				heap.Push(h, s2pkg.Pair{Member: string(name[5:]), Score: float64(sizeOfBucket(bk))})
	// 			}
	// 			if bytes.HasPrefix(name, []byte("q.")) {
	// 				_, _, l := queueLenImpl(bk)
	// 				heap.Push(h, s2pkg.Pair{Member: string(name[2:]), Score: float64(l)})
	// 			}
	// 			if h.Len() > n {
	// 				heap.Pop(h)
	// 			}
	// 			return nil
	// 		})
	// 	})
	// }
	// var x []s2pkg.Pair
	// for h.Len() > 0 {
	// 	p := heap.Pop(h).(s2pkg.Pair)
	// 	x = append(x, p)
	// }
	// return x
}

func getRemoteIP(addr net.Addr) net.IP {
	switch addr := addr.(type) {
	case *net.TCPAddr:
		return addr.IP
	case *net.UnixAddr:
		return net.IPv4(127, 0, 0, 1)
	}
	return net.IPv4bcast
}

func (s *Server) removeCache(key string) {
	s.Cache.Delete(key)
}

func (s *Server) getCache(h string, ttl time.Duration) interface{} {
	s.Survey.CacheReq.Incr(1)
	v, ok := s.Cache.Get(h)
	if ok {
		s.Survey.CacheHit.Incr(1)
		return v
	}
	if ttl == 0 {
		return nil
	}
	v, ok = s.WeakCache.Get(h)
	if !ok {
		return nil
	}
	if i := v.(*s2pkg.WeakCacheItem); time.Since(time.Unix(i.Time, 0)) <= ttl {
		s.Survey.WeakCacheHit.Incr(1)
		return i.Data
	}
	return nil
}

func (s *Server) addCache(key string, h string, data interface{}, wm int64) {
	var sz int
	switch data := data.(type) {
	case []s2pkg.Pair:
		sz = s2pkg.SizePairs(data)
	case [][]byte:
		sz = s2pkg.SizeBytes(data)
	}
	if sz > 0 {
		if sz > s.CacheObjMaxSize*1024 {
			log.Infof("omit big key cache: %q->%q (%db)", key, h, sz)
			return
		}
		s.Survey.CacheSize.Incr(int64(sz))
	}
	s.Cache.Add(key, h, data, wm)
	s.WeakCache.Add("", h, &s2pkg.WeakCacheItem{Data: data, Time: time.Now().Unix()}, 0)
}

func makeHTMLStat(s string) template.HTML {
	var a, b, c float64
	if n, _ := fmt.Sscanf(s, "%f %f %f", &a, &b, &c); n != 3 {
		return template.HTML(s)
	}
	return template.HTML(fmt.Sprintf("%s&nbsp;&nbsp;%s&nbsp;&nbsp;%s", s2pkg.FormatFloatShort(a), s2pkg.FormatFloatShort(b), s2pkg.FormatFloatShort(c)))
}

func btoi(v bool) int {
	if v {
		return 1
	}
	return 0
}

func waitLimiter(lm *rate.Limiter) {
	if lm != nil {
		lm.Wait(context.TODO())
	}
}

func incrBytes(b []byte) []byte {
	b = dupBytes(b)
	for i := len(b) - 1; i >= 0; i-- {
		b[i]++
		if b[i] != 0 {
			break
		}
	}
	return b
}

func dupBytes(b []byte) []byte {
	return append([]byte{}, b...)
}

func bAppendUint64(b []byte, v uint64) []byte {
	return append(dupBytes(b), s2pkg.Uint64ToBytes(v)...)
}

func GetKeyCopy(db s2pkg.Storage, key []byte) ([]byte, error) {
	buf, rd, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer rd.Close()
	return dupBytes(buf), nil
}

func GetKeyNumber(db s2pkg.Storage, key []byte) (float64, uint64, bool, error) {
	buf, rd, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	defer rd.Close()
	if len(buf) != 8 {
		return 0, 0, false, fmt.Errorf("invalid number bytes (8)")
	}
	return s2pkg.BytesToFloat(buf), s2pkg.BytesToUint64(buf), true, nil
}

func IncrKey(db s2pkg.Storage, key []byte, v int64) error {
	buf, rd, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return db.Set(key, s2pkg.Uint64ToBytes(uint64(v)), pebble.Sync)
		}
		return err
	}
	old := int64(s2pkg.BytesToUint64(buf))
	rd.Close()
	old += v
	if old == 0 {
		return db.Delete(key, pebble.Sync)
	}
	return db.Set(key, s2pkg.Uint64ToBytes(uint64(old)), pebble.Sync)
}

func (s *Server) createDBListener() pebble.EventListener {
	return pebble.EventListener{
		BackgroundError: func(a error) {
			dbLogger.Error(a)
			s.runScriptFunc("PebbleBackgroundError", a)
		},
		CompactionBegin: func(a pebble.CompactionInfo) {
			dbLogger.Info("[CompactionBegin] ", a)
			s.runScriptFunc("PebbleCompactionBegin", a)
		},
		CompactionEnd: func(a pebble.CompactionInfo) {
			dbLogger.Info("[CompactionEnd] ", a)
			s.runScriptFunc("PebbleCompactionEnd", a)
		},
		DiskSlow: func(a pebble.DiskSlowInfo) {
			dbLogger.Info("[DiskSlow] ", a)
			s.runScriptFunc("PebbleDiskSlow", a)
		},
		FlushBegin: func(a pebble.FlushInfo) {
			dbLogger.Info("[FlushBegin] ", a)
			s.runScriptFunc("PebbleFlushBegin", a)
		},
		FlushEnd: func(a pebble.FlushInfo) {
			dbLogger.Info("[FlushEnd] ", a)
			s.runScriptFunc("PebbleFlushEnd", a)
		},
		FormatUpgrade: func(a pebble.FormatMajorVersion) {
			dbLogger.Info("[FormatUpgrade] ", a)
			s.runScriptFunc("PebbleFormatUpgrade", a)
		},
		ManifestCreated: func(a pebble.ManifestCreateInfo) {
			dbLogger.Info("[ManifestCreated] ", a)
			s.runScriptFunc("PebbleManifestCreated", a)
		},
		ManifestDeleted: func(a pebble.ManifestDeleteInfo) {
			dbLogger.Info("[ManifestDeleted] ", a)
			s.runScriptFunc("PebbleManifestDeleted", a)
		},
		TableCreated: func(a pebble.TableCreateInfo) {
			// dbLogger.Info("[TableCreated] ", a)
			s.runScriptFunc("PebbleTableCreated", a)
		},
		TableDeleted: func(a pebble.TableDeleteInfo) {
			// dbLogger.Info("[TableDeleted] ", a)
			s.runScriptFunc("PebbleTableDeleted", a)
		},
		TableIngested: func(a pebble.TableIngestInfo) {
			dbLogger.Info("[TableIngested] ", a)
			s.runScriptFunc("PebbleTableIngested", a)
		},
		TableStatsLoaded: func(a pebble.TableStatsInfo) {
			dbLogger.Info("[TableStatsLoaded] ", a)
			s.runScriptFunc("PebbleTableStatsLoaded", a)
		},
		TableValidated: func(a pebble.TableValidatedInfo) {
			dbLogger.Info("[TableValidated] ", a)
			s.runScriptFunc("PebbleTableValidated", a)
		},
		WALCreated: func(a pebble.WALCreateInfo) {
			dbLogger.Info("[WALCreated] ", a)
			s.runScriptFunc("PebbleWALCreated", a)
		},
		WALDeleted: func(a pebble.WALDeleteInfo) {
			dbLogger.Info("[WALDeleted] ", a)
			s.runScriptFunc("PebbleWALDeleted", a)
		},
		WriteStallBegin: func(a pebble.WriteStallBeginInfo) {
			dbLogger.Info("[WriteStallBegin] ", a)
			s.runScriptFunc("PebbleWriteStallBegin", a)
		},
		WriteStallEnd: func() { dbLogger.Info("WriteStallEnd") },
	}
}
