package main

import (
	"bytes"
	"container/heap"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"html/template"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/s2db/redisproto"
	"github.com/coyove/s2db/s2pkg"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

var (
	isReadCommand = map[string]bool{
		"ZSCORE": true, "ZMSCORE": true, "ZMDATA": true,
		"ZCARD": true, "ZCOUNT": true, "ZCOUNTBYLEX": true,
		"ZRANK": true, "ZREVRANK": true, "ZRANGE": true, "ZREVRANGE": true,
		"ZRANGEBYLEX": true, "ZREVRANGEBYLEX": true, "ZRANGEBYSCORE": true, "ZREVRANGEBYSCORE": true,
		"GEORADIUS": true, "GEORADIUS_RO": true,
		"GEORADIUSBYMEMBER": true, "GEORADIUSBYMEMBER_RO": true,
		"GEODIST": true, "GEOPOS": true,
		"SCAN": true,
		"QLEN": true, "QHEAD": true, "QINDEX": true, "QSCAN": true,
	}
	isWriteCommand = map[string]bool{
		"UNLINK": true, "DEL": true,
		"ZREM": true, "ZREMRANGEBYLEX": true, "ZREMRANGEBYSCORE": true, "ZREMRANGEBYRANK": true,
		"ZADD": true, "ZINCRBY": true, "QAPPEND": true,
		"IDXADD": true, "IDXDEL": true,
	}
)

func init() {
	redisproto.MaxBulkSize = 1 << 20
	redisproto.MaxNumArg = 10000
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	rand.Seed(time.Now().Unix())
}

func checkScore(s float64) error {
	if math.IsNaN(s) {
		return fmt.Errorf("score is NaN")
	}
	return nil
}

func shardIndex(key string) int {
	return int(s2pkg.HashStr(key) % ShardNum)
}

func restCommandsToKeys(i int, command *redisproto.Command) []string {
	keys := []string{}
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
	data, err := s.ZMData(key, keys, redisproto.Flags{})
	if err != nil {
		return err
	}
	for i := range in {
		in[i].Data = data[i]
	}
	return nil
}

func dumpCommand(cmd *redisproto.Command) []byte {
	return joinCommand(cmd.Argv...)
}

func splitCommand(in string) (*redisproto.Command, error) {
	command := &redisproto.Command{}
	buf, _ := base64.URLEncoding.DecodeString(in)
	err := gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&command.Argv)
	return command, err
}

func joinCommand(cmd ...[]byte) []byte {
	buf := &bytes.Buffer{}
	buf.WriteByte(0x93)
	gob.NewEncoder(buf).Encode(cmd)
	return buf.Bytes()
}

func (s *Server) Info(section string) (data []string) {
	if section == "" || section == "server" {
		data = append(data, "# server",
			fmt.Sprintf("version:%v", Version),
			fmt.Sprintf("servername:%v", s.ServerName),
			fmt.Sprintf("listen:%v", s.ln.Addr().String()),
			fmt.Sprintf("uptime:%v", time.Since(s.Survey.StartAt)),
			fmt.Sprintf("readonly:%v", s.ReadOnly),
			fmt.Sprintf("connections:%v", s.Survey.Connections),
			"")
	}
	if section == "" || section == "server_misc" {
		sz, dataSize := 0, 0
		fls := []string{}
		for i := range s.db {
			fi, err := os.Stat(s.db[i].Path())
			s2pkg.PanicErr(err)
			sz += int(fi.Size())
			fls = append(fls, strconv.Itoa(s.db[i].FreelistSize()/1024))
		}
		dataFiles, _ := ioutil.ReadDir(filepath.Dir(s.ConfigDB.Path()))
		for _, fi := range dataFiles {
			dataSize += int(fi.Size())
		}
		cwd, _ := os.Getwd()
		data = append(data, "# server_misc",
			fmt.Sprintf("cwd:%v", cwd),
			fmt.Sprintf("args:%v", strings.Join(os.Args, " ")),
			fmt.Sprintf("db_freelist_size:%v", strings.Join(fls, " ")),
			fmt.Sprintf("db_size:%v", sz),
			fmt.Sprintf("db_size_mb:%.2f", float64(sz)/1024/1024),
			fmt.Sprintf("configdb_size_mb:%.2f", float64(s.ConfigDB.Size())/1024/1024),
			fmt.Sprintf("data_size_mb:%.2f", float64(dataSize)/1024/1024),
			"")
	}
	if section == "" || section == "replication" {
		data = append(data, "# replication",
			fmt.Sprintf("master_mode:%v", s.MasterMode),
			fmt.Sprintf("master:%v", s.MasterAddr),
			fmt.Sprintf("master_name:%v", s.Master.ServerName),
			fmt.Sprintf("master_version:%v", s.Master.Version),
			fmt.Sprintf("slaves:%v", s.Slaves.Len()),
			"")
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
			fmt.Sprintf("proxy_write_qps:%v", s.Survey.Proxy.String()),
			fmt.Sprintf("proxy_write_avg_lat:%v", s.Survey.Proxy.MeanString()),
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
	if section == "" {
		data = append(data, s.SlaveInfo(section)...)
	}
	return
}

func (s *Server) ShardInfo(shard int) []string {
	x := &s.db[shard]
	fi, err := os.Stat(x.Path())
	s2pkg.PanicErr(err)
	tmp := []string{
		fmt.Sprintf("# shard%d", shard),
		fmt.Sprintf("path:%v", x.Path()),
		fmt.Sprintf("freelist_size:%v", x.FreelistSize()),
		fmt.Sprintf("freelist_dist_debug:%v", x.FreelistDistribution()),
		fmt.Sprintf("db_size:%v", fi.Size()),
		fmt.Sprintf("db_size_mb:%.2f", float64(fi.Size())/1024/1024),
		fmt.Sprintf("batch_queue:%v", strconv.Itoa(len(x.batchTx))),
	}
	var myTail uint64
	x.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("wal"))
		if bk == nil {
			return nil
		}
		firstKey, _ := bk.Cursor().First()
		first := s2pkg.BytesToUint64(firstKey)
		tmp = append(tmp, fmt.Sprintf("log_count:%d", bk.Sequence()-first+1))
		tmp = append(tmp, fmt.Sprintf("log_tail:%d", bk.Sequence()))
		myTail = bk.Sequence()
		return nil
	})
	minTail := uint64(math.MaxUint64)
	tmp = append(tmp, fmt.Sprintf("slave_queue:%d", len(s.Slaves.q)))
	s.Slaves.Foreach(func(si *serverInfo) {
		tail := si.LogTails[shard]
		if tail < minTail {
			minTail = tail
		}
		tmp = append(tmp, fmt.Sprintf("slave_%v_logtail:%d", si.RemoteAddr, tail))
	})
	tmp = append(tmp, fmt.Sprintf("slave_logtail_min:%d", minTail))
	tmp = append(tmp, fmt.Sprintf("slave_logtail_diff:%d", int64(myTail)-int64(minTail)))
	tmp = append(tmp, "")
	return tmp //strings.Join(tmp, "\r\n") + "\r\n"
}

func (s *Server) WaitFirstSlaveCatchUp(timeout float64) (*serverInfo, error) {
	_, mine, _, err := s.myLogTails()
	if err != nil {
		return nil, err
	}
	for start := time.Now(); time.Since(start).Seconds() < timeout; {
		var first *serverInfo
		s.Slaves.Foreach(func(si *serverInfo) {
			var total uint64
			for _, v := range si.LogTails {
				total += v
			}
			if total == mine {
				first = si
			}
		})
		if first != nil {
			return first, nil
		}
	}
	return nil, nil
}

func ifZero(v *int, v2 int) {
	if *v <= 0 {
		*v = v2
	}
}

func parseDeferFlag(in *redisproto.Command) bool {
	if len(in.Argv) > 2 && bytes.EqualFold(in.Argv[2], []byte("--defer--")) {
		in.Argv = append(in.Argv[:2], in.Argv[3:]...)
		return true
	}
	return false
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

func (s *Server) TypeofKey(key string) (t string) {
	t = "none"
	s.pick(key).View(func(tx *bbolt.Tx) error {
		if bk := tx.Bucket([]byte("zset.score." + key)); bk != nil {
			t = "zset"
		}
		if bk := tx.Bucket([]byte("q." + key)); bk != nil {
			t = "queue"
		}
		return nil
	})
	return
}

func (s *Server) BigKeys(n, shard int) []s2pkg.Pair {
	if n <= 0 {
		n = 10
	}
	h := &s2pkg.PairHeap{}
	heap.Init(h)
	for i := range s.db {
		if shard != -1 && i != shard {
			continue
		}
		s.db[i].View(func(tx *bbolt.Tx) error {
			return tx.ForEach(func(name []byte, bk *bbolt.Bucket) error {
				if bytes.HasPrefix(name, []byte("zset.score.")) {
					return nil
				}
				if bytes.HasPrefix(name, []byte("zset.")) {
					heap.Push(h, s2pkg.Pair{Member: string(name[5:]), Score: float64(sizeOfBucket(bk))})
				}
				if bytes.HasPrefix(name, []byte("q.")) {
					_, _, l := queueLenImpl(bk)
					heap.Push(h, s2pkg.Pair{Member: string(name[2:]), Score: float64(l)})
				}
				if h.Len() > n {
					heap.Pop(h)
				}
				return nil
			})
		})
	}
	var x []s2pkg.Pair
	for h.Len() > 0 {
		p := heap.Pop(h).(s2pkg.Pair)
		x = append(x, p)
	}
	return x
}

func getRemoteIP(addr net.Addr) net.IP {
	tcp, _ := addr.(*net.TCPAddr)
	if tcp == nil {
		return net.IPv4bcast
	}
	return tcp.IP
}

func closeAllReadTxs(txs []*bbolt.Tx) {
	for _, tx := range txs {
		if tx != nil {
			tx.Rollback()
		}
	}
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

func (s *Server) addCache(key string, h string, data interface{}) {
	var sz int
	switch data := data.(type) {
	case []s2pkg.Pair:
		sz = s2pkg.SizePairs(data)
	case [][]byte:
		sz = s2pkg.SizeBytes(data)
	}
	if sz > 0 {
		if sz > s.CacheObjMaxSize*1024 {
			logrus.Infof("omit big key cache: %q->%q (%db)", key, h, sz)
			return
		}
		s.Survey.CacheSize.Incr(int64(sz))
	}
	s.Cache.Add(key, h, data)
	s.WeakCache.Add("", h, &s2pkg.WeakCacheItem{Data: data, Time: time.Now().Unix()})
}

func makeHTMLStat(s string) template.HTML {
	var a, b, c float64
	if n, _ := fmt.Sscanf(s, "%f %f %f", &a, &b, &c); n != 3 {
		return template.HTML(s)
	}
	trilabel := func(a, b, c float64) (ap, bp, cp string) {
		text := [3]string{"stat1", "stat5", "stat15"}
		if (a == 0 && b == 0 && c == 0) || (a != a && b != b && c != c) {
			return text[0], text[0], text[0]
		}
		if math.Abs(a-b)/a < 0.01 && math.Abs(a-c)/a < 0.01 {
			return text[0], text[0], text[0]
		}
		x := [3][2]interface{}{{&ap, a}, {&bp, b}, {&cp, c}}
		sort.Slice(x[:], func(i, j int) bool { return x[i][1].(float64) < x[j][1].(float64) })
		*x[0][0].(*string), *x[1][0].(*string), *x[2][0].(*string) = text[0], text[1], text[2]
		return
	}
	al, bl, cl := trilabel(a, b, c)
	return template.HTML(fmt.Sprintf("<div class=stat><div class=%s>%s</div><div class=%s>%s</div><div class=%s>%s</div></div>",
		al, s2pkg.FormatFloatShort(a), bl, s2pkg.FormatFloatShort(b), cl, s2pkg.FormatFloatShort(c)))
}

func sizeOfBucket(bk *bbolt.Bucket) int64 {
	if seq := bk.Sequence(); seq > 0 {
		return int64(seq)
	}
	return int64(bk.KeyN())
}
