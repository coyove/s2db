package main

import (
	"bytes"
	"container/heap"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/coyove/s2db/internal"
	"github.com/coyove/s2db/redisproto"
	"go.etcd.io/bbolt"
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

func hashCommands(in *redisproto.Command) (h [2]uint64) {
	h = [2]uint64{0, 5381}
	for _, buf := range in.Argv {
		for _, b := range buf {
			old := h[1]
			h[1] = h[1]*33 + uint64(b)
			if h[1] < old {
				h[0]++
			}
		}
		h[1]++
	}
	return h
}

func shardIndex(key string) int {
	return int(internal.HashStr(key) % ShardNum)
}

func restCommandsToKeys(i int, command *redisproto.Command) []string {
	keys := []string{}
	for ; i < command.ArgCount(); i++ {
		keys = append(keys, string(command.At(i)))
	}
	return keys
}

func writePairs(in []internal.Pair, w *redisproto.Writer, flags redisproto.Flags) error {
	data := make([]string, 0, len(in))
	for _, p := range in {
		data = append(data, p.Member)
		if flags.WITHSCORES || flags.WITHDATA {
			data = append(data, internal.FormatFloat(p.Score))
		}
		if flags.WITHDATA {
			data = append(data, string(p.Data))
		}
	}
	return w.WriteBulkStrings(data)
}

func sizeBytes(in [][]byte) int {
	sz := 1
	for _, p := range in {
		sz += len(p)
	}
	return sz
}

func sizePairs(in []internal.Pair) int {
	sz := 1
	for _, p := range in {
		sz += len(p.Member) + 8 + len(p.Data)
	}
	return sz
}

func (s *Server) fillPairsData(name string, in []internal.Pair) error {
	if len(in) == 0 {
		return nil
	}
	keys := make([]string, len(in))
	for i, el := range in {
		keys[i] = el.Member
	}
	data, err := s.ZMData(name, keys...)
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

func joinCommandString(cmd ...string) []byte {
	tmp := make([]struct {
		v   string
		cap int
	}, len(cmd))
	for i := range cmd {
		tmp[i].v = cmd[i]
		tmp[i].cap = len(cmd[i])
	}
	res := joinCommand(*(*[][]byte)(unsafe.Pointer(&tmp))...)
	runtime.KeepAlive(tmp)
	return res
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
			internal.PanicErr(err)
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
			fmt.Sprintf("sys_read_qps:%v", s.Survey.SysRead),
			fmt.Sprintf("sys_read_avg_lat:%v", s.Survey.SysReadLat.MeanString()),
			fmt.Sprintf("sys_write_qps:%v", s.Survey.SysWrite),
			fmt.Sprintf("sys_write_avg_lat:%v", s.Survey.SysWriteLat.MeanString()),
			fmt.Sprintf("sys_write_discards:%v", s.Survey.SysWriteDiscards.MeanString()),
			fmt.Sprintf("proxy_write_qps:%v", s.Survey.Proxy),
			fmt.Sprintf("proxy_write_avg_lat:%v", s.Survey.ProxyLat.MeanString()),
			"")
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
			fmt.Sprintf("cache_hit_qps:%v", s.Survey.Cache),
			fmt.Sprintf("cache_obj_count:%v", s.Cache.Len()),
			fmt.Sprintf("cache_size:%v", s.Cache.curWeight),
			fmt.Sprintf("weak_cache_hit_qps:%v", s.Survey.WeakCache),
			fmt.Sprintf("weak_cache_obj_count:%v", s.WeakCache.Len()),
			fmt.Sprintf("weak_cache_size:%v", s.WeakCache.Weight()),
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
	internal.PanicErr(err)
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
		stat := bk.Stats()
		tmp = append(tmp, "", "# log")
		inuse := stat.LeafInuse + stat.BranchInuse
		alloc := stat.LeafAlloc + stat.BranchAlloc
		tmp = append(tmp, fmt.Sprintf("log_count:%d", stat.KeyN))
		tmp = append(tmp, fmt.Sprintf("log_count_fast:%d", bk.KeyN()))
		tmp = append(tmp, fmt.Sprintf("log_tail:%d", bk.Sequence()))
		tmp = append(tmp, fmt.Sprintf("log_size:%d", inuse))
		tmp = append(tmp, fmt.Sprintf("log_alloc_size:%d", alloc))
		tmp = append(tmp, fmt.Sprintf("log_size_ratio:%.2f", float64(inuse)/float64(alloc)))
		myTail = bk.Sequence()
		return nil
	})
	minTail := uint64(math.MaxUint64)
	tmp = append(tmp, "", "# slave_log", fmt.Sprintf("slave_queue:%d", len(s.Slaves.q)))
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
		x := internal.MustParseFloatBytes(in.Argv[i+1])
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

type bigKeysHeap []internal.Pair

func (h bigKeysHeap) Len() int           { return len(h) }
func (h bigKeysHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h bigKeysHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *bigKeysHeap) Push(x interface{}) {
	*h = append(*h, x.(internal.Pair))
}

func (h *bigKeysHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (s *Server) BigKeys(n, shard int) map[string]int {
	if n <= 0 {
		n = 10
	}
	h := &bigKeysHeap{}
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
					heap.Push(h, internal.Pair{Member: "--z--" + string(name[5:]), Score: float64(bk.KeyN())})
				}
				if bytes.HasPrefix(name, []byte("q.")) {
					heap.Push(h, internal.Pair{Member: "--q--" + string(name[2:]), Score: float64(bk.KeyN())})
				}
				if h.Len() > n {
					heap.Pop(h)
				}
				return nil
			})
		})
	}
	x := map[string]int{}
	for h.Len() > 0 {
		p := heap.Pop(h).(internal.Pair)
		x[p.Member] = int(p.Score)
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
