package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/coyove/common/lru"
	"github.com/coyove/common/sched"
	"github.com/secmask/go-redisproto"
	log "github.com/sirupsen/logrus"
	"gitlab.litatom.com/zhangzezhong/zset/calc"
	"go.etcd.io/bbolt"
)

func init() {
	redisproto.MaxBulkSize = 1 << 20
	redisproto.MaxNumArg = 10000
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	sched.Verbose = false
	rand.Seed(time.Now().Unix())
}

func checkScore(s float64) error {
	if math.IsNaN(s) {
		return fmt.Errorf("score is NaN")
	}
	return nil
}

func intToBytes(i uint64) []byte {
	v := [8]byte{}
	binary.BigEndian.PutUint64(v[:], i)
	return v[:]
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func bytesToFloatZero(b []byte) float64 {
	if len(b) != 8 {
		return 0
	}
	return bytesToFloat(b)
}

func bytesToFloat(b []byte) float64 {
	x := binary.BigEndian.Uint64(b)
	if x>>63 == 1 {
		x = x << 1 >> 1
	} else {
		x = ^x
	}
	return math.Float64frombits(x)
}

func floatToBytes(v float64) []byte {
	x := math.Float64bits(v)
	if v >= 0 {
		x |= 1 << 63
	} else {
		x = ^x
	}
	tmp := [8]byte{}
	binary.BigEndian.PutUint64(tmp[:8], x)
	return tmp[:]
}

func floatBytesStep(buf []byte, s int64) []byte {
	v := binary.BigEndian.Uint64(buf)
	binary.BigEndian.PutUint64(buf, uint64(int64(v)+s))
	return buf
}

func hashStr(s string) (h uint64) {
	h = 5381
	for i := 0; i < len(s); i++ {
		h = h*33 + uint64(s[i])
	}
	return h
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

func atof(a string) (float64, error) { return calc.Eval(a) }

func atof2(a []byte) (float64, error) { return atof(*(*string)(unsafe.Pointer(&a))) }

func ftoa(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

func ftob(f float64) []byte {
	if math.IsNaN(f) {
		return nil
	}
	return []byte(strconv.FormatFloat(f, 'f', -1, 64))
}

func atoi(a string) int {
	i, _ := strconv.Atoi(a)
	return i
}

func atoip(a string) int {
	i, err := strconv.Atoi(a)
	if err != nil {
		panic(err)
	}
	return i
}

func atoi64(a string) uint64 {
	i, _ := strconv.ParseUint(a, 10, 64)
	return i
}

func shardIndex(key string) int {
	return int(hashStr(key) % ShardNum)
}

func restCommandsToKeys(i int, command *redisproto.Command) []string {
	keys := []string{}
	for ; i < command.ArgCount(); i++ {
		keys = append(keys, string(command.Get(i)))
	}
	return keys
}

func reversePairs(in []Pair) []Pair {
	for i := 0; i < len(in)/2; i++ {
		j := len(in) - 1 - i
		in[i], in[j] = in[j], in[i]
	}
	return in
}

func writePairs(in []Pair, w *redisproto.Writer, command *redisproto.Command) error {
	withScores, withData := false, false
	for i := len(command.Argv) - 1; i >= len(command.Argv)-3 && i >= 0; i-- {
		if bytes.EqualFold(command.Get(i), []byte("WITHSCORES")) {
			withScores = true
		}
		if bytes.EqualFold(command.Get(i), []byte("WITHDATA")) {
			withData = true
		}
	}

	data := make([]string, 0, len(in))
	for _, p := range in {
		data = append(data, p.Key)
		if withScores || withData {
			data = append(data, strconv.FormatFloat(p.Score, 'f', -1, 64))
		}
		if withData {
			data = append(data, string(p.Data))
		}
	}
	return w.WriteBulkStrings(data)
}

func sizePairs(in []Pair) int {
	sz := 1
	for _, p := range in {
		sz += len(p.Key) + 8 + len(p.Data)
	}
	return sz
}

func (s *Server) fillPairsData(name string, in []Pair) error {
	if len(in) == 0 {
		return nil
	}
	keys := make([]string, len(in))
	for i, el := range in {
		keys[i] = el.Key
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
	enc := base64.NewEncoder(base64.URLEncoding, buf)
	gob.NewEncoder(enc).Encode(cmd)
	enc.Close()
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

type RangeLimit struct {
	Value     string
	Float     float64
	Inclusive bool
}

type RangeOptions struct {
	OffsetStart int
	OffsetEnd   int
	CountOnly   bool
	WithData    bool
	DeleteLog   []byte
	LexMatch    string
	Limit       int
}

func (r RangeLimit) fromString(v string) RangeLimit {
	r.Value = v
	r.Inclusive = true
	if strings.HasPrefix(v, "[") {
		r.Value = r.Value[1:]
	} else if strings.HasPrefix(v, "(") {
		r.Value = r.Value[1:]
		r.Inclusive = false
	} else if v == "+" {
		r.Value = "\xff" // TODO
	} else if v == "-" {
		r.Value = ""
	}
	return r
}

func (r RangeLimit) fromFloatString(v string) (RangeLimit, error) {
	var err error
	r.Inclusive = true
	if strings.HasPrefix(v, "[") {
		r.Float, err = atof(v[1:])
	} else if strings.HasPrefix(v, "(") {
		r.Float, err = atof(v[1:])
		r.Inclusive = false
	} else {
		r.Float, err = atof(v)
	}
	return r, err
}

type ServerConfig struct {
	ServerName         string
	HardLimit          int
	CacheSize          int
	WeakCacheSize      int
	WeakTTL            int // s
	SlowLimit          int // ms
	PurgeLogMaxRunTime int // s
	PurgeLogRun        int
	ResponseLogRun     int
	ResponseLogSize    int // kb
	ZAddDeferBatchRun  int
	ZAddDeferBlocking  int // bool
}

func (s *Server) validateConfig() {
	ifZero(&s.HardLimit, 10000)
	ifZero(&s.WeakTTL, 300)
	ifZero(&s.CacheSize, 1024)
	ifZero(&s.WeakCacheSize, 1024)
	ifZero(&s.SlowLimit, 500)
	ifZero(&s.PurgeLogMaxRunTime, 1)
	ifZero(&s.PurgeLogRun, 100)
	ifZero(&s.ResponseLogRun, 200)
	ifZero(&s.ResponseLogSize, 16)
	ifZero(&s.ZAddDeferBatchRun, 50)

	s.cache = NewCache(int64(s.CacheSize) * 1024 * 1024)
	s.weakCache = lru.NewCache(int64(s.WeakCacheSize) * 1024 * 1024)
}

func (s *Server) loadConfig() error {
	if err := s.db[0].Update(func(tx *bbolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte("_config"))
		if err != nil {
			return err
		}
		s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
			buf := bk.Get([]byte(strings.ToLower(f.Name)))
			switch f.Type {
			case reflect.TypeOf(0):
				fv.SetInt(int64(bytesToFloatZero(buf)))
			case reflect.TypeOf(""):
				fv.SetString(string(buf))
			}
			return nil
		})
		return nil
	}); err != nil {
		return err
	}
	s.validateConfig()
	return s.saveConfig()
}

func (s *Server) saveConfig() error {
	return s.db[0].Update(func(tx *bbolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte("_config"))
		if err != nil {
			return err
		}
		return s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
			var buf []byte
			switch f.Type {
			case reflect.TypeOf(0):
				buf = floatToBytes(float64(fv.Int()))
			case reflect.TypeOf(""):
				buf = []byte(fv.String())
			}
			return bk.Put([]byte(strings.ToLower(f.Name)), buf)
		})
	})
}

func (s *Server) updateConfig(key, value string) error {
	if strings.HasPrefix(key, "readonly") {
		s.db[atoip(key[8:])].readonly, _ = strconv.ParseBool(value)
		return nil
	}
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		if strings.ToLower(f.Name) != key {
			return nil
		}
		switch f.Type {
		case reflect.TypeOf(0):
			fv.SetInt(int64(atoi(value)))
		case reflect.TypeOf(""):
			fv.SetString(value)
		}
		return fmt.Errorf("exit")
	})
	s.validateConfig()
	return s.saveConfig()
}

func (s *Server) getConfig(key string) (v string, ok bool) {
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		if strings.ToLower(f.Name) == key {
			v, ok = fmt.Sprint(fv.Interface()), true
		}
		return nil
	})
	return
}

func (s *Server) listConfig() (list []string) {
	for _, b := range s.ReadOnly() {
		list = append(list, strconv.Itoa(boolToInt(b)))
	}
	list = []string{"readonly", strings.Join(list, ",")}
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		list = append(list, strings.ToLower(f.Name), fmt.Sprint(fv.Interface()))
		return nil
	})
	return
}

func (s *Server) configForEachField(cb func(reflect.StructField, reflect.Value) error) error {
	rv := reflect.ValueOf(&s.ServerConfig)
	rt := reflect.TypeOf(s.ServerConfig)
	for i := 0; i < rt.NumField(); i++ {
		if err := cb(rt.Field(i), rv.Elem().Field(i)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) info() string {
	p := s.slaves.Take(time.Minute)
	data := []string{
		"# server",
		fmt.Sprintf("version:%v", Version),
		fmt.Sprintf("servername:%v", s.ServerName),
		fmt.Sprintf("listen:%v", s.ln.Addr().String()),
		fmt.Sprintf("uptime:%v", time.Since(s.survey.startAt)),
		fmt.Sprintf("death_scheduler:%v", s.dieKey),
		fmt.Sprintf("master:%v", s.MasterAddr),
		fmt.Sprintf("master_name:%v", s.master.ServerName),
		fmt.Sprintf("master_version:%v", s.master.Version),
		fmt.Sprintf("slaves:%v", len(p)),
		fmt.Sprintf("connections:%v", s.survey.connections),
		"", "# read_write",
		fmt.Sprintf("sys_read_qps:%v", s.survey.sysRead),
		fmt.Sprintf("sys_read_avg_lat:%v", s.survey.sysReadLat.MeanString()),
		fmt.Sprintf("sys_write_qps:%v", s.survey.sysWrite),
		fmt.Sprintf("sys_write_avg_lat:%v", s.survey.sysWriteLat.MeanString()),
		"", "# zadd_batch",
		fmt.Sprintf("zadd_batch_avg_items:%v", s.survey.addBatchSize.MeanString()),
		fmt.Sprintf("zadd_batch_drop_qps:%v", s.survey.addBatchDrop),
		"", "# cache",
		fmt.Sprintf("cache_hit_qps:%v", s.survey.cache),
		fmt.Sprintf("cache_obj_count:%v", s.cache.Len()),
		fmt.Sprintf("cache_size:%v", s.cache.curWeight),
		"", "# weak_cache",
		fmt.Sprintf("weak_cache_hit_qps:%v", s.survey.weakCache),
		fmt.Sprintf("weak_cache_obj_count:%v", s.weakCache.Len()),
		fmt.Sprintf("weak_cache_size:%v", s.weakCache.Weight()),
	}
	return strings.Join(data, "\r\n") + "\r\n"
}

func (s *Server) shardInfo(shard int) string {
	x := &s.db[shard]
	fi, err := os.Stat(x.Path())
	if err != nil {
		panic(err)
	}
	tmp := []string{
		fmt.Sprintf("# shard%d", shard),
		fmt.Sprintf("path:%v", x.Path()),
		fmt.Sprintf("size:%v", fi.Size()),
		fmt.Sprintf("readonly:%v", x.readonly),
		fmt.Sprintf("zadd_defer_queue:%v", strconv.Itoa(len(x.deferAdd))),
	}
	var myTail uint64
	start := time.Now()
	x.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("wal"))
		if bk == nil {
			return nil
		}
		tmp = append(tmp, "", "# log")
		stat := bk.Stats()
		inuse := stat.LeafInuse + stat.BranchInuse
		alloc := stat.LeafAlloc + stat.BranchAlloc
		tmp = append(tmp, fmt.Sprintf("log_count:%d", stat.KeyN))
		tmp = append(tmp, fmt.Sprintf("log_tail:%d", bk.Sequence()))
		tmp = append(tmp, fmt.Sprintf("log_size:%d", inuse))
		tmp = append(tmp, fmt.Sprintf("log_alloc_size:%d", alloc))
		tmp = append(tmp, fmt.Sprintf("log_size_ratio:%.2f", float64(inuse)/float64(alloc)))
		myTail = bk.Sequence()
		return nil
	})
	log.Info("shard", shard, " info in ", time.Since(start))
	minTail := uint64(0)
	for i, sv := range s.slaves.Take(time.Minute) {
		si := &serverInfo{}
		json.Unmarshal(sv.Data, si)
		tail := si.KnownLogTails[shard]
		if i == 0 {
			tmp = append(tmp, "", "# slave_log")
			tmp = append(tmp, fmt.Sprintf("slave_queue:%d", len(s.slaves.Slaves)))
		}
		if i == 0 || tail < minTail {
			minTail = tail
		}
		tmp = append(tmp, fmt.Sprintf("slave_%v_log_tail:%d", sv.Key, tail))
	}
	if minTail > 0 {
		tmp = append(tmp, fmt.Sprintf("slave_min_log_tail:%d", minTail))
		tmp = append(tmp, fmt.Sprintf("slave_log_tail_diff:%d", int64(myTail)-int64(minTail)))
	}
	return strings.Join(tmp, "\r\n") + "\r\n"
}

func ifZero(v *int, v2 int) {
	if *v <= 0 {
		*v = v2
	}
}
