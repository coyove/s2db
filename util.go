package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
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
	"github.com/coyove/s2db/calc"
	"github.com/coyove/s2db/redisproto"
	"go.etcd.io/bbolt"
)

var HardLimit = 100000

func init() {
	redisproto.MaxBulkSize = 1 << 20
	redisproto.MaxNumArg = 10000
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	sched.Verbose = false
	rand.Seed(time.Now().Unix())
}

type Pair struct {
	Key   string
	Score float64
	Data  []byte
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

func floatToInternalUint64(v float64) uint64 {
	x := math.Float64bits(v)
	if v >= 0 {
		x |= 1 << 63
	} else {
		x = ^x
	}
	return x
}

func floatToBytes(v float64) []byte {
	tmp := [8]byte{}
	binary.BigEndian.PutUint64(tmp[:8], floatToInternalUint64(v))
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
		if len(buf) > 0 && buf[0] == '=' { // argument may be a computable expression starting with '=', which should be calculated into numbers first
			v, err := atof2(buf)
			if err == nil {
				buf = []byte(ftoa(v))
			}
		}
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

func atof(a string) (float64, error) {
	if strings.HasPrefix(a, "=") {
		return calc.Eval(a[1:])
	}
	return strconv.ParseFloat(a, 64)
}

func atof2(a []byte) (float64, error) {
	return atof(*(*string)(unsafe.Pointer(&a)))
}

func atofp(a string) float64 {
	f, err := atof(a)
	if err != nil {
		panic(err)
	}
	return f
}

func atof2p(a []byte) float64 {
	f, err := atof2(a)
	if err != nil {
		panic(err)
	}
	return f
}

func ftoa(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

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
		panic("invalid integer: " + strconv.Quote(a))
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
		keys = append(keys, string(command.At(i)))
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
	var withScores, withData bool
	for i := len(command.Argv) - 1; i >= len(command.Argv)-3 && i >= 0; i-- {
		withScores = withScores || command.EqualFold(i, "WITHSCORES")
		withData = withData || command.EqualFold(i, "WITHDATA")
	}
	data := make([]string, 0, len(in))
	for _, p := range in {
		data = append(data, p.Key)
		if withScores || withData {
			data = append(data, ftoa(p.Score))
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

func (o *RangeOptions) translateOffset(keyName string, bk *bbolt.Bucket) {
	if o.OffsetStart < 0 || o.OffsetEnd < 0 {
		n := bk.KeyN()
		if o.OffsetStart < 0 {
			o.OffsetStart += n
		}
		if o.OffsetEnd < 0 {
			o.OffsetEnd += n
		}
	}
}

func (o *RangeOptions) getLimit() int {
	limit := HardLimit
	if o.Limit > 0 && o.Limit < HardLimit {
		limit = o.Limit
	}
	return limit
}

type ServerConfig struct {
	ServerName      string
	CacheSize       int
	CacheKeyMaxLen  int
	WeakCacheSize   int
	SlowLimit       int // ms
	ResponseLogRun  int
	ResponseLogSize int // kb
	BatchMaxRun     int
	SchedCompactJob string
	CompactLogHead  int
	CompactTxSize   int
	CompactTmpDir   string
	CompactNoBackup int // disable backup files when compacting, dangerous when you are master
	FillPercent     int // 1~10 will be translated to 0.1~1.0 and 0 means bbolt default (0.5)
	StopLogPull     int
}

func (s *Server) loadConfig() error {
	if err := s.configDB.Update(func(tx *bbolt.Tx) error {
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
	return s.saveConfig()
}

func (s *Server) saveConfig() error {
	ifZero(&s.CacheSize, 1024)
	ifZero(&s.CacheKeyMaxLen, 100)
	ifZero(&s.WeakCacheSize, 1024)
	ifZero(&s.SlowLimit, 500)
	ifZero(&s.ResponseLogRun, 200)
	ifZero(&s.ResponseLogSize, 16)
	ifZero(&s.BatchMaxRun, 50)
	ifZero(&s.CompactLogHead, 1500)
	ifZero(&s.CompactTxSize, 50000)

	s.cache = newKeyedCache(int64(s.CacheSize) * 1024 * 1024)
	s.weakCache = lru.NewCache(int64(s.WeakCacheSize) * 1024 * 1024)

	return s.configDB.Update(func(tx *bbolt.Tx) error {
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

func (s *Server) updateConfig(key, value string) (bool, error) {
	key = strings.ToLower(key)
	if strings.EqualFold(key, "readonly") {
		s.ReadOnly, _ = strconv.ParseBool(value)
		return true, nil
	}
	found := false
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
		found = true
		return fmt.Errorf("exit")
	})
	return found, s.saveConfig()
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

func (s *Server) listConfig() string {
	list := []string{"readonly:" + strconv.FormatBool(s.ReadOnly)}
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		list = append(list, strings.ToLower(f.Name)+":"+fmt.Sprint(fv.Interface()))
		return nil
	})
	return strings.Join(list, "\r\n") + "\r\n"
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

func (s *Server) info(section string) string {
	sz := 0
	for i := range s.db {
		fi, err := os.Stat(s.db[i].Path())
		if err != nil {
			panic(err)
		}
		sz += int(fi.Size())
	}
	cwd, _ := os.Getwd()
	data := []string{
		"# server",
		fmt.Sprintf("version:%v", Version),
		fmt.Sprintf("servername:%v", s.ServerName),
		fmt.Sprintf("listen:%v", s.ln.Addr().String()),
		fmt.Sprintf("uptime:%v", time.Since(s.survey.startAt)),
		fmt.Sprintf("readonly:%v", s.ReadOnly),
		fmt.Sprintf("connections:%v", s.survey.connections),
		"",
		"# server_misc",
		fmt.Sprintf("cwd:%v", cwd),
		fmt.Sprintf("args:%v", strings.Join(os.Args, " ")),
		fmt.Sprintf("death_scheduler:%v", s.dieKey),
		fmt.Sprintf("db_size:%v", sz),
		fmt.Sprintf("db_size_mb:%.2f", float64(sz)/1024/1024),
		"",
		"# replication",
		fmt.Sprintf("master_mode:%v", s.MasterMode),
		fmt.Sprintf("master:%v", s.MasterAddr),
		fmt.Sprintf("master_name:%v", s.master.ServerName),
		fmt.Sprintf("master_version:%v", s.master.Version),
		fmt.Sprintf("slaves:%v", s.slaves.Len()),
		"",
		"# sys_rw_stats",
		fmt.Sprintf("sys_read_qps:%v", s.survey.sysRead),
		fmt.Sprintf("sys_read_avg_lat:%v", s.survey.sysReadLat.MeanString()),
		fmt.Sprintf("sys_write_qps:%v", s.survey.sysWrite),
		fmt.Sprintf("sys_write_avg_lat:%v", s.survey.sysWriteLat.MeanString()),
		"",
		"# batch",
		fmt.Sprintf("batch_size:%v", s.survey.batchSize.MeanString()),
		fmt.Sprintf("batch_lat:%v", s.survey.batchLat.MeanString()),
		fmt.Sprintf("batch_size_slave:%v", s.survey.batchSizeSv.MeanString()),
		fmt.Sprintf("batch_lat_slave:%v", s.survey.batchLatSv.MeanString()),
		"",
		"# cache",
		fmt.Sprintf("cache_hit_qps:%v", s.survey.cache),
		fmt.Sprintf("cache_obj_count:%v", s.cache.Len()),
		fmt.Sprintf("cache_size:%v", s.cache.curWeight),
		fmt.Sprintf("weak_cache_hit_qps:%v", s.survey.weakCache),
		fmt.Sprintf("weak_cache_obj_count:%v", s.weakCache.Len()),
		fmt.Sprintf("weak_cache_size:%v", s.weakCache.Weight()),
		"",
	}
	data = append(data, s.slavesSection()...)
	if section != "" {
		for i, r := range data {
			if strings.HasPrefix(r, "# ") && strings.HasSuffix(r, section) {
				for j := i + 1; j < len(data); j++ {
					if data[j] == "" {
						return strings.Join(data[i:j+1], "\r\n")
					}
				}
			}
		}
	}
	return strings.Join(data, "\r\n")
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
	tmp = append(tmp, "", "# slave_log", fmt.Sprintf("slave_queue:%d", len(s.slaves.q)))
	s.slaves.Foreach(func(si *serverInfo) {
		tail := si.LogTails[shard]
		if tail < minTail {
			minTail = tail
		}
		tmp = append(tmp, fmt.Sprintf("slave_%v_logtail:%d", si.RemoteAddr, tail))
	})
	tmp = append(tmp, fmt.Sprintf("slave_logtail_min:%d", minTail))
	tmp = append(tmp, fmt.Sprintf("slave_logtail_diff:%d", int64(myTail)-int64(minTail)))
	return strings.Join(tmp, "\r\n") + "\r\n"
}

func ifZero(v *int, v2 int) {
	if *v <= 0 {
		*v = v2
	}
}

func parseDeferFlag(in *redisproto.Command) bool {
	if bytes.EqualFold(in.Argv[2], []byte("--defer--")) {
		in.Argv = append(in.Argv[:2], in.Argv[3:]...)
		return true
	}
	return false
}

func parseWeakFlag(in *redisproto.Command) time.Duration {
	i := in.ArgCount() - 2
	if i >= 2 && in.EqualFold(i, "WEAK") {
		x := atof2p(in.Argv[i+1])
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
