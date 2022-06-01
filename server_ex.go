package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"html/template"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/ranges"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
	log "github.com/sirupsen/logrus"
)

var (
	isReadCommand = map[string]bool{
		"ZCARD":  true,
		"ZSCORE": true, "ZMSCORE": true,
		"ZDATA": true, "ZMDATA": true,
		"ZCOUNT": true, "ZCOUNTBYLEX": true,
		"ZRANK": true, "ZREVRANK": true,
		"ZRANGE": true, "ZREVRANGE": true,
		"ZRANGEBYLEX": true, "ZREVRANGEBYLEX": true,
		"ZRANGEBYSCORE": true, "ZREVRANGEBYSCORE": true,
		"SCAN": true,
	}
	isWriteCommand = map[string]bool{
		"DEL":  true,
		"ZREM": true, "ZREMRANGEBYLEX": true, "ZREMRANGEBYSCORE": true, "ZREMRANGEBYRANK": true,
		"ZADD": true, "ZINCRBY": true,
	}
)

func init() {
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

func toStrings(b [][]byte) (keys []string) {
	for _, b := range b {
		keys = append(keys, string(b))
	}
	return keys
}

func redisPairs(in []s2pkg.Pair, flags wire.Flags) []string {
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

func dd(cmd *wire.Command) []byte {
	return joinMultiBytes(cmd.Argv)
}

func splitRawMultiBytesNoHeader(buf []byte) (*wire.Command, error) {
	tmp := &s2pkg.BytesArray{}
	if err := tmp.UnmarshalBytes(buf); err != nil {
		return nil, err
	}
	return &wire.Command{Argv: tmp.Data}, nil
}

func joinMultiBytesEmptyNoSig() []byte {
	return crc32.NewIEEE().Sum([]byte{0x95, 0, 0, 0, 0})
}

func joinMultiBytes(cmd [][]byte) []byte {
	buf := []byte{0x95, 0, 0, 0, 0} // version: 0x95 + signature: 4b
	rand.Reader.Read(buf[1:])
	buf = (&s2pkg.BytesArray{Data: cmd}).MarshalAppend(buf)
	h := crc32.NewIEEE()
	h.Write(buf[5:])
	return h.Sum(buf) // crc32: 4b
}

func (s *Server) InfoCommand(section string) (data []string) {
	if section == "" || section == "server" {
		data = append(data, "# server",
			fmt.Sprintf("version:%v", Version),
			fmt.Sprintf("servername:%v", s.ServerName),
			fmt.Sprintf("listen:%v", s.ln.Addr()),
			fmt.Sprintf("listen_unix:%v", s.lnLocal.Addr()),
			fmt.Sprintf("uptime:%v", time.Since(s.Survey.StartAt)),
			fmt.Sprintf("readonly:%v", s.ReadOnly),
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
			fmt.Sprintf("data_files:%d", len(dataFiles)),
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
	}
	if !compacted {
		tmp = append(tmp, fmt.Sprintf("compacted:%v", compacted))
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
		log.Info("wait: no acknowledged slave found")
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
	log.Errorf("wait %s: timeout", s.Slave.RemoteIP)
}

func ifZero(v *int, v2 int) {
	if *v <= 0 {
		*v = v2
	}
}

func parseWeakFlag(in *wire.Command) time.Duration {
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
		sz = s2pkg.SizeOfPairs(data)
	case [][]byte:
		sz = s2pkg.SizeOfBytes(data)
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
	return template.HTML(fmt.Sprintf("%s&nbsp;&nbsp;%s&nbsp;&nbsp;%s",
		s2pkg.FormatFloatShort(a), s2pkg.FormatFloatShort(b), s2pkg.FormatFloatShort(c)))
}

func appendUint(b []byte, v uint64) []byte {
	return append(s2pkg.Bytes(b), s2pkg.Uint64ToBytes(v)...)
}

func (s *Server) createDBListener() pebble.EventListener {
	L, R := dbLogger.Info, s.runScriptFunc
	return pebble.EventListener{
		BackgroundError:  func(a error) { dbLogger.Error(a); R("DBBackgroundError", a) },
		CompactionBegin:  func(a pebble.CompactionInfo) { L("[CompactionBegin] ", a); R("DBCompactionBegin", a) },
		CompactionEnd:    func(a pebble.CompactionInfo) { L("[CompactionEnd] ", a); R("DBCompactionEnd", a) },
		DiskSlow:         func(a pebble.DiskSlowInfo) { L("[DiskSlow] ", a); R("DBDiskSlow", a) },
		FlushBegin:       func(a pebble.FlushInfo) { L("[FlushBegin] ", a); R("DBFlushBegin", a) },
		FlushEnd:         func(a pebble.FlushInfo) { L("[FlushEnd] ", a); R("DBFlushEnd", a) },
		FormatUpgrade:    func(a pebble.FormatMajorVersion) { L("[FormatUpgrade] ", a); R("DBFormatUpgrade", a) },
		ManifestCreated:  func(a pebble.ManifestCreateInfo) { L("[ManifestCreated] ", a); R("DBManifestCreated", a) },
		ManifestDeleted:  func(a pebble.ManifestDeleteInfo) { L("[ManifestDeleted] ", a); R("DBManifestDeleted", a) },
		TableCreated:     func(a pebble.TableCreateInfo) { R("DBTableCreated", a) },
		TableDeleted:     func(a pebble.TableDeleteInfo) { R("DBTableDeleted", a) },
		TableIngested:    func(a pebble.TableIngestInfo) { L("[TableIngested] ", a); R("DBTableIngested", a) },
		TableStatsLoaded: func(a pebble.TableStatsInfo) { L("[TableStatsLoaded] ", a); R("DBTableStatsLoaded", a) },
		TableValidated:   func(a pebble.TableValidatedInfo) { L("[TableValidated] ", a); R("DBTableValidated", a) },
		WALCreated:       func(a pebble.WALCreateInfo) { L("[WALCreated] ", a); R("DBWALCreated", a) },
		WALDeleted:       func(a pebble.WALDeleteInfo) { L("[WALDeleted] ", a); R("DBWALDeleted", a) },
		WriteStallBegin:  func(a pebble.WriteStallBeginInfo) { L("[WriteStallBegin] ", a); R("DBWriteStallBegin", a) },
		WriteStallEnd:    func() { L("WriteStallEnd"); R("DBWriteStallEnd") },
	}
}

func (s *Server) ZCard(key string) (count int64) {
	_, i, _, err := extdb.GetKeyNumber(s.DB, ranges.GetZSetCounterKey(key))
	s2pkg.PanicErr(err)
	return int64(i)
}

func (s *Server) ZMScore(key string, memebrs []string) (scores []float64, err error) {
	if len(memebrs) == 0 {
		return nil, nil
	}
	for range memebrs {
		scores = append(scores, math.NaN())
	}
	bkName, _, _ := ranges.GetZSetRangeKey(key)
	for i, m := range memebrs {
		score, _, found, _ := extdb.GetKeyNumber(s.DB, append(bkName, m...))
		if found {
			scores[i] = score
		}
	}
	return
}

func (s *Server) ZMData(key string, members []string) (data [][]byte, err error) {
	if len(members) == 0 {
		return nil, nil
	}
	data = make([][]byte, len(members))
	bkName, bkScore, _ := ranges.GetZSetRangeKey(key)
	for i, m := range members {
		scoreBuf, _ := extdb.GetKey(s.DB, append(bkName, m...))
		if len(scoreBuf) != 0 {
			d, err := extdb.GetKey(s.DB, append(bkScore, append(scoreBuf, m...)...))
			if err != nil {
				return nil, err
			}
			data[i] = d
		}
	}
	return
}

func errorExit(msg string) {
	log.Error(msg)
	os.Exit(1)
}

func (s *Server) webConsoleServer() {
	if testFlag {
		return
	}
	uuid := s2pkg.UUID()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		start := time.Now()

		if chartSources := strings.Split(q.Get("chart"), ","); len(chartSources) > 0 && chartSources[0] != "" {
			startTs, endTs := s2pkg.MustParseInt64(q.Get("chart-start")), s2pkg.MustParseInt64(q.Get("chart-end"))
			w.Header().Add("Content-Type", "text/json")
			data, _ := s.GetMetricsPairs(int64(startTs)*1e6, int64(endTs)*1e6, chartSources...)
			if len(data) == 0 {
				w.Write([]byte("[]"))
				return
			}
			m := []interface{}{data[0].Timestamp}
			for _, d := range data {
				m = append(m, d.Value)
			}
			json.NewEncoder(w).Encode(m)
			return
		}

		if s.Password != "" && s.Password != q.Get("p") {
			w.WriteHeader(400)
			w.Write([]byte("s2db: password required"))
			return
		}

		shardInfos, wg := [ShardLogNum][]string{}, sync.WaitGroup{}
		if q.Get("noshard") != "1" {
			for i := 0; i < ShardLogNum; i++ {
				wg.Add(1)
				go func(i int) { shardInfos[i] = s.ShardLogInfoCommand(i); wg.Done() }(i)
			}
			wg.Wait()
		}

		sp := []string{s.DBPath}
		// sp = append(sp, filepath.Dir(s.db.
		cpu, iops, disk := s2pkg.GetOSUsage(sp[:])
		for ; len(cpu)%5 != 0; cpu = append(cpu, "") {
		}
		w.Header().Add("Content-Type", "text/html")
		template.Must(template.New("").Funcs(template.FuncMap{
			"kv": func(s string) template.HTML {
				var r struct{ Key, Value template.HTML }
				r.Key = template.HTML(s)
				if idx := strings.Index(s, ":"); idx > 0 {
					r.Key, r.Value = template.HTML(s[:idx]), template.HTML(s[idx+1:])
				}
				if strings.Count(string(r.Value), " ") == ShardLogNum-1 {
					parts := strings.Split(string(r.Value)+"   ", " ")
					for i, p := range parts {
						if p == "" {
							parts[i] = "<div class=box>" + p + "</div>"
						} else {
							parts[i] = "<div class=box><span class=mark>" + strconv.Itoa(i) + "</span>" + p + "</div>"
						}
					}
					r.Value = template.HTML("<div class=section-box>" + strings.Join(parts, "") + "</div>")
				} else {
					r.Value = makeHTMLStat(string(r.Value))
				}
				return template.HTML(fmt.Sprintf("<div class=s-key>%v</div><div class=s-value>%v</div>", r.Key, r.Value))
			},
			"stat":      makeHTMLStat,
			"timeSince": func(a time.Time) time.Duration { return time.Since(a) },
		}).Parse(webuiHTML)).Execute(w, map[string]interface{}{
			"s": s, "start": start,
			"CPU": cpu, "IOPS": iops, "Disk": disk, "REPLPath": uuid, "ShardInfo": shardInfos, "MetricsNames": s.ListMetricsNames(),
			"Sections": []string{"server", "server_misc", "replication", "sys_rw_stats", "batch", "command_qps", "command_avg_lat", "cache"},
		})
	})
	http.HandleFunc("/"+uuid, func(w http.ResponseWriter, r *http.Request) {
		nj.PlaygroundHandler(s.InspectorSource+"\n--BRK"+uuid+". DO NOT EDIT THIS LINE\n\n"+
			"local ok, err = server.UpdateConfig('InspectorSource', SOURCE_CODE.findsub('\\n--BRK"+uuid+"'), false)\n"+
			"println(ok, err)", s.getScriptEnviron())(w, r)
	})
	go http.Serve(s.lnWebConsole, nil)
}

func (s *Server) checkWritable() error {
	if s.ReadOnly {
		return wire.ErrServerReadonly
	}
	return nil
}
