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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/s2db/bitmap"
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
		"ZDATA": true, "ZMDATA": true, "ZDATABM16": true,
		"ZCOUNT": true, "ZCOUNTBYLEX": true,
		"ZRANK": true, "ZREVRANK": true,
		"ZRANGE": true, "ZREVRANGE": true,
		"ZRANGEBYLEX": true, "ZREVRANGEBYLEX": true,
		"ZRANGEBYSCORE": true, "ZREVRANGEBYSCORE": true,
		"ZRANGERANGEBYSCORE": true, "ZREVRANGERANGEBYSCORE": true,
		"ZRI":   true,
		"SCAN":  true,
		"SSCAN": true, "SCARD": true, "SMEMBERS": true, "SISMEMBER": true, "SMISMEMBER": true,
		"GET": true, "MGET": true,
	}
	isWriteCommand = map[string]bool{
		"DEL":  true,
		"ZREM": true, // "ZREMRANGEBYLEX": true, "ZREMRANGEBYSCORE": true, "ZREMRANGEBYRANK": true,
		"ZADD": true, "ZINCRBY": true,
		"SADD": true, "SREM": true,
		"SET": true, "SETNX": true,
	}
)

func shardIndex(key string) int {
	return int(s2pkg.HashStr(key) % ShardLogNum)
}

func toStrings(b [][]byte) (keys []string) {
	for _, b := range b {
		keys = append(keys, string(b))
	}
	return keys
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
			fmt.Sprintf("reverseproxy:%v", s.ReverseProxy),
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
			diffs, diffSum := diffLogtails(tails, s.Slave.Logtails)
			data = append(data,
				fmt.Sprintf("slave_conn:%v", s.Slave.Config().Raw),
				fmt.Sprintf("slave_ack:%v", s.Slave.IsAcked(s)),
				fmt.Sprintf("slave_ack_before:%v", s.Slave.AckBefore()),
				fmt.Sprintf("slave_logtail:%v", joinArray(s.Slave.Logtails)),
				fmt.Sprintf("slave_logtail_diff_sum:%v", diffSum),
				fmt.Sprintf("slave_logtail_diff:%v", joinArray(diffs)),
			)
		}
		if s.PullMaster.Redis() != nil {
			lags := [ShardLogNum]int64{}
			now := clock.Unix()
			for i := range s.shards {
				lags[i] = now - clock.IdNano(s.PullMaster.Logtails[i])
			}
			data = append(data,
				fmt.Sprintf("pullmaster_conn:%v", s.PullMaster.Config().Raw),
				fmt.Sprintf("pullmaster_ack:%v", s.PullMaster.IsAcked(s)),
				fmt.Sprintf("pullmaster_ack_before:%v", s.PullMaster.AckBefore()),
				fmt.Sprintf("pullmaster_lag:%v", joinArray(lags)),
			)
		}
		if s.Master.RemoteIP != "" {
			data = append(data,
				fmt.Sprintf("master_ip:%v", s.Master.RemoteIP),
				fmt.Sprintf("master_ack_before:%v", s.Master.AckBefore()),
			)
		}
		s.Pullers.Range(func(k, v interface{}) bool {
			diffs, diffSum := diffLogtails(tails, v.(*endpoint).Logtails)
			data = append(data,
				fmt.Sprintf("puller_ip:%v", k),
				fmt.Sprintf("puller_ack_before:%v", v.(*endpoint).AckBefore()),
				fmt.Sprintf("puller_logtail:%v", joinArray(v.(*endpoint).Logtails)),
				fmt.Sprintf("puller_logtail_diff_sum:%v", diffSum),
				fmt.Sprintf("puller_logtail_diff:%v", joinArray(diffs)),
			)
			return true
		})
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
			fmt.Sprintf("cache_obj_count:%v/%v", s.Cache.Len(), s.Cache.Cap()),
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

func parseWeakFlag(in *wire.Command) time.Duration {
	i := in.ArgCount() - 2
	if i >= 2 && in.StrEqFold(i, "WEAK") {
		x := s2pkg.MustParseFloatBytes(in.Argv[i+1])
		in.Argv = in.Argv[:i]
		return time.Duration(int64(x*1e6) * 1e3)
	}
	return 0
}

func defaultNorm(in []s2pkg.Pair) []s2pkg.Pair {
	return in
}

func parseNormFlag(rev bool, in *wire.Command) (func([]s2pkg.Pair) []s2pkg.Pair, wire.Flags) {
	if !in.StrEqFold(2, "--NORM--") {
		return defaultNorm, in.Flags(4)
	}

	normValue := in.Int64(3)
	if normValue <= 0 {
		panic("invalid normalization value")
	}

	in.Argv = append(in.Argv[:2], in.Argv[4:]...)
	flags := in.Flags(4)
	start, end := ranges.Score(in.Str(2)), ranges.Score(in.Str(3))

	normStart := start
	if !math.IsInf(start.Float, 0) {
		x := int64(start.Float) / normValue
		if rev {
			x++
		}
		normStart.Float = float64(x * normValue)
		in.Argv[2] = normStart.ToScore()

		flags.ILimit = new(float64)
		*flags.ILimit = start.Float
		if !start.Inclusive {
			x := s2pkg.FloatToOrderedUint64(start.Float) + uint64(ifInt(rev, -1, 1))
			*flags.ILimit = s2pkg.OrderedUint64ToFloat(x)
		}
	}

	normEnd := end
	if !math.IsInf(end.Float, 0) {
		x := int64(end.Float) / normValue
		if !rev {
			x++
		}
		normEnd.Float = float64(x * normValue)
		in.Argv[3] = normEnd.ToScore()
	}

	if testFlag {
		fmt.Println(toStrings(in.Argv), string(start.ToScore()), string(end.ToScore()))
	}

	return func(data []s2pkg.Pair) []s2pkg.Pair {
		if rev {
			for i, d := range data {
				if (d.Score <= start.Float && start.Inclusive) || (d.Score < start.Float && !start.Inclusive) {
					data = data[i:]
					for i := len(data) - 1; i >= 0; i-- {
						if d := data[i]; (d.Score >= end.Float && end.Inclusive) || (d.Score > end.Float && !end.Inclusive) {
							data = data[:i+1]
							return data
						}
					}
					break
				}
			}
		} else {
			for i, d := range data {
				if (d.Score >= start.Float && start.Inclusive) || (d.Score > start.Float && !start.Inclusive) {
					data = data[i:]
					for i := len(data) - 1; i >= 0; i-- {
						if d := data[i]; (d.Score <= end.Float && end.Inclusive) || (d.Score < end.Float && !end.Inclusive) {
							data = data[:i+1]
							return data
						}
					}
					break
				}
			}
		}
		return data[:0]
	}, flags
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

func (s *Server) getCache(key string, h uint64) interface{} {
	s.Survey.CacheReq.Incr(1)
	v, ok := s.Cache.Get(key, h)
	if ok {
		s.Survey.CacheHit.Incr(1)
		return v
	}
	return nil
}

func (s *Server) addCache(key string, h uint64, data interface{}, wm int64) {
	sz := 0
	switch data := data.(type) {
	case []s2pkg.Pair:
		sz = s2pkg.SizeOfPairs(data)
	case [][]byte:
		sz = s2pkg.SizeOfBytes(data)
	}
	if sz > 0 {
		if sz > s.CacheObjMaxSize*1024 {
			return
		}
		s.Survey.CacheSize.Incr(int64(sz))
	}
	if !s.Cache.Add(key, h, data, wm) {
		s.Survey.CacheAddConflict.Incr(1)
	}
}

func (s *Server) readCache(K *wire.Command, f func() (interface{}, error)) (interface{}, error) {
	key := K.StrRef(1)
	cmdHash := s2pkg.HashMultiBytes(K.Argv)
	cachewm := s.Cache.GetWatermark(key)
	var err error
	x := s.getCache(key, cmdHash)
	if x == nil {
		x, err = f()
		if err != nil {
			return nil, err
		}
		s.addCache(key, cmdHash, x, cachewm)
	}
	return x, nil
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
	bkName := ranges.GetZSetNameKey(key)
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

func (s *Server) ZDataBM16(key string, member string, start, end uint16) (bits [][]byte, err error) {
	bkName, bkScore, _ := ranges.GetZSetRangeKey(key)
	bkName = append(bkName, member...)
	err = extdb.GetKeyFunc(s.DB, bkName, func(scoreBuf []byte) error {
		bkScore = append(bkScore, append(scoreBuf, member...)...)
		return extdb.GetKeyFunc(s.DB, bkScore, func(d []byte) error {
			bitmap.Iterate(d, func(v uint16) bool {
				if start != 0 && v < start {
					return true
				}
				if end != 0 && v > end {
					return false
				}
				bits = append(bits, s2pkg.FormatFloatBulk(float64(v)))
				return true
			})
			return nil
		})
	})
	return
}

func (s *Server) webConsoleHandler() {
	if testFlag {
		return
	}
	uuid := s2pkg.UUID()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		defer s2pkg.HTTPRecover(w, r)

		q := r.URL.Query()
		start := time.Now()

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
	http.HandleFunc("/chart/", func(w http.ResponseWriter, r *http.Request) {
		defer s2pkg.HTTPRecover(w, r)
		chartSources := strings.Split(r.URL.Path[7:], ",")
		if len(chartSources) == 0 || chartSources[0] == "" {
			w.Write([]byte("[]"))
			return
		}
		startTs, endTs := s2pkg.MustParseInt64(r.URL.Query().Get("start")), s2pkg.MustParseInt64(r.URL.Query().Get("end"))
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
	})
	http.HandleFunc("/ssd", func(w http.ResponseWriter, r *http.Request) {
		defer s2pkg.HTTPRecover(w, r)

		q := r.URL.Query()

		if s.Password != "" && s.Password != q.Get("p") {
			w.WriteHeader(400)
			w.Write([]byte("s2db: password required"))
			return
		}

		sk, ek := q.Get("from"), q.Get("to")
		if sk == "" {
			w.WriteHeader(400)
			w.Write([]byte("s2db: ssd start key required"))
			return
		}

		if ek == "" {
			ek = sk
		}

		start, end := math.Inf(-1), math.Inf(1)
		if sv := q.Get("start"); sv != "" {
			start = s2pkg.MustParseFloat(sv)
		}
		if sv := q.Get("end"); sv != "" {
			end = s2pkg.MustParseFloat(sv)
		}

		w.Header().Add("Content-Type", "application/octet-stream")
		w.Header().Add("Content-Encoding", "gzip")
		w.Header().Add("Content-Disposition", fmt.Sprintf("attachment; filename=\"ssd_%d.csv\"", clock.UnixNano()))
		fmt.Println(q.Get("match"))
		s.ScanScoreDump(w, sk, ek, start, end, wire.Flags{
			WithData: q.Get("data") == "1",
			Match:    q.Get("match"),
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

func diffLogtails(a, b [ShardLogNum]uint64) (diffs [ShardLogNum]float64, diffSum float64) {
	for i := range a {
		diffs[i] = clock.IdDiff(a[i], b[i])
		diffSum += diffs[i]
	}
	return
}

func (s *Server) Scan(cursor string, flags wire.Flags) (pairs []s2pkg.Pair, nextCursor string) {
	count := flags.Count + 1
	startCursor := cursor
	timedout, start := "", clock.Now()
	if len(cursor) > 0 {
		switch cursor[0] {
		case 'Z':
			cursor = cursor[1:]
		case 'S':
			cursor = cursor[1:]
			goto SCAN_SET
		case 'K':
			cursor = cursor[1:]
			goto SCAN_KV
		}
	}

	s.ForeachZSet(cursor, func(k string) bool {
		if time.Since(start) > flags.Timeout {
			timedout = "Z" + k
			return false
		}
		if flags.Match != "" && !s2pkg.Match(flags.Match, k) {
			return true
		}
		pairs = append(pairs, s2pkg.Pair{Member: k, Score: float64(s.ZCard(k)), Data: []byte("zset")})
		return len(pairs) < count
	})
	if len(pairs) >= count {
		pairs, nextCursor = pairs[:count-1], "Z"+pairs[count-1].Member
		return
	}

	cursor = startCursor
SCAN_SET:
	if timedout == "" {
		s.ForeachSet(cursor, func(k string) bool {
			if time.Since(start) > flags.Timeout {
				timedout = "S" + k
				return false
			}
			if flags.Match != "" && !s2pkg.Match(flags.Match, k) {
				return true
			}
			pairs = append(pairs, s2pkg.Pair{Member: k, Score: float64(s.SCard(k)), Data: []byte("set")})
			return len(pairs) < count
		})
	}
	if len(pairs) >= count {
		pairs, nextCursor = pairs[:count-1], "S"+pairs[count-1].Member
		return
	}

	cursor = startCursor
SCAN_KV:
	if timedout == "" {
		s.ForeachKV(cursor, func(k string, v []byte) bool {
			if time.Since(start) > flags.Timeout {
				timedout = "K" + k
				return false
			}
			if flags.Match != "" && !s2pkg.Match(flags.Match, k) {
				return true
			}
			pairs = append(pairs, s2pkg.Pair{Member: k, Score: float64(len(v)), Data: []byte("string")})
			return len(pairs) < count
		})
	}
	if len(pairs) >= count {
		pairs, nextCursor = pairs[:count-1], "K"+pairs[count-1].Member
	}

	if timedout != "" {
		return pairs, timedout
	}
	return
}

func (s *Server) getUpstreamShard(key string) int {
	return int(s2pkg.HashStr32(key) % 1024)
}

func (s *Server) getUpstreamList() []s2pkg.Pair {
	key := "_upstreams_"
	mwm := s.Cache.GetWatermark(key)
	if v := s.getCache(key, 0); v != nil {
		return v.([]s2pkg.Pair)
	}
	p, err := s.ZRange(false, key, 0, -1, wire.Flags{Limit: math.MaxInt64, WithData: true})
	if err != nil {
		panic(err)
	}
	s.addCache(key, 0, p, mwm)
	return p
}
