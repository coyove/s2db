package main

import (
	"context"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/wire"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
)

var (
	isReadCommand = map[string]bool{
		"PSELECT":     true,
		"SELECT":      true,
		"SELECTCOUNT": true,
		"LOOKUP":      true,
		"SCAN":        true,
	}
	isWriteCommand = map[string]bool{
		"APPEND": true,
		"RAWSET": true,
	}

	//go:embed scripts/index.html
	webuiHTML string
)

func (s *Server) InfoCommand(section string) (data []string) {
	if section == "" || strings.EqualFold(section, "server") {
		data = append(data, "# server",
			fmt.Sprintf("version:%v", Version),
			fmt.Sprintf("servername:%v", s.Config.ServerName),
			fmt.Sprintf("listen:%v", s.lnRESP.Addr()),
			fmt.Sprintf("uptime:%v", time.Since(s.Survey.StartAt)),
			fmt.Sprintf("wall_clock:%v", time.Now().Format("15:04:05.000000000")),
			fmt.Sprintf("future_clock:%v", time.Unix(0, future.UnixNano()).Format("15:04:05.000000000")),
			fmt.Sprintf("readonly:%v", s.ReadOnly),
			fmt.Sprintf("connections:%v", s.Survey.Connections),
		)
		if ntp := future.Chrony.Load(); ntp != nil {
			data = append(data,
				fmt.Sprintf("ntp_stats:%v/%v",
					time.Duration(ntp.EstimatedOffset*1e9),
					time.Duration(ntp.EstimatedOffsetErr*1e9)))
		}
		data = append(data, "")
	}
	if section == "" || strings.EqualFold(section, "server_misc") {
		dataSize := 0
		dataFiles, _ := ioutil.ReadDir(s.DBPath)
		for _, fi := range dataFiles {
			dataSize += int(fi.Size())
		}
		iDisk, _ := s.DB.EstimateDiskUsage([]byte{'z'}, []byte{'z' + 1})
		HDisk, _ := s.DB.EstimateDiskUsage([]byte("H"), []byte("I"))
		cwd, _ := os.Getwd()
		data = append(data, "# server_misc",
			fmt.Sprintf("cwd:%v", cwd),
			fmt.Sprintf("args:%v", strings.Join(os.Args, " ")),
			fmt.Sprintf("data_files:%d", len(dataFiles)),
			fmt.Sprintf("data_size:%d", dataSize),
			fmt.Sprintf("data_size_mb:%.2f", float64(dataSize)/1024/1024),
			fmt.Sprintf("index_size:%d", iDisk),
			fmt.Sprintf("index_size_mb:%.2f", float64(iDisk)/1024/1024),
			fmt.Sprintf("hll_size:%d", HDisk),
			fmt.Sprintf("hll_size_mb:%.2f", float64(HDisk)/1024/1024),
			fmt.Sprintf("fill_cache:%v", s.fillCache.Len()),
			fmt.Sprintf("wm_cache:%v", s.wmCache.Len()),
			"")
	}
	if section == "" || strings.EqualFold(section, "peers") {
		data = append(data, "# peers")
		s.ForeachPeer(func(i int, p *endpoint, cli *redis.Client) {
			u, _ := url.Parse(p.Config().URI)
			addr, _ := net.ResolveTCPAddr("tcp", u.Host)
			addr.Port++
			data = append(data, fmt.Sprintf("peer%d:%s", i, p.Config().URI))
			data = append(data, fmt.Sprintf("peer%d_console:http://%s", i, addr.String()))
		})
		data = append(data, "")
	}
	if section == "" || strings.EqualFold(section, "sys_rw_stats") {
		data = append(data, "# sys_rw_stats",
			fmt.Sprintf("sys_read_qps:%v", s.Survey.SysRead.String()),
			fmt.Sprintf("sys_read_avg_lat:%v", s.Survey.SysRead.MeanString()),
			fmt.Sprintf("sys_write_qps:%v", s.Survey.SysWrite.String()),
			fmt.Sprintf("sys_write_avg_lat:%v", s.Survey.SysWrite.MeanString()),
			fmt.Sprintf("slow_logs_qps:%v", s.Survey.SlowLogs.QPSString()),
			fmt.Sprintf("slow_logs_avg_lat:%v", s.Survey.SlowLogs.MeanString()),
			"")
	}
	if section == "" || strings.EqualFold(section, "command_qps") || strings.EqualFold(section, "command_avg_lat") {
		var keys []string
		s.Survey.Command.Range(func(k, v interface{}) bool { keys = append(keys, k.(string)); return true })
		sort.Strings(keys)
		add := func(f func(*s2.Survey) string) (res []string) {
			for _, k := range keys {
				v, _ := s.Survey.Command.Load(k)
				res = append(res, fmt.Sprintf("%v:%v", k, f(v.(*s2.Survey))))
			}
			return append(res, "")
		}
		if section == "" || section == "command_avg_lat" {
			data = append(data, "# command_avg_lat")
			data = append(data, add(func(s *s2.Survey) string { return s.MeanString() })...)
		}
		if section == "" || section == "command_qps" {
			data = append(data, "# command_qps")
			data = append(data, add(func(s *s2.Survey) string { return s.QPSString() })...)
		}
	}
	if strings.HasPrefix(section, ":") {
		key := section[1:]
		add, del, _ := s.getHLL(key)
		startKey := kkp(key)
		disk, _ := s.DB.EstimateDiskUsage(startKey, s2.IncBytes(startKey))
		data = append(data, "# key "+key)
		data = append(data, fmt.Sprintf("size:%d", disk))
		if wm, wmok := s.wmCache.Get16(s2.HashStr128(key)); wmok {
			data = append(data, fmt.Sprintf("watermark:%s", hexEncode(wm[:])))
		} else {
			data = append(data, "watermark:miss")
		}
		data = append(data, fmt.Sprintf("hll_add:%d", add.Count()))
		data = append(data, fmt.Sprintf("hll_del:%d", del.Count()))
		data = append(data, "")
	}
	if strings.HasPrefix(section, "*") {
		data = append(data, "# id "+section[1:])
		id := s.translateCursor([]byte(section[1:]), false)
		v, key, _ := s.LookupID(id)
		data = append(data, fmt.Sprintf("hash:%08x", id[8:12]))
		data = append(data, fmt.Sprintf("key:%s", key))
		data = append(data, fmt.Sprintf("data_size:%d", len(v)))
		data = append(data, "")
	}
	if strings.HasPrefix(section, "=") {
		data = append(data, "# metrics "+section[1:])
		switch v := s.getMetrics(section[1:]).(type) {
		case *s2.Survey:
			m := v.Metrics()
			data = append(data, fmt.Sprintf("qps1:%f", m.QPS[0]), fmt.Sprintf("qps5:%f", m.QPS[1]), fmt.Sprintf("qps:%f", m.QPS[2]))
			data = append(data, fmt.Sprintf("mean1:%f", m.Mean[0]), fmt.Sprintf("mean5:%f", m.Mean[1]), fmt.Sprintf("mean:%f", m.Mean[2]))
			data = append(data, fmt.Sprintf("max1:%d", m.Max[0]), fmt.Sprintf("max5:%d", m.Max[1]), fmt.Sprintf("max:%d", m.Max[2]))
		case *s2.P99SurveyMinute:
			data = append(data, fmt.Sprintf("p99:%f", v.P99()))
		}
		data = append(data, "")
	}
	return
}

func makeHTMLStat(s string) template.HTML {
	var a, b, c float64
	if n, _ := fmt.Sscanf(s, "%f %f %f", &a, &b, &c); n != 3 {
		return template.HTML(s)
	}
	return template.HTML(fmt.Sprintf("%s&nbsp;&nbsp;%s&nbsp;&nbsp;%s",
		s2.FormatFloatShort(a), s2.FormatFloatShort(b), s2.FormatFloatShort(c)))
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

func (s *Server) httpServer() {
	uuid := s2.UUID()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		defer s2.HTTPRecover(w, r)

		q := r.URL.Query()
		start := time.Now()

		if s.Config.Password != "" && s.Config.Password != q.Get("p") {
			w.WriteHeader(400)
			w.Write([]byte("s2db: password required"))
			return
		}

		sp := []string{s.DBPath}
		// sp = append(sp, filepath.Dir(s.db.
		cpu, iops, disk := s2.GetOSUsage(sp[:])
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
				r.Value = makeHTMLStat(string(r.Value))
				return template.HTML(fmt.Sprintf("<div class=s-key>%v</div><div class=s-value>%v</div>", r.Key, r.Value))
			},
			"stat":      makeHTMLStat,
			"timeSince": func(a time.Time) time.Duration { return time.Since(a) },
		}).Parse(webuiHTML)).Execute(w, map[string]interface{}{
			"s": s, "start": start,
			"CPU": cpu, "IOPS": iops, "Disk": disk, "REPLPath": uuid, "MetricsNames": s.ListMetricsNames(),
			"Sections": []string{"server", "server_misc", "sys_rw_stats", "peers", "command_qps", "command_avg_lat"},
		})
	})
	mux.HandleFunc("/chart/", func(w http.ResponseWriter, r *http.Request) {
		defer s2.HTTPRecover(w, r)
		chartSources := strings.Split(r.URL.Path[7:], ",")
		if len(chartSources) == 0 || chartSources[0] == "" {
			w.Write([]byte("[]"))
			return
		}
		startTs, endTs := s2.MustParseInt64(r.URL.Query().Get("start")), s2.MustParseInt64(r.URL.Query().Get("end"))
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
	mux.HandleFunc("/ssd", func(w http.ResponseWriter, r *http.Request) {
		defer s2.HTTPRecover(w, r)

		// q := r.URL.Query()

		// if s.Password != "" && s.Password != q.Get("p") {
		// 	w.WriteHeader(400)
		// 	w.Write([]byte("s2db: password required"))
		// 	return
		// }

		// sk, ek := q.Get("from"), q.Get("to")
		// if sk == "" {
		// 	w.WriteHeader(400)
		// 	w.Write([]byte("s2db: ssd start key required"))
		// 	return
		// }

		// if ek == "" {
		// 	ek = sk
		// }

		// start, end := math.Inf(-1), math.Inf(1)
		// if sv := q.Get("start"); sv != "" {
		// 	start = s2.MustParseFloat(sv)
		// }
		// if sv := q.Get("end"); sv != "" {
		// 	end = s2.MustParseFloat(sv)
		// }

		// w.Header().Add("Content-Type", "application/octet-stream")
		// w.Header().Add("Content-Encoding", "gzip")
		// w.Header().Add("Content-Disposition", fmt.Sprintf("attachment; filename=\"ssd_%d.csv\"", clock.UnixNano()))
		// fmt.Println(q.Get("match"))
	})
	mux.HandleFunc("/"+uuid, func(w http.ResponseWriter, r *http.Request) {
		nj.PlaygroundHandler(s.Config.InspectorSource+
			"\n--BRK"+uuid+". DO NOT EDIT THIS LINE\n\n"+
			"local ok, err = server.UpdateConfig('InspectorSource', SOURCE_CODE.findsub('\\n--BRK"+uuid+"'), false)\n"+
			"println(ok, err)", s.getScriptEnviron())(w, r)
	})
	http.Serve(s.lnHTTP, mux)
}

func (s *Server) checkWritable() error {
	if s.ReadOnly {
		return wire.ErrServerReadonly
	}
	return nil
}

func (s *Server) wrapLookup(id []byte) (data []byte, err error) {
	data, _, err = s.LookupID(id)
	if err != nil {
		return nil, err
	}
	if len(data) > 0 || !s.HasOtherPeers() {
		return data, nil
	}

	recv, out := s.ForeachPeerSendCmd(func() redis.Cmder {
		return redis.NewStringCmd(context.TODO(), "LOOKUP", id, "LOCAL")
	})
	if recv == 0 {
		return data, nil
	}
	var m string
	s.ProcessPeerResponse(false, recv, out, func(cmd redis.Cmder) bool {
		m0 := cmd.(*redis.StringCmd).Val()
		if m0 != "" {
			m = m0
		}
		return true
	})
	return []byte(m), nil
}

func (s *Server) convertPairs(w *wire.Writer, p []s2.Pair, max int) (err error) {
	if len(p) > max {
		p = p[:max]
	}
	a := make([][]byte, 0, len(p)*3)
	for _, p := range p {
		d := p.Data
		if v, ok := p.Future().Cookie(); ok {
			d = append(strconv.AppendInt(append(d, "[[mark="...), int64(v), 10), "]]"...)
		}
		a = append(a, p.IDHex(), p.UnixMilliBytes(), d)
	}
	return w.WriteBulks(a)
}

func (s *Server) translateCursor(buf []byte, desc bool) (start []byte) {
	switch x := *(*string)(unsafe.Pointer(&buf)); x {
	case "+", "+inf", "+INF", "+Inf":
		start = []byte(maxCursor)
	case "recent", "RECENT":
		tmp := s2.ConvertFutureTo16B(future.Get(s.Channel))
		start = tmp[:]
	case "0", "":
		start = make([]byte, 16)
	default:
		if len(x) == 32 || len(x) == 33 {
			start = hexDecode(buf)
		} else if len(x) == 16 {
			start = buf
		} else if desc {
			start = make([]byte, 16)
			binary.BigEndian.PutUint64(start, uint64(s2.MustParseFloat(x)*1e9+1e9-1))
		} else {
			start = make([]byte, 16)
			binary.BigEndian.PutUint64(start, uint64(s2.MustParseFloat(x)*1e9))
		}
	}
	if *(*string)(unsafe.Pointer(&start)) > maxCursor {
		start = []byte(maxCursor)
	}
	return
}

func (s *Server) Get(key []byte) ([]byte, error) {
	buf, rd, err := s.DB.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer rd.Close()
	return s2.Bytes(buf), nil
}

func (s *Server) GetInt64(key []byte) (int64, error) {
	buf, rd, err := s.DB.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	defer rd.Close()
	if len(buf) != 8 {
		return 0, fmt.Errorf("invalid number bytes (8)")
	}
	return int64(s2.BytesToUint64(buf)), nil
}

func (s *Server) SetInt64(key []byte, vi int64) error {
	return s.DB.Set(key, s2.Uint64ToBytes(uint64(vi)), pebble.Sync)
}
