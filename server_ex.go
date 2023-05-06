package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

var (
	isReadCommand = map[string]bool{
		"ISELECT": true,
		"SELECT":  true,
		"GET":     true,
		"MGET":    true,
		"SCAN":    true,
		"HCOUNT":  true,
	}
	isWriteCommand = map[string]bool{
		"APPEND": true,
	}
)

func ssRef(b [][]byte) (keys []string) {
	for i, b := range b {
		keys = append(keys, "")
		*(*[2]uintptr)(unsafe.Pointer(&keys[i])) = *(*[2]uintptr)(unsafe.Pointer(&b))
	}
	return keys
}

func (s *Server) InfoCommand(section string) (data []string) {
	if section == "" || strings.EqualFold(section, "server") {
		data = append(data, "# server",
			fmt.Sprintf("version:%v", Version),
			fmt.Sprintf("servername:%v", s.ServerConfig.ServerName),
			fmt.Sprintf("listen:%v", s.ln.Addr()),
			fmt.Sprintf("uptime:%v", time.Since(s.Survey.StartAt)),
			fmt.Sprintf("readonly:%v", s.ReadOnly),
			fmt.Sprintf("connections:%v", s.Survey.Connections),
			fmt.Sprintf("fill_cache:%v", s.fillCache.Len()),
			fmt.Sprintf("wm_cache:%v", s.wmCache.Len()),
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
		iDisk, _ := s.DB.EstimateDiskUsage([]byte("i"), []byte("j"))
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
			"")
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
	if strings.HasPrefix(section, ":") {
		key := section[1:]
		add, del, _ := s.getHLL(key)
		startKey := extdb.GetKeyPrefix(key)
		disk, _ := s.DB.EstimateDiskUsage(startKey, s2pkg.IncBytes(startKey))
		data = append(data, "# key "+key)
		data = append(data, fmt.Sprintf("size:%d", disk))
		if wm, wmok := s.wmCache.Get16(s2pkg.HashStr128(key)); wmok {
			data = append(data, fmt.Sprintf("watermark:%s", hexEncode(wm[:])))
		} else {
			data = append(data, "watermark:miss")
		}
		data = append(data, fmt.Sprintf("hll_add:%d", add.Count()))
		data = append(data, fmt.Sprintf("hll_del:%d", del.Count()))
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
		s2pkg.FormatFloatShort(a), s2pkg.FormatFloatShort(b), s2pkg.FormatFloatShort(c)))
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
	uuid := s2pkg.UUID()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		defer s2pkg.HTTPRecover(w, r)

		q := r.URL.Query()
		start := time.Now()

		if s.ServerConfig.Password != "" && s.ServerConfig.Password != q.Get("p") {
			w.WriteHeader(400)
			w.Write([]byte("s2db: password required"))
			return
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
				r.Value = makeHTMLStat(string(r.Value))
				return template.HTML(fmt.Sprintf("<div class=s-key>%v</div><div class=s-value>%v</div>", r.Key, r.Value))
			},
			"stat":      makeHTMLStat,
			"timeSince": func(a time.Time) time.Duration { return time.Since(a) },
		}).Parse(webuiHTML)).Execute(w, map[string]interface{}{
			"s": s, "start": start,
			"CPU": cpu, "IOPS": iops, "Disk": disk, "REPLPath": uuid, "MetricsNames": s.ListMetricsNames(),
			"Sections": []string{"server", "server_misc", "sys_rw_stats", "command_qps", "command_avg_lat"},
		})
	})
	mux.HandleFunc("/chart/", func(w http.ResponseWriter, r *http.Request) {
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
	mux.HandleFunc("/ssd", func(w http.ResponseWriter, r *http.Request) {
		defer s2pkg.HTTPRecover(w, r)

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
		// 	start = s2pkg.MustParseFloat(sv)
		// }
		// if sv := q.Get("end"); sv != "" {
		// 	end = s2pkg.MustParseFloat(sv)
		// }

		// w.Header().Add("Content-Type", "application/octet-stream")
		// w.Header().Add("Content-Encoding", "gzip")
		// w.Header().Add("Content-Disposition", fmt.Sprintf("attachment; filename=\"ssd_%d.csv\"", clock.UnixNano()))
		// fmt.Println(q.Get("match"))
	})
	mux.HandleFunc("/"+uuid, func(w http.ResponseWriter, r *http.Request) {
		nj.PlaygroundHandler(s.ServerConfig.InspectorSource+"\n--BRK"+uuid+". DO NOT EDIT THIS LINE\n\n"+
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

func (s *Server) wrapMGet(ids [][]byte) (data [][]byte, err error) {
	data, consolidated, err := s.MGet(ids)
	if err != nil {
		return nil, err
	}
	if consolidated {
		s.Survey.AllConsolidated.Incr(1)
		return data, nil
	}
	var missings []any
	for i, d := range data {
		if d == nil {
			missings = append(missings, ids[i])
		}
	}
	if len(missings) == 0 || !s.HasPeers() {
		return data, nil
	}

	missings = append([]any{"ISELECT", "*"}, missings...)
	recv, out := s.ForeachPeerSendCmd(func() redis.Cmder {
		return redis.NewStringSliceCmd(context.TODO(), missings...)
	})
	if recv == 0 {
		return data, nil
	}

	m := map[string]string{}
	s.ProcessPeerResponse(recv, out, func(cmd redis.Cmder) bool {
		m0 := cmd.(*redis.StringSliceCmd).Val()
		for i := 0; i < len(m0); i += 2 {
			m[m0[i]] = m0[i+1]
		}
		return true
	})
	for i, d := range data {
		if d != nil {
			continue
		}
		if v, ok := m[*(*string)(unsafe.Pointer(&ids[i]))]; ok {
			data[i] = []byte(v)
		}
	}
	return data, nil
}

func (s *Server) PeerCount() (c int) {
	for i, p := range s.Peers {
		if p.Redis() != nil && s.Channel != int64(i) {
			c++
		}
	}
	return
}

func (s *Server) HasPeers() bool {
	for i, p := range s.Peers {
		if p.Redis() != nil && s.Channel != int64(i) {
			return true
		}
	}
	return false
}

func (s *Server) ForeachPeer(f func(p *endpoint, c *redis.Client)) {
	for i, p := range s.Peers {
		if cli := p.Redis(); cli != nil && s.Channel != int64(i) {
			f(p, cli)
		}
	}
}

func (s *Server) ForeachPeerSendCmd(f func() redis.Cmder) (int, <-chan *commandIn) {
	recv := 0
	out := make(chan *commandIn, len(s.Peers))
	s.ForeachPeer(func(p *endpoint, cli *redis.Client) {
		select {
		case p.jobq <- &commandIn{e: p, Cmder: f(), wait: out, pstart: future.UnixNano()}:
			recv++
		case <-time.After(time.Duration(s.ServerConfig.TimeoutPeer) * time.Millisecond):
			logrus.Errorf("failed to send peer job (%s), timed out", p.Config().Addr)
		}
	})
	return recv, out
}

func (s *Server) ProcessPeerResponse(recv int, out <-chan *commandIn, f func(redis.Cmder) bool) (success int) {
	if recv == 0 {
		return
	}
MORE:
	select {
	case res := <-out:
		x, _ := s.Survey.PeerLatency.LoadOrStore(res.e.Config().Addr, new(s2pkg.Survey))
		x.(*s2pkg.Survey).Incr((future.UnixNano() - res.pstart) / 1e6)

		if err := res.Cmder.Err(); err != nil {
			logrus.Errorf("[%s] failed to request %s: %v", res.Cmder.Name(), res.e.Config().URI, err)
			if s.test.MustAllPeers {
				panic("not all peers respond")
			}
		} else {
			if f(res.Cmder) {
				success++
			}
		}
		if recv--; recv > 0 {
			goto MORE
		}
	case <-time.After(time.Duration(s.ServerConfig.TimeoutPeer) * time.Millisecond):
		logrus.Errorf("failed to request peer, timed out, remains: %v", recv)
	}
	return
}

func (s *Server) convertPairs(w *wire.Writer, p []s2pkg.Pair, max int) (err error) {
	if len(p) > max {
		p = p[:max]
	}
	a := make([][]byte, 0, len(p)*3)
	for _, p := range p {
		a = append(a, p.IDHex(), p.UnixMilliBytes(), p.Data)
	}
	return w.WriteBulks(a)
}

func (s *Server) translateCursor(buf []byte, desc bool) (start []byte) {
	switch s := *(*string)(unsafe.Pointer(&buf)); s {
	case "+", "+inf", "+INF", "+Inf", "recent", "RECENT":
		start = []byte(maxCursor)
	case "0":
		start = make([]byte, 16)
	default:
		if len(s) == 32 || len(s) == 33 {
			start = hexDecode(buf)
		} else if len(s) == 16 {
			start = buf
		} else if desc {
			start = make([]byte, 16)
			binary.BigEndian.PutUint64(start, uint64(s2pkg.MustParseFloat(s)*1e9+1e9-1))
		} else {
			start = make([]byte, 16)
			binary.BigEndian.PutUint64(start, uint64(s2pkg.MustParseFloat(s)*1e9))
		}
	}
	if *(*string)(unsafe.Pointer(&start)) > maxCursor {
		start = []byte(maxCursor)
	}
	return
}
