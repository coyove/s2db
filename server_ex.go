package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
	"github.com/go-redis/redis/v8"
)

var (
	isReadCommand = map[string]bool{
		"RANGE":  true,
		"IRANGE": true,
	}
	isWriteCommand = map[string]bool{
		"APPEND":        true,
		"IAPPEND":       true,
		"EXPIREBEFORE":  true,
		"IEXPIREBEFORE": true,
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
	if section == "" || section == "server" {
		data = append(data, "# server",
			fmt.Sprintf("version:%v", Version),
			fmt.Sprintf("servername:%v", s.ServerConfig.ServerName),
			fmt.Sprintf("listen:%v", s.ln.Addr()),
			fmt.Sprintf("listen_unix:%v", s.lnLocal.Addr()),
			fmt.Sprintf("uptime:%v", time.Since(s.Survey.StartAt)),
			fmt.Sprintf("readonly:%v", s.ReadOnly),
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
	if section == "" || section == "sys_rw_stats" {
		data = append(data, "# sys_rw_stats",
			fmt.Sprintf("sys_read_qps:%v", s.Survey.SysRead.String()),
			fmt.Sprintf("sys_read_avg_lat:%v", s.Survey.SysRead.MeanString()),
			fmt.Sprintf("sys_write_qps:%v", s.Survey.SysWrite.String()),
			fmt.Sprintf("sys_write_avg_lat:%v", s.Survey.SysWrite.MeanString()),
			fmt.Sprintf("slow_logs_qps:%v", s.Survey.SlowLogs.QPSString()),
			fmt.Sprintf("slow_logs_avg_lat:%v", s.Survey.SlowLogs.MeanString()),
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

	return
}

func joinArray(v interface{}) string {
	rv := reflect.ValueOf(v)
	p := make([]string, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		p = append(p, fmt.Sprint(rv.Index(i).Interface()))
	}
	return strings.Join(p, " ")
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

func (s *Server) webConsoleHandler() {
	if testFlag {
		return
	}
	uuid := s2pkg.UUID()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
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
	http.HandleFunc("/"+uuid, func(w http.ResponseWriter, r *http.Request) {
		nj.PlaygroundHandler(s.ServerConfig.InspectorSource+"\n--BRK"+uuid+". DO NOT EDIT THIS LINE\n\n"+
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

// func (s *Server) Scan(cursor string, flags wire.Flags) (pairs []s2pkg.Pair, nextCursor string) {
// 	count := flags.Count + 1
// 	startCursor := cursor
// 	timedout, start := "", clock.Now()
// 	if len(cursor) > 0 {
// 		switch cursor[0] {
// 		case 'Z':
// 			cursor = cursor[1:]
// 		case 'S':
// 			cursor = cursor[1:]
// 			goto SCAN_SET
// 		case 'K':
// 			cursor = cursor[1:]
// 			goto SCAN_KV
// 		}
// 	}
//
// 	s.ForeachZSet(cursor, func(k string) bool {
// 		if time.Since(start) > flags.Timeout {
// 			timedout = "Z" + k
// 			return false
// 		}
// 		if flags.Match != "" && !s2pkg.Match(flags.Match, k) {
// 			return true
// 		}
// 		pairs = append(pairs, s2pkg.Pair{Member: k, Score: float64(s.ZCard(k)), Data: []byte("zset")})
// 		return len(pairs) < count
// 	})
// 	if len(pairs) >= count {
// 		pairs, nextCursor = pairs[:count-1], "Z"+pairs[count-1].Member
// 		return
// 	}
//
// 	cursor = startCursor
// SCAN_SET:
// 	if timedout == "" {
// 		s.ForeachSet(cursor, func(k string) bool {
// 			if time.Since(start) > flags.Timeout {
// 				timedout = "S" + k
// 				return false
// 			}
// 			if flags.Match != "" && !s2pkg.Match(flags.Match, k) {
// 				return true
// 			}
// 			pairs = append(pairs, s2pkg.Pair{Member: k, Score: float64(s.SCard(k)), Data: []byte("set")})
// 			return len(pairs) < count
// 		})
// 	}
// 	if len(pairs) >= count {
// 		pairs, nextCursor = pairs[:count-1], "S"+pairs[count-1].Member
// 		return
// 	}
//
// 	cursor = startCursor
// SCAN_KV:
// 	if timedout == "" {
// 		s.ForeachKV(cursor, func(k string, v []byte) bool {
// 			if time.Since(start) > flags.Timeout {
// 				timedout = "K" + k
// 				return false
// 			}
// 			if flags.Match != "" && !s2pkg.Match(flags.Match, k) {
// 				return true
// 			}
// 			pairs = append(pairs, s2pkg.Pair{Member: k, Score: float64(len(v)), Data: []byte("string")})
// 			return len(pairs) < count
// 		})
// 	}
// 	if len(pairs) >= count {
// 		pairs, nextCursor = pairs[:count-1], "K"+pairs[count-1].Member
// 	}
//
// 	if timedout != "" {
// 		return pairs, timedout
// 	}
// 	return
// }

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
