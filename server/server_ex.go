package server

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
	"net/http/pprof"
	"net/url"
	"os"
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
	"github.com/sirupsen/logrus"
)

var (
	isReadCommand = map[string]bool{
		"PSELECT":  true,
		"PHGETALL": true,
		"PLOOKUP":  true,
		"SELECT":   true,
		"LOOKUP":   true,
		"COUNT":    true,
		"SCAN":     true,
		"HSYNC":    true,
		"HLEN":     true,
		"HGET":     true,
		"HTIME":    true,
		"HGETALL":  true,
	}
	isWriteCommand = map[string]bool{
		"APPEND": true,
		"HSET":   true,
	}

	//go:embed index.html
	webuiHTML string
)

func (s *Server) InfoCommand(section string) (data []string) {
	if section == "" || strings.EqualFold(section, "server") {
		cwd, _ := os.Getwd()
		data = append(data, "# server",
			fmt.Sprintf("version:%v", Version),
			fmt.Sprintf("servername:%v", s.Config.ServerName),
			fmt.Sprintf("listen:%v", s.lnRESP.Addr()),
			fmt.Sprintf("uptime:%v", time.Since(s.Survey.StartAt)),
			fmt.Sprintf("wall_clock:%v", time.Now().Format("15:04:05.000000000")),
			fmt.Sprintf("future_clock:%v", time.Unix(0, future.UnixNano()).Format("15:04:05.000000000")),
			fmt.Sprintf("readonly:%v", s.ReadOnly),
			fmt.Sprintf("connections:%v", s.Survey.Connections),
			fmt.Sprintf("cwd:%v", cwd),
			fmt.Sprintf("args:%v", strings.Join(os.Args, " ")),
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
		hDisk, _ := s.DB.EstimateDiskUsage([]byte("h"), []byte("i"))
		data = append(data, "# server_misc",
			fmt.Sprintf("data_files:%d", len(dataFiles)),
			fmt.Sprintf("data_size:%d", dataSize),
			fmt.Sprintf("data_size_mb:%.2f", float64(dataSize)/1024/1024),
			fmt.Sprintf("index_size:%d", iDisk),
			fmt.Sprintf("index_size_mb:%.2f", float64(iDisk)/1024/1024),
			fmt.Sprintf("hashmap_size:%d", hDisk),
			fmt.Sprintf("hashmap_size_mb:%.2f", float64(hDisk)/1024/1024),
			fmt.Sprintf("hll_size:%d", HDisk),
			fmt.Sprintf("hll_size_mb:%.2f", float64(HDisk)/1024/1024),
			fmt.Sprintf("fill_cache:%v", s.fillCache.Len()),
			fmt.Sprintf("wm_cache:%v", s.wmCache.Len()),
			fmt.Sprintf("ttl_once:%v", s.ttlOnce.Count()),
			fmt.Sprintf("distinct_once:%v", s.distinctOnce.Count()),
			fmt.Sprintf("hash_sync:%v", s.hashSyncOnce.Count()),
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
			if m, ok := s.Survey.PeerLatency.Load(p.Config().Addr); ok {
				data = append(data, fmt.Sprintf("peer%d_qps:%v", i, m.(*s2.Survey).QPSString()))
				data = append(data, fmt.Sprintf("peer%d_lat:%v", i, m.(*s2.Survey).MeanString()))
			}
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
	if section == "" || strings.EqualFold(section, "command_qps") {
		data = append(data, "# command_qps")
		s.Survey.Command.Range(func(k, v interface{}) bool {
			data = append(data, fmt.Sprintf("%v:%v", k, v.(*s2.Survey).QPSString()))
			return true
		})
		data = append(data, "")
	}
	if section == "" || strings.EqualFold(section, "command_lat") {
		data = append(data, "# command_lat")
		s.Survey.Command.Range(func(k, v interface{}) bool {
			data = append(data, fmt.Sprintf("%v:%v", k, v.(*s2.Survey).MeanString()))
			return true
		})
		data = append(data, "")
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
		v, key, _ := s.implLookupID(id)
		data = append(data, fmt.Sprintf("hash:%08x", id[8:12]))
		data = append(data, fmt.Sprintf("key:%s", key))
		data = append(data, fmt.Sprintf("data_size:%d", len(v)))
		data = append(data, "")
	}
	if strings.HasPrefix(section, "#") {
		count, _ := s.implHLen(section[1:])
		hash, size, _ := s.hChecksum(section[1:])
		data = append(data, "# hashmap "+section[1:])
		data = append(data, fmt.Sprintf("size:%d", size))
		data = append(data, fmt.Sprintf("count:%d", count))
		data = append(data, fmt.Sprintf("hash:%x", hash))
		data = append(data, "")
	}
	if strings.HasPrefix(section, "=") {
		data = append(data, "# metrics "+section[1:])
		switch v := s.getMetricsCommand(section[1:]).(type) {
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
		if strings.HasPrefix(s, "http://") {
			return template.HTML(fmt.Sprintf("<a href='%s'>%s</a>", s, s))
		}
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
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
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
			"Sections": []string{"server", "server_misc", "sys_rw_stats", "peers", "command_qps", "command_lat"},
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
		data := s.GetMetrics(chartSources, int64(startTs)*1e6, int64(endTs)*1e6)
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
	data, _, err = s.implLookupID(id)
	if err != nil {
		return nil, err
	}
	if len(data) > 0 || !s.HasOtherPeers() {
		return data, nil
	}
	var m []byte
	s.ForeachPeerSendCmd(SendCmdOptions{Oneshot: true}, func() redis.Cmder {
		return redis.NewStringCmd(context.TODO(), "PLOOKUP", id)
	}, func(cmd redis.Cmder) bool {
		m0, _ := cmd.(*redis.StringCmd).Bytes()
		if len(m0) > 0 {
			m = m0
			return true
		}
		return false
	})
	return m, nil
}

func (s *Server) syncHashmap(key string, sync bool) error {
	if !s.HasOtherPeers() {
		return nil
	}

	work := func() error {
		defer func(start time.Time) {
			s.Survey.HashSyncer.Incr(time.Since(start).Milliseconds())
		}(time.Now())

		checksum, _, err := s.hChecksum(key)
		if err != nil {
			return err
		}

		var pres [][]byte
		_, success := s.ForeachPeerSendCmd(SendCmdOptions{}, func() redis.Cmder {
			return redis.NewStringCmd(context.TODO(), "PHGETALL", key, checksum[:])
		}, func(cmd redis.Cmder) bool {
			p, _ := cmd.(*redis.StringCmd).Bytes()
			pres = append(pres, p)
			return true
		})
		if sync && success != s.OtherPeersCount() {
			return fmt.Errorf("sync failed")
		}

		bkLive := makeHashSetKey(key)
		tx := s.DB.NewBatch()
		defer tx.Close()
		for _, p := range pres {
			if len(p) == 0 {
				continue
			}
			if err := tx.Merge(bkLive, p, pebble.Sync); err != nil {
				return err
			}
		}
		if err := tx.Commit(pebble.Sync); err != nil {
			return err
		}
		return nil
	}

	if sync {
		return work()
	}
	if s.hashSyncOnce.Lock(key) {
		s.Survey.HashSyncOnce.Incr(s.hashSyncOnce.Count())
		go func() {
			if err := work(); err != nil {
				logrus.Errorf("hashmap sync error: %v", err)
			}
			time.AfterFunc(time.Second, func() { s.hashSyncOnce.Unlock(key) })
		}()
	}
	return nil
}

func (s *Server) convertPairs(w wire.WriterImpl, p []s2.Pair, max int, q bool) (err error) {
	if len(p) > max {
		p = p[:max]
	}
	a := make([][]byte, 0, len(p)*3)
	var maxFuture future.Future
	for _, p := range p {
		p.Q = q
		d := p.Data
		if v, ok := p.Future().Cookie(); ok {
			d = append(strconv.AppendInt(append(d, "[[mark="...), int64(v), 10), "]]"...)
		}
		a = append(a, p.IDHex(), p.UnixMilliBytes(), d)
		if p.Future() > maxFuture {
			maxFuture = p.Future()
		}
	}

	// Data may contain Pairs from the future created by other channels. We wait until we reach the timestamp.
	maxFuture.Wait()
	return w.WriteBulks(a)
}

func (s *Server) translateCursor(buf []byte, desc bool) (start []byte) {
	switch x := *(*string)(unsafe.Pointer(&buf)); x {
	case "+", "+inf", "+INF", "+Inf":
		start = []byte(maxCursor)
	case "recent", "RECENT", "now", "NOW":
		tmp := s2.ConvertFutureTo16B(future.Future(future.UnixNano()))
		start = tmp[:]
	case "0", "":
		start = make([]byte, 16)
	default:
		if len(x) == 32 || len(x) == 33 {
			start = hexDecode(buf)
		} else if len(x) == 16 {
			start = s2.Bytes(buf)
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
