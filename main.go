package main

import (
	"context"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coyove/nj"
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	Version = ""

	masterConnString = flag.String("master", "", "connect to master server, minimal form: <Ip>:<Port>/?Name=<MasterName>")
	listenAddr       = flag.String("l", ":6379", "listen address")
	dataDir          = flag.String("d", "test", "data directory")
	readOnly         = flag.Bool("ro", false, "start server as read-only, slaves are always read-only")
	masterMode       = flag.Bool("M", false, "tag server as master even it may not have any slaves")

	showLogTail = flag.String("logtail", "", "")
	showVersion = flag.Bool("v", false, "print s2db version")
	calcShard   = flag.String("calc-shard", "", "simple utility to calc the shard number of the given value")
	benchmark   = flag.String("bench", "", "")
	configSet   = func() (f [6]*string) {
		for i := range f {
			f[i] = flag.String("C"+strconv.Itoa(i), "", "update config before serving, form: key=value")
		}
		return f
	}()

	testFlag   = false
	slowLogger *log.Logger
)

//go:embed scripts/index.html
var webuiHTML string

func main() {
	flag.Parse()
	rand.Seed(time.Now().Unix())
	go s2pkg.OSWatcher()

	if *calcShard != "" {
		fmt.Print(shardIndex(*calcShard))
		return
	}
	if *showVersion {
		fmt.Println("s2db", Version)
		return
	}

	log.SetReportCaller(true)
	log.SetFormatter(&s2pkg.LogFormatter{})
	log.SetOutput(io.MultiWriter(os.Stdout, &lumberjack.Logger{
		Filename: "log/runtime.log", MaxSize: 100, MaxBackups: 8, MaxAge: 28, Compress: true,
	}))

	slowLogger = log.New()
	slowLogger.SetFormatter(&s2pkg.LogFormatter{SlowLog: true})
	slowLogger.SetOutput(io.MultiWriter(os.Stdout, &lumberjack.Logger{
		Filename: "log/slow.log", MaxSize: 100, MaxBackups: 16, MaxAge: 7, Compress: true,
	}))

	rdb := redis.NewClient(&redis.Options{
		Addr:        *listenAddr,
		DialTimeout: time.Second / 2,
	})

	start := time.Now()
	if *benchmark == "write" {
		wg := sync.WaitGroup{}
		ctx := context.TODO()
		for i := 0; i < 100; i += 1 {
			wg.Add(1)
			go func(i int) {
				fmt.Println("client #", i)
				p := rdb.Pipeline()
				for c := 0; c < 100; c++ {
					p.ZAdd(ctx, "bench", &redis.Z{Member: strconv.Itoa(c), Score: rand.Float64()*10 - 5})
				}
				_, err := p.Exec(ctx)
				if err != nil {
					fmt.Println(err)
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
		fmt.Println(time.Since(start).Seconds())
		return
	}

	if *benchmark == "seqwrite" {
		ctx := context.TODO()
		for i := 0; i < 1000; i += 1 {
			args := []interface{}{"ZADD", "seqbench"}
			for c := 0; c < 100; c++ {
				args = append(args, i*100+c, fmt.Sprintf("s%09d", i*100+c))
			}
			cmd := redis.NewStringCmd(ctx, args...)
			rdb.Process(ctx, cmd)
			if cmd.Err() != nil {
				fmt.Println(i, cmd.Err())
			}
		}
		fmt.Println(time.Since(start).Seconds())
		return
	}

	if *benchmark != "" {
		ctx := context.TODO()
		for i := 0; i < 100; i += 1 {
			go func(i int) {
				fmt.Println("client #", i)
				for {
					start, end := rand.Intn(10), rand.Intn(10)+10
					err := rdb.ZRevRange(ctx, *benchmark, int64(start), int64(end)).Err()
					if err != nil {
						fmt.Println(err)
					}
				}
			}(i)
		}
		select {}
	}

	if *showLogTail != "" {
		db, err := bbolt.Open(*showLogTail, 0666, bboltReadonlyOptions)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(-1)
		}
		defer db.Close()
		var tail, seq uint64
		db.View(func(tx *bbolt.Tx) error {
			bk := tx.Bucket([]byte("wal"))
			if bk != nil {
				k, _ := bk.Cursor().Last()
				if len(k) == 8 {
					tail = binary.BigEndian.Uint64(k)
				}
				seq = bk.Sequence()
			}
			return nil
		})
		fmt.Println(tail, seq)
		return
	}

	if err := rdb.Ping(context.TODO()).Err(); err == nil || strings.Contains(err.Error(), "NOAUTH") {
		return
	}

	log.Info("version: ", Version)

	s, err := Open(*dataDir)
	if err != nil {
		log.Panic(err)
	}

	for _, cd := range configSet {
		if idx := strings.Index(*cd, "="); idx != -1 {
			key, value := (*cd)[:idx], (*cd)[idx+1:]
			old, _ := s.getConfig(key)
			log.Infof("update %s from %q to %q", key, old, value)
			if _, err := s.UpdateConfig(key, value, false); err != nil {
				log.Panic(err)
			}
		}
	}

	if *masterConnString != "" {
		s.MasterConfig, err = redisproto.ParseConnString(*masterConnString)
		if err != nil {
			log.Error("invalid master endpoint: ", err)
			return
		}
		if s.MasterConfig.Name == "" {
			log.Error("master name must be provided")
			return
		}
		if s.ServerName == "" {
			log.Error("slave name must be provided, use flag: '-C0 ServerName=<Name>'")
			return
		}
		s.MasterRedis = s.MasterConfig.GetClient()
	}

	s.MasterMode = *masterMode
	if *readOnly || s.MasterConfig.Name != "" {
		s.ReadOnly = 1
	}
	log.Error(s.Serve(*listenAddr))
}

func (s *Server) webConsoleServer() {
	if testFlag {
		return
	}
	sp := s2pkg.UUID()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		start := time.Now()

		if s.DisableWebConsole == 1 {
			w.Write([]byte("s2db: web console disabled"))
			return
		}

		if dumpShard := q.Get("dump"); dumpShard != "" {
			x := &s.db[s2pkg.MustParseInt(dumpShard)]
			w.Header().Add("Content-Type", "application/octet-stream")
			w.Header().Add("X-Size", strconv.Itoa(int(x.Size())))
			m := s.DumpSafeMargin * 1024 * 1024 * (1 + s2pkg.ParseInt(q.Get("dump-margin-x")))
			if err := x.DumpTo(w, m); err != nil {
				log.Errorf("http dumper #%s: %v", dumpShard, err)
			}
			log.Infof("http dumper #%s finished in %v", dumpShard, time.Since(start))
			return
		}

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
			w.Write([]byte("s2db: password required"))
			return
		}

		shardInfos, wg := [ShardNum][]string{}, sync.WaitGroup{}
		if q.Get("noshard") != "1" {
			for i := 0; i < ShardNum; i++ {
				wg.Add(1)
				go func(i int) { shardInfos[i] = s.ShardInfo(i); wg.Done() }(i)
			}
			wg.Wait()
		}

		sp := []string{}
		for i := range s.db {
			sp = append(sp, filepath.Dir(s.db[i].Path()))
		}
		if s.ServerConfig.CompactDumpTmpDir != "" {
			sp = append(sp, s.CompactDumpTmpDir)
		}
		cpu, iops, disk := s2pkg.GetOSUsage(sp[:])
		w.Header().Add("Content-Type", "text/html")
		template.Must(template.New("").Funcs(template.FuncMap{
			"kv": func(s string) template.HTML {
				var r struct{ Key, Value template.HTML }
				r.Key = template.HTML(s)
				if idx := strings.Index(s, ":"); idx > 0 {
					r.Key, r.Value = template.HTML(s[:idx]), template.HTML(s[idx+1:])
				}
				if strings.Count(string(r.Value), " ") == 31 {
					parts := strings.Split(string(r.Value)+"   ", " ")
					for i, p := range parts {
						parts[i] = "<div class=box>" + p + "</div>"
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
			"s": s, "start": start, "CPU": cpu, "IOPS": iops, "Disk": disk, "REPLPath": sp,
			"Sections":     []string{"server", "server_misc", "replication", "slave", "sys_rw_stats", "batch", "command_qps", "command_avg_lat", "cache"},
			"ShardInfo":    shardInfos,
			"MetricsNames": s.ListMetricsNames(),
		})
	})
	http.HandleFunc("/"+sp, func(w http.ResponseWriter, r *http.Request) {
		nj.PlaygroundHandler(s.InspectorSource+"\n--BRK"+sp+". DO NOT EDIT THIS LINE\n\n"+
			"local ok, err = server.UpdateConfig('InspectorSource', SOURCE_CODE.findsub('\\n--BRK"+sp+"'), false)\n"+
			"println(ok, err)", s.getScriptEnviron())(w, r)
	})
	go http.Serve(s.lnWebConsole, nil)
}
