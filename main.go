package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coyove/nj"
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	Version = ""

	listenAddr   = flag.String("l", ":6379", "listen address")
	dataDir      = flag.String("d", "test", "data directory")
	readOnly     = flag.Bool("ro", false, "start server as read-only, slaves are always read-only")
	showVersion  = flag.Bool("v", false, "print s2db version then exit")
	sendRedisCmd = flag.String("cmd", "", "send redis command to the address specified by '-l' then exit")
	benchmark    = flag.String("bench", "", "")
	configSet    = func() (f [6]*string) {
		for i := range f {
			f[i] = flag.String("C"+strconv.Itoa(i), "", "update config before serving, form: key=value")
		}
		return f
	}()
	pebbleMemtableSize = flag.Int("pebble.memtablesize", 128, "[pebble] memtable size in megabytes")
	pebbleCacheSize    = flag.Int("pebble.cachesize", 1024, "[pebble] cache size in megabytes")

	testFlag   = false
	slowLogger *log.Logger
	dbLogger   *log.Logger
)

//go:embed scripts/index.html
var webuiHTML string

func main() {
	flag.Parse()
	rand.Seed(time.Now().Unix())
	go s2pkg.OSWatcher()

	if *showVersion {
		fmt.Println("s2db", Version)
		return
	}

	if *sendRedisCmd != "" {
		cfg, err := redisproto.ParseConnString(*listenAddr)
		if err != nil {
			errorExit("cmd: invalid address: " + err.Error())
		}
		v, err := cfg.GetClient().Do(context.TODO(), redisproto.SplitCmdLine(*sendRedisCmd)...).Result()
		if err != nil {
			errorExit("cmd: " + err.Error())
		}
		fmt.Println(v)
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
		Filename: "log/slow.log", MaxSize: 100, MaxBackups: 8, MaxAge: 7, Compress: true,
	}))

	dbLogger = log.New()
	dbLogger.SetFormatter(&s2pkg.LogFormatter{SlowLog: true})
	dbLogger.SetOutput(io.MultiWriter(os.Stdout, &lumberjack.Logger{
		Filename: "log/db.log", MaxSize: 100, MaxBackups: 16, MaxAge: 28, Compress: true,
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

	if err := rdb.Ping(context.TODO()).Err(); err == nil || strings.Contains(err.Error(), "NOAUTH") {
		return
	}

	log.Info("version: ", Version)
	flag.VisitAll(func(f *flag.Flag) {
		if v := f.Value.String(); v != "" && f.Name != "v" {
			log.Info("[flag] ", f.Name, "=", v)
		}
	})

	s, err := Open(*dataDir)
	if err != nil {
		errorExit(err.Error())
	}

	for _, cd := range configSet {
		if idx := strings.Index(*cd, "="); idx != -1 {
			key, value := (*cd)[:idx], (*cd)[idx+1:]
			old, _ := s.GetConfig(key)
			log.Infof("update %s from %q to %q", key, old, value)
			if _, err := s.UpdateConfig(key, value, false); err != nil {
				errorExit(err.Error())
			}
		}
	}

	s.ReadOnly = *readOnly
	log.Error(s.Serve(*listenAddr))
	time.Sleep(time.Second)
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
