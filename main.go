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
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coyove/nj"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	Version = ""

	masterAddr     = flag.String("master", "", "connect to master server, form: master_name@ip:port")
	masterPassword = flag.String("mp", "", "connect to master server with password")
	listenAddr     = flag.String("l", ":6379", "listen address")
	dataDir        = flag.String("d", "test", "data directory")
	readOnly       = flag.Bool("ro", false, "start server as read-only")
	masterMode     = flag.Bool("M", false, "tag server as master, so it knows its role when losing connections to slaves")

	noWebUI     = flag.Bool("no-web-console", false, "disable web console interface")
	showLogTail = flag.String("logtail", "", "")
	showVersion = flag.Bool("v", false, "print s2db version")
	calcShard   = flag.String("calc-shard", "", "simple utility to calc the shard number of the given value")
	benchmark   = flag.String("bench", "", "")
	configSet   = func() (f [4]*string) {
		for i := range f {
			f[i] = flag.String("C"+strconv.Itoa(i), "", "update config before serving, form: key=value")
		}
		return f
	}()
)

//go:embed scripts/index.html
var webuiHTML string

var slowLogger *log.Logger

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

	var s *Server
	var err error
	var configOpened = make(chan bool)
	var fullyOpened sync.Mutex
	go func() {
		select {
		case <-configOpened:
		case <-time.After(time.Second * 30):
			log.Panic("failed to open database, locked by others?")
		}
		if *noWebUI {
			log.Info("web console and pprof disabled")
			return
		}
		fullyOpened.Lock()
		sp := s2pkg.UUID()
		http.HandleFunc("/", webConsole(sp, &s))
		http.HandleFunc("/"+sp, func(w http.ResponseWriter, r *http.Request) {
			nj.PlaygroundHandler("local smc = --<<BRK"+sp+"\n"+
				s.InspectorSource+"\nBRK"+sp+"\n\n"+
				"local ok, err = server.UpdateConfig('InspectorSource', smc, false)\n"+
				"println(ok, err)", s.getScriptEnviron())(w, r)
		})
		s.lnWeb, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			log.Panic(err)
		}
		log.Info("serving web console and pprof at ", s.lnWeb.Addr())
		fullyOpened.Unlock()
		log.Error("http: ", http.Serve(s.lnWeb, nil))
	}()
	fullyOpened.Lock()
	s, err = Open(*dataDir, configOpened)
	if err != nil {
		log.Panic(err)
	}
	fullyOpened.Unlock()

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

	if *masterAddr != "" {
		parts := strings.Split(*masterAddr, "@")
		if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
			log.Error("invalid master address, form: master_name@ip:port")
			return
		}
		s.MasterNameAssert = parts[0]
		s.MasterAddr = parts[1]
	}

	s.MasterMode = *masterMode
	s.MasterPassword = *masterPassword
	s.ReadOnly = *readOnly || s.MasterAddr != ""
	s.Serve(*listenAddr)
}

func webConsole(evalPath string, ps **Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		s, q := *ps, r.URL.Query()
		shard := q.Get("shard")
		password := q.Get("p")
		ins := q.Get("inspector")
		chartSources := strings.Split(q.Get("chart"), ",")

		if shard == "" {
			shard = "-1"
		}

		if len(chartSources) > 0 && chartSources[0] != "" {
			w.Header().Add("Content-Type", "text/json")
			data, _ := s.GetMetricsPairs(0, 0, chartSources...)
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

		if s.Password != "" && s.Password != password {
			w.Header().Add("Content-Type", "text/html")
			w.WriteHeader(400)
			w.Write([]byte("* password required"))
			return
		}

		if ins != "" {
			s.UpdateConfig("InspectorSource", ins, false)
			http.Redirect(w, r, "/?p="+password, http.StatusFound)
			return
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
			"kv": func(s string) (r struct{ Key, Value string }) {
				r.Key = s
				if idx := strings.Index(s, ":"); idx > 0 {
					r.Key, r.Value = s[:idx], s[idx+1:]
				}
				return
			},
			"box32": func(v string) template.HTML {
				if v == "-" {
					parts := [ShardNum]string{}
					for i := range parts {
						parts[i] = fmt.Sprintf("<div class='box shard shard%d'><a href='?shard=%d&p=%s'>#%d</a></div>", i, i, s.Password, i)
					}
					return template.HTML(strings.Join(parts[:], ""))
				}
				parts := strings.Split(v, " ")
				for i, p := range parts {
					parts[i] = "<div class=box>" + p + "</div>"
				}
				return template.HTML(strings.Join(parts, ""))
			},
			"timeSince": func(a time.Time) time.Duration { return time.Since(a) },
			"stat":      makeHTMLStat,
		}).Parse(webuiHTML)).Execute(w, map[string]interface{}{
			"s": s, "start": time.Now(), "CPU": cpu, "IOPS": iops, "Disk": disk, "REPLPath": evalPath,
			"Sections":     []string{"server", "server_misc", "replication", "sys_rw_stats", "batch", "command_qps", "command_avg_lat", "cache"},
			"Slaves":       s.Slaves.List(),
			"MetricsNames": s.ListMetricsNames(),
			"Shard":        s2pkg.MustParseInt(shard), "ShardNum": ShardNum,
		})
	}
}
