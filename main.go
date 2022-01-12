package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/binary"
	"flag"
	"fmt"
	"html/template"
	"io"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coyove/nj"
	"github.com/coyove/s2db/internal"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	Version = ""

	masterAddr     = flag.String("master", "", "connect to master server, form: master_name@ip:port")
	masterPassword = flag.String("mp", "", "")

	listenAddr      = flag.String("l", ":6379", "listen address")
	pprofListenAddr = flag.String("pprof", ":16379", "pprof listen address")
	serverName      = flag.String("n", "", "same as: CONFIG SET servername <Name>")
	dataDir         = flag.String("d", "test", "data directory")

	showLogTail = flag.String("logtail", "", "")

	readOnly   = flag.Bool("ro", false, "start server as read-only")
	masterMode = flag.Bool("M", false, "tag server as master, so it knows its role when losing connections to slaves")

	showVersion = flag.Bool("v", false, "print s2db version")
	calcShard   = flag.String("calc-shard", "", "simple utility to calc the shard number of the given value")
	benchmark   = flag.String("bench", "", "")

	noFreelistSync = flag.Bool("F", false, "DEBUG flag, do not use")
)

//go:embed internal/webui.html
var webuiHTML string

func main() {
	flag.Parse()
	rand.Seed(time.Now().Unix())

	if *calcShard != "" {
		fmt.Print(shardIndex(*calcShard))
		return
	}
	if *showVersion {
		fmt.Println("s2db", Version)
		return
	}
	bboltOptions.NoFreelistSync = *noFreelistSync

	log.SetReportCaller(true)
	log.SetFormatter(&LogFormatter{})
	log.SetOutput(io.MultiWriter(os.Stdout, &lumberjack.Logger{
		Filename:   "x_s2db.log",
		MaxSize:    100, // megabytes
		MaxBackups: 16,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
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
	var opened = make(chan bool)
	var fullyOpened sync.Mutex
	go func() {
		select {
		case <-opened:
		case <-time.After(time.Second * 30):
			log.Panic("failed to open database, locked by others?")
		}
		fullyOpened.Lock()
		sp := internal.UUID()
		http.HandleFunc("/", webInfo(sp, &s))
		http.HandleFunc("/"+sp, func(w http.ResponseWriter, r *http.Request) {
			nj.PlaygroundHandler(s.getCompileOptions())(w, r)
		})
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		log.Info("serving HTTP info and pprof at ", ln.Addr())
		s.lnWeb = ln
		fullyOpened.Unlock()
		log.Error("http: ", http.Serve(ln, nil))
	}()
	fullyOpened.Lock()
	s, err = Open(*dataDir, opened)
	if err != nil {
		log.Panic(err)
	}
	fullyOpened.Unlock()

	if *serverName != "" {
		old, _ := s.getConfig("servername")
		log.Infof("update server name from %q to %q", old, *serverName)
		if _, err := s.updateConfig("servername", *serverName, false); err != nil {
			log.Fatal(err)
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

type LogFormatter struct{}

func (f *LogFormatter) Format(entry *log.Entry) ([]byte, error) {
	buf := bytes.Buffer{}
	if entry.Level <= log.ErrorLevel {
		buf.WriteString("@err")
	} else {
		buf.WriteString("@info")
	}
	if v, ok := entry.Data["shard"]; ok {
		buf.WriteString("`shard")
		buf.WriteString(v.(string))
	}
	buf.WriteString("\t")
	buf.WriteString(entry.Time.UTC().Format("2006-01-02T15:04:05.000\t"))
	buf.WriteString(filepath.Base(entry.Caller.File))
	buf.WriteString(":")
	buf.WriteString(strconv.Itoa(entry.Caller.Line))
	buf.WriteString("\t")
	buf.WriteString(entry.Message)
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

func webInfo(evalPath string, ps **Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		s, q := *ps, r.URL.Query()
		// section := q.Get("section")
		shard := q.Get("shard")
		password := q.Get("p")
		ins := q.Get("inspector")

		if s.Password != "" && s.Password != password {
			w.Header().Add("Content-Type", "text/html")
			w.WriteHeader(400)
			w.Write([]byte("* password required"))
			return
		}

		if ins != "" {
			s.updateConfig("InspectorSource", ins, false)
			http.Redirect(w, r, "/?p="+password, http.StatusFound)
			return
		}

		start := time.Now()
		cpu, disk := getOSUsage(s)
		w.Header().Add("Content-Type", "text/html")
		template.Must(template.New("").Funcs(template.FuncMap{
			"kv": func(s string) struct{ Key, Value string } {
				if idx := strings.Index(s, ":"); idx > 0 {
					return struct{ Key, Value string }{s[:idx], s[idx+1:]}
				}
				return struct{ Key, Value string }{s, ""}
			},
		}).Parse(webuiHTML)).Execute(w, map[string]interface{}{
			"s": s, "Elapsed": time.Since(start).Seconds(),
			"Sections": map[string]string{
				"Server": "server", "Others": "server_misc", "Replication": "replication",
				"Read-write": "sys_rw_stats", "Batch": "batch", "Cache": "cache",
			},
			"Slaves": s.Slaves.List(), "Shard": shard, "ShardNum": ShardNum, "CPU": cpu, "Disk": disk, "REPLPath": evalPath,
		})
	}
}

func getOSUsage(s *Server) (cpu string, diskFreeGB uint64) {
	buf, _ := exec.Command("ps", "aux").Output()
	pid := []byte(" " + strconv.Itoa(os.Getpid()) + " ")
	for _, line := range bytes.Split(buf, []byte("\n")) {
		if bytes.Contains(line, pid) {
			line = bytes.TrimSpace(line[bytes.Index(line, pid)+len(pid):])
			end := bytes.IndexByte(line, ' ')
			if end == -1 {
				break
			}
			cpu = string(line[:end])
			break
		}
	}
	var stat unix.Statfs_t
	unix.Statfs(s.DataPath, &stat)
	diskFreeGB = stat.Bavail * uint64(stat.Bsize) / 1024 / 1024 / 1024
	return
}
