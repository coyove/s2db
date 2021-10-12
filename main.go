package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
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

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
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

	showLogTail       = flag.String("logtail", "", "")
	dumpOverWire      = flag.String("dow", "", "")
	checkDumpOverWire = flag.String("check-dow", "", "")

	readOnly   = flag.Bool("ro", false, "start server as read-only")
	masterMode = flag.Bool("M", false, "tag server as master, so it knows its role when losing connections to slaves")

	showVersion = flag.Bool("v", false, "print s2db version")
	calcShard   = flag.String("calc-shard", "", "simple utility to calc the shard number of the given value")
	benchmark   = flag.String("bench", "", "")

	inspector = flag.String("I", "", "inspector script file")

	noFreelistSync = flag.Bool("F", false, "DEBUG flag, do not use")
)

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

	if *dumpOverWire != "" {
		parts := strings.SplitN(*dumpOverWire, "/", 3)
		if len(parts) != 3 {
			log.Panic("dump over wire format: server_address/shard/local_path")
		}
		addr, path := parts[0], parts[2]
		if strings.EqualFold(parts[1], "ALL") {
			os.MkdirAll(path, 0777)
			for i := 0; i < ShardNum; i++ {
				requestDumpShardOverWire(addr, filepath.Join(path, "shard"+strconv.Itoa(i)), i)
			}
		} else {
			requestDumpShardOverWire(addr, path, atoip(parts[1]))
		}
		return
	}

	if *checkDumpOverWire != "" {
		if fi, _ := os.Stat(*checkDumpOverWire); fi != nil && fi.IsDir() {
			for i := 0; i < ShardNum; i++ {
				checkDumpWireFile(filepath.Join(*checkDumpOverWire, "shard"+strconv.Itoa(i)))
			}
		} else {
			checkDumpWireFile(*checkDumpOverWire)
		}
		return
	}

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
	go func() {
		select {
		case <-opened:
		case <-time.After(time.Second * 30):
			log.Panic("failed to open database, locked by others?")
		}
		http.HandleFunc("/", webInfo(&s))
		log.Info("serving HTTP info and pprof at ", *pprofListenAddr)
		log.Error("http: ", http.ListenAndServe(*pprofListenAddr, nil))
	}()
	s, err = Open(*dataDir, opened)
	if err != nil {
		log.Panic(err)
	}

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

	if *inspector != "" {
		buf, err := ioutil.ReadFile(*inspector)
		if err != nil {
			log.Error("invalid inspector file path")
			return
		}
		s.updateConfig("InspectorSource", string(buf), false)
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

func webInfo(ps **Server) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		s := *ps
		section := r.URL.Query().Get("section")
		shard := r.URL.Query().Get("shard")
		password := r.URL.Query().Get("p")

		w.Header().Add("Content-Type", "text/html")
		w.Write([]byte("<meta name='viewport' content='width=device-width, initial-scale=1'><meta charset='utf-8'>"))
		w.Write([]byte("<title>s2db - " + s.ServerName + "</title>"))
		w.Write([]byte("<style>body{line-height:1.5}*{font-family:monospace}pre{white-space:pre-wrap;width:100%}td{padding:0.2em}</style><h1>" + s.ServerName + "</h1>"))
		if s.Password != "" && s.Password != password {
			w.Write([]byte("* password required"))
			return
		}

		if shard != "" {
			start := time.Now()
			w.Write([]byte("<a href='/'>&laquo; index</a>"))
			w.Write([]byte("<pre>" + s.shardInfo(atoip(shard)) + "</pre>"))
			w.Write([]byte("<span>returned in " + time.Since(start).String() + "</span>"))
		} else {
			cpu, disk := getOSUsage()
			w.Write([]byte("<pre>cpu: " + cpu + "%<br><br>" + disk + "</pre><br>"))
			w.Write([]byte("golang pprof: <button onclick='location.href=\"/debug/pprof\"'>open</button><br>"))
			w.Write([]byte("view shard info: <select onchange='location.href=\"?shard=\"+this.value'><option value=''>-</option>"))
			for i := 0; i < ShardNum; i++ {
				w.Write([]byte("<option value=" + strconv.Itoa(i) + ">#" + strconv.Itoa(i) + "</option>"))
			}
			w.Write([]byte("</select> (performance may degrade)<br>remote pprof port: <input type=number id=port value='16379'/><br>"))
			w.Write([]byte("<table border=1 style='margin-top:0.5em'><tr><td>role</td><td>address</td><td>name</td></tr>"))
			const row = `<tr><td>%s</td><td>%s</td><td>%s</td><td><button onclick='location.href="http://%s:"+port.value'>view</button></td></tr>`
			slavesInfo := s.slavesSection()
			s.slaves.Foreach(func(si *serverInfo) {
				w.Write([]byte(fmt.Sprintf(row, "slave", si.RemoteAddr, si.ServerName, si.RemoteAddr)))
				w.Write([]byte(fmt.Sprintf("<tr><td colspan=4><pre>%s</pre></td></tr>", extractSlaveSections(slavesInfo, si.RemoteAddr))))
			})
			if s.MasterAddr != "" {
				host, _, _ := net.SplitHostPort(s.MasterAddr)
				w.Write([]byte(fmt.Sprintf(row, "master", host, s.MasterNameAssert, host)))
			}
			w.Write([]byte("</table><br>"))
			w.Write([]byte("<pre>" + s.Info(section) + "\n# config\n" + strings.Join(s.listConfig(), "\n") + "</pre>"))
		}
	}
}

func getOSUsage() (cpu, disk string) {
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
	buf, _ = exec.Command("df", "-h").Output()
	disk = string(bytes.TrimSpace(buf))
	return
}

func extractSlaveSections(in []string, addr string) string {
	x := []string{}
	for _, row := range in {
		if strings.HasPrefix(row, "slave_"+addr) {
			x = append(x, row[len(addr)+6:])
		}
	}
	return strings.Join(x, "\n")
}
