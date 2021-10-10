package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
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
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Type", "text/html")
			section := r.URL.Query().Get("section")
			shard := r.URL.Query().Get("shard")
			w.Write([]byte("<meta charset='utf-8'><style>*{font-family:monospace}</style><h1>" + s.ServerName + "</h1><a href='/debug/pprof'>pprof</a><br><br>"))
			if shard != "" {
				start := time.Now()
				w.Write([]byte("<a href='/'>&laquo; index</a><title>Shard #" + shard + "</title>"))
				w.Write([]byte("<pre>" + s.shardInfo(atoip(shard)) + "</pre>"))
				w.Write([]byte("<span>returned in " + time.Since(start).String() + "</span>"))
			} else {
				for i := 0; i < ShardNum; i++ {
					w.Write([]byte(fmt.Sprintf("<a href='?shard=%d'>shard #%02d &raquo;</a>&nbsp;", i, i)))
					if i%4 == 3 {
						w.Write([]byte("<br>"))
					}
				}
				w.Write([]byte("<br><br>pprof port: <input id=port value='16379'/><br>"))
				s.slaves.Foreach(func(si *serverInfo) {
					w.Write([]byte(fmt.Sprintf(`<a href='javascript:location.href="http://%s:"+port.value'>[S] %s (%s)</a>
                    <a href='javascript:location.href="http://%s:"+port.value+"/debug/pprof"'>pprof</a><br>`,
						si.RemoteAddr, si.RemoteAddr, si.ServerName, si.RemoteAddr)))
				})
				if s.MasterAddr != "" {
					w.Write([]byte(fmt.Sprintf(`<a href='javascript:location.href="http://%s:"+port.value'>[M] %s (%s)</a>`, s.MasterAddr, s.MasterAddr, s.MasterNameAssert)))
				}
				w.Write([]byte("<br>"))
				w.Write([]byte("<pre>" + s.info(section) + "</pre><title>Server Status</title>"))
			}
		})
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
		if _, err := s.updateConfig("servername", *serverName); err != nil {
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
