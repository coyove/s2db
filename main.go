package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	Version = ""

	masterAddr      = flag.String("master", "", "connect to master server")
	listenAddr      = flag.String("l", ":6379", "listen address")
	pprofListenAddr = flag.String("pprof", ":16379", "pprof listen address")
	dataDir         = flag.String("d", "test", "data directory")
	showVersion     = flag.Bool("v", false, "print version")
	readOnly        = flag.Bool("ro", false, "read only server")
	calcShard       = flag.String("calc-shard", "", "simple utility to calc the shard number of the given value")
	benchmark       = flag.Bool("bench", false, "")
)

func main() {
	flag.Parse()
	if *calcShard != "" {
		fmt.Print(shardIndex(*calcShard))
		return
	}
	if *showVersion {
		fmt.Println("s2db", Version)
		return
	}

	rand.Seed(time.Now().Unix())

	log.SetReportCaller(true)
	log.SetFormatter(&LogFormatter{})
	log.SetOutput(io.MultiWriter(os.Stdout, &lumberjack.Logger{
		Filename:   "s2db.log",
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
	if *benchmark {
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

	if rdb.Ping(context.TODO()).Err() == nil {
		return
	}

	log.Info("version: ", Version)
	opened := make(chan bool)
	go func() {
		select {
		case <-opened:
		case <-time.After(time.Second * 2):
			log.Panic("failed to open database, locked by others?")
		}
		log.Info("serving pprof at ", *pprofListenAddr)
		log.Error("pprof: ", http.ListenAndServe(*pprofListenAddr, nil))
	}()

	s, err := Open(*dataDir)
	if err != nil {
		log.Panic(err)
	}
	opened <- true

	s.MasterAddr = *masterAddr
	s.SetReadOnly(*readOnly || s.MasterAddr != "")
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
