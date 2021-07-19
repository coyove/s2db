package main

import (
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

	masterAddr = flag.String("master", "", "")
	listenAddr = flag.String("l", ":6379", "")
	dataDir    = flag.String("d", "test", "")
	readOnly   = flag.Bool("ro", false, "")
	benchmark  = flag.Bool("bench", false, "")
)

func main() {
	flag.Parse()
	rand.Seed(time.Now().Unix())

	log.SetReportCaller(true)
	log.SetOutput(io.MultiWriter(os.Stdout, &lumberjack.Logger{
		Filename:   filepath.Join(*dataDir, "s2db.log"),
		MaxSize:    100, // megabytes
		MaxBackups: 16,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
	}))

	start := time.Now()
	if *benchmark {
		rdb := redis.NewClient(&redis.Options{Addr: *listenAddr})
		wg := sync.WaitGroup{}
		ctx := context.TODO()
		for i := 0; i < 10; i += 1 {
			wg.Add(1)
			go func(i int) {
				fmt.Println("client #", i)
				p := rdb.Pipeline()
				for c := 0; c < 100; c += 10 {
					data := []*redis.Z{}
					for cc := c; cc < c+10; cc++ {
						data = append(data, &redis.Z{Member: strconv.Itoa(c), Score: rand.Float64()*10 - 5})
					}
					p.ZAdd(ctx, "bench"+strconv.Itoa(i), data...)
					// p.ZAdd("bench"+strconv.Itoa(i), []Pair{{strconv.Itoa(c), rand.Float64() * 2}}, false, false)
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

	go func() {
		log.Info("serving pprof at :16379")
		log.Error("pprof: ", http.ListenAndServe(":16379", nil))
	}()

	s, err := Open(*dataDir)
	if err != nil {
		log.Panic(err)
	}

	s.MasterAddr = *masterAddr
	s.SetReadOnly(*readOnly)
	if s.MasterAddr != "" {
		s.SetReadOnly(true)
	}

	s.Serve(*listenAddr)
}
