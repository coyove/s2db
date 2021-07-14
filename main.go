package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/mitchellh/go-ps"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	slaveAddr  = flag.String("slave", "", "")
	listenAddr = flag.String("l", ":6379", "")
	readOnly   = flag.Bool("ro", false, "")
	benchmark  = flag.Bool("bench", false, "")
	coward     = flag.Bool("c", false, "")
)

func main() {
	flag.Parse()
	rand.Seed(time.Now().Unix())

	log.SetReportCaller(true)
	log.SetOutput(io.MultiWriter(os.Stdout, &lumberjack.Logger{
		Filename:   "zset.log",
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

	p, _ := ps.Processes()
	for _, p := range p {
		if strings.Contains(p.Executable(), "zset") && os.Getpid() != p.Pid() {
			if *coward {
				log.Info("coward mode, existing server: ", p.Pid(), ", exit quietly")
				return
			}
			log.Info("terminate old server: ", p.Pid(), exec.Command("kill", "-9", strconv.Itoa(p.Pid())).Run())
			time.Sleep(time.Second)
		}
	}

	s, _ := Open("test")
	s.SlaveAddr = *slaveAddr
	s.ReadOnly = *readOnly
	s.Serve(*listenAddr)

	if false {
		wg := sync.WaitGroup{}
		for i := 0; i < 1000; i += 1 {
			wg.Add(1)
			go func(i int) {
				fmt.Println(i)
				s.ZAdd("test", []Pair{{strconv.Itoa(i), rand.Float64() * 2}}, false, false)
				wg.Done()
			}(i)
		}
		wg.Wait()
	}

	fmt.Println(s.ZCard("test"))
	fmt.Println(s.ZCount("test", "0", "+inf"))
	// fmt.Println(db.rangeScore("test", RangeLimit{Value: "0.1"}, RangeLimit{Value: "0.3"}, 0, 9, true))
	// fmt.Println(db.rangeScoreIndex("test", 0, 20))
	fmt.Println(time.Since(start).Seconds())
}
