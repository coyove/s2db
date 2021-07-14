package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var slaveAddr = flag.String("slave", "", "")
var listenAddr = flag.String("l", ":6379", "")

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
	s, _ := Open("test")
	s.SlaveAddr = *slaveAddr
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
