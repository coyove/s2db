package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/coyove/s2db/s2/resp"
	"github.com/coyove/s2db/server"
	"github.com/coyove/sdss/future"
	log "github.com/sirupsen/logrus"
)

var (
	listenAddr       = flag.String("l", ":6379", "listen address")
	dataDir          = flag.String("d", "test", "data directory")
	writeConfig      = flag.Bool("C", false, "init config then exit")
	readOnly         = flag.Bool("ro", false, "start server as readonly")
	channel          = flag.Int64("ch", -1, "channel 0-"+strconv.Itoa(int(future.Channels)-1))
	showVersion      = flag.Bool("v", false, "print s2db version then exit")
	sendRedisCmd     = flag.String("cmd", "", "send redis command to the address specified by '-l' then exit")
	dumpReceiverDir  = flag.String("dump", "", "dump database")
	logRuntimeConfig = flag.String("log.runtime", "100,8,28,log/runtime.log", "[log] runtime log config")
	logSlowConfig    = flag.String("log.slow", "100,8,7,log/slow.log", "[log] slow commands log config")
	logDBConfig      = flag.String("log.db", "100,16,28,log/db.log", "[log] pebble log config")
	logWorkerConfig  = flag.String("log.worker", "100,8,28,log/worker.log", "[log] worker log config")
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	rand.Seed(future.UnixNano())
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Println("s2db", server.Version)
		return
	}

	server.InitLogger(*logRuntimeConfig, *logSlowConfig, *logDBConfig, *logWorkerConfig)

	if *sendRedisCmd != "" {
		cfg, err := resp.ParseConnString(*listenAddr)
		if err != nil {
			log.Errorf("-cmd: invalid address: %v", err)
			os.Exit(-1)
		}
		v, err := cfg.GetClient().Do(context.TODO(), resp.SplitCmdLine(*sendRedisCmd)...).Result()
		if err != nil {
			log.Errorf("-cmd: %v", err)
			os.Exit(-1)
		}
		log.Info(v)
		return
	}

	if *dumpReceiverDir != "" {
		if err := server.StartDumpReceiver(*listenAddr, *dumpReceiverDir); err != nil {
			log.Errorf("dump: %v", err)
			os.Exit(-1)
		}
		return
	}

	server.PrintAllFlags()

	s, err := server.Open(*dataDir, *channel)
	if err != nil {
		log.Errorf("open: %v", err)
		os.Exit(-1)
	}

	if *writeConfig {
		log.Infof("finished config init")
		return
	}

	s.ReadOnly = *readOnly

	log.Error(s.Serve(*listenAddr))
	time.Sleep(time.Second)
}
