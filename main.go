package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/server"
	"github.com/coyove/s2db/wire"
	"github.com/coyove/sdss/future"
	log "github.com/sirupsen/logrus"
)

var (
	listenAddr   = flag.String("l", ":6379", "listen address")
	dataDir      = flag.String("d", "test", "data directory")
	readOnly     = flag.Bool("ro", false, "start server as readonly")
	channel      = flag.Int64("ch", -1, "channel 0-"+strconv.Itoa(int(future.Channels)-1))
	showVersion  = flag.Bool("v", false, "print s2db version then exit")
	sendRedisCmd = flag.String("cmd", "", "send redis command to the address specified by '-l' then exit")
	configSet    = func() (f [6]*string) {
		for i := range f {
			f[i] = flag.String("C"+strconv.Itoa(i), "", "update config before serving, form: key=value")
		}
		return f
	}()
	dumpReceiverDir  = flag.String("dump", "", "dump database")
	logRuntimeConfig = flag.String("log.runtime", "100,8,28,log/runtime.log", "[log] runtime log config")
	logSlowConfig    = flag.String("log.slow", "100,8,7,log/slow.log", "[log] slow commands log config")
	logDBConfig      = flag.String("log.db", "100,16,28,log/db.log", "[log] pebble log config")
	logDebug         = flag.Bool("log.debug", false, "[log] debug level output")
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	rand.Seed(future.UnixNano())
}

func main() {
	flag.Parse()
	go s2.OSWatcher()

	if *showVersion {
		fmt.Println("s2db", server.Version)
		return
	}

	server.InitLogger(*logDebug, *logRuntimeConfig, *logSlowConfig, *logDBConfig)

	if *sendRedisCmd != "" {
		cfg, err := wire.ParseConnString(*listenAddr)
		if err != nil {
			log.Errorf("-cmd: invalid address: %v", err)
			os.Exit(-1)
		}
		v, err := cfg.GetClient().Do(context.TODO(), wire.SplitCmdLine(*sendRedisCmd)...).Result()
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

	s.ReadOnly = *readOnly

	for _, cd := range configSet {
		if idx := strings.Index(*cd, "="); idx != -1 {
			key, value := (*cd)[:idx], (*cd)[idx+1:]
			old, _ := s.GetConfig(key)
			log.Infof("update %s from %q to %q", key, old, value)
			if _, err := s.UpdateConfig(key, value, false); err != nil {
				log.Errorf("update config: %v", err)
				os.Exit(-1)
			}
		}
	}

	log.Error(s.Serve(*listenAddr))
	time.Sleep(time.Second)
}
