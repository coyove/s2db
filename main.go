package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/wire"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

var (
	Version = ""

	listenAddr   = flag.String("l", ":6379", "listen address")
	dataDir      = flag.String("d", "test", "data directory")
	readOnly     = flag.Bool("ro", false, "start server as read-only, slaves are always read-only")
	channel      = flag.Int64("ch", -1, "channel 0-"+strconv.Itoa(int(future.Channels)))
	showVersion  = flag.Bool("v", false, "print s2db version then exit")
	sendRedisCmd = flag.String("cmd", "", "send redis command to the address specified by '-l' then exit")
	configSet    = func() (f [6]*string) {
		for i := range f {
			f[i] = flag.String("C"+strconv.Itoa(i), "", "update config before serving, form: key=value")
		}
		return f
	}()
	dumpReceiverDir    = flag.String("dump", "", "dump database")
	pebbleMemtableSize = flag.Int("pebble.memtablesize", 128, "[pebble] memtable size in megabytes")
	pebbleCacheSize    = flag.Int("pebble.cachesize", 1024, "[pebble] cache size in megabytes")
	pebbleMaxOpenFiles = flag.Int("pebble.maxopenfiles", 1024, "[pebble] max open files")
	ttlEvictLimit      = flag.Int("db.ttlevictlimit", 1024, "[db] TTL eviction hard limit: max elements a single APPEND can expire")
	logRuntimeConfig   = flag.String("log.runtime", "100,8,28,log/runtime.log", "[log] runtime log config")
	logSlowConfig      = flag.String("log.slow", "100,8,7,log/slow.log", "[log] slow commands log config")
	logDBConfig        = flag.String("log.db", "100,16,28,log/db.log", "[log] pebble log config")
	netTCPWbufSize     = flag.Int("tcp.wbufsiz", 0, "[tcp] TCP write buffer size")
	blacklistIPsFlag   = flag.String("ip.blacklist", "", "")

	testFlag     bool
	testDedup    sync.Map
	slowLogger   *log.Logger
	dbLogger     *log.Logger
	blacklistIPs []*net.IPNet
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	rand.Seed(future.UnixNano())
}

func main() {
	flag.Parse()
	go s2.OSWatcher()

	if *showVersion {
		fmt.Println("s2db", Version)
		return
	}

	log.SetReportCaller(true)
	s2.SetLogger(log.StandardLogger(), *logRuntimeConfig, false)

	if *channel < 0 || *channel >= future.Channels {
		errorExit("invalid channel")
		return
	}

	go future.StartWatcher(func(err error) {
		log.Errorf("future NTP watcher: %v", err)
	})

	if *netTCPWbufSize%4096 != 0 {
		errorExit("invalid TCP write buffer size")
		return
	}

	if *influxdb1MetricsEndpoint != "" {
		cli, db, err := getInfluxDB1Client(*influxdb1MetricsEndpoint)
		if err != nil {
			errorExit("failed to craete influxdb1 client: " + err.Error())
		}
		influxdb1Client.Client, influxdb1Client.Database = cli, db
	}

	if *sendRedisCmd != "" {
		cfg, err := wire.ParseConnString(*listenAddr)
		if err != nil {
			errorExit("cmd: invalid address: " + err.Error())
		}
		v, err := cfg.GetClient().Do(context.TODO(), wire.SplitCmdLine(*sendRedisCmd)...).Result()
		if err != nil {
			errorExit("cmd: " + err.Error())
		}
		log.Info(v)
		return
	}

	if *dumpReceiverDir != "" {
		dumpReceiver(*dumpReceiverDir)
		return
	}

	for _, p := range strings.Split(*blacklistIPsFlag, ",") {
		if p != "" {
			_, nw, err := net.ParseCIDR(p)
			if err != nil {
				errorExit(err.Error())
			}
			blacklistIPs = append(blacklistIPs, nw)
		}
	}

	slowLogger = log.New()
	s2.SetLogger(slowLogger, *logSlowConfig, true)

	dbLogger = log.New()
	s2.SetLogger(dbLogger, *logDBConfig, true)

	rdb := redis.NewClient(&redis.Options{
		Addr:        *listenAddr,
		DialTimeout: time.Second / 2,
	})

	if err := rdb.Ping(context.TODO()).Err(); err == nil || strings.Contains(err.Error(), wire.ErrNoAuth.Error()) {
		return
	}

	log.Info("version: ", Version)
	flag.VisitAll(func(f *flag.Flag) {
		if v := f.Value.String(); v != "" && f.Name != "v" {
			log.Info("[flag] ", f.Name, "=", v)
		}
	})

	s, err := Open(*dataDir)
	if err != nil {
		errorExit(err.Error())
	}

	for _, cd := range configSet {
		if idx := strings.Index(*cd, "="); idx != -1 {
			key, value := (*cd)[:idx], (*cd)[idx+1:]
			old, _ := s.GetConfig(key)
			log.Infof("update %s from %q to %q", key, old, value)
			if _, err := s.UpdateConfig(key, value, false); err != nil {
				errorExit(err.Error())
			}
		}
	}

	s.ReadOnly = *readOnly
	s.Channel = *channel
	log.Error(s.Serve(*listenAddr))
	time.Sleep(time.Second)
}

func errorExit(msg string) {
	log.Error(msg)
	os.Exit(1)
}
