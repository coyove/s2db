package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coyove/s2db/ranges"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

var (
	Version = ""

	listenAddr   = flag.String("l", ":6379", "listen address")
	dataDir      = flag.String("d", "test", "data directory")
	readOnly     = flag.Bool("ro", false, "start server as read-only, slaves are always read-only")
	showVersion  = flag.Bool("v", false, "print s2db version then exit")
	sendRedisCmd = flag.String("cmd", "", "send redis command to the address specified by '-l' then exit")
	benchmark    = flag.String("bench", "", "")
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
	dsltMaxMembers     = flag.Int("db.dsltlimit", 1024, "[db] limit max members to delete during DSLT")
	deleteKeyQPSLimit  = flag.Int("db.delkeylimit", 1024, "[db] max QPS of deleting keys")
	rangeHardLimit     = flag.Int("db.rangelimit", 65535, "[db] hard limit: max members a single command can return")
	logRuntimeConfig   = flag.String("log.runtime", "100,8,28,log/runtime.log", "[log] runtime log config")
	logSlowConfig      = flag.String("log.slow", "100,8,7,log/slow.log", "[log] slow commands log config")
	logDBConfig        = flag.String("log.db", "100,16,28,log/db.log", "[log] pebble log config")

	testFlag   = false
	slowLogger *log.Logger
	dbLogger   *log.Logger
)

//go:embed scripts/index.html
var webuiHTML string

func main() {
	flag.Parse()
	rand.Seed(time.Now().Unix())
	go s2pkg.OSWatcher()
	ranges.HardLimit = *rangeHardLimit

	if *showVersion {
		fmt.Println("s2db", Version)
		return
	}

	log.SetReportCaller(true)
	s2pkg.SetLogger(log.StandardLogger(), *logRuntimeConfig, false)

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

	slowLogger = log.New()
	s2pkg.SetLogger(slowLogger, *logSlowConfig, true)

	dbLogger = log.New()
	s2pkg.SetLogger(dbLogger, *logDBConfig, true)

	rdb := redis.NewClient(&redis.Options{
		Addr:        *listenAddr,
		DialTimeout: time.Second / 2,
	})

	start := time.Now()
	if *benchmark == "write" {
		wg := sync.WaitGroup{}
		ctx := context.TODO()
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				fmt.Println("client #", i)
				for c := 0; c < 10000; c++ {
					rdb.ZAdd(ctx, "bench", &redis.Z{Member: strconv.Itoa(c + i*100), Score: rand.Float64()*10 - 5})
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
		fmt.Println(time.Since(start).Seconds())
		return
	}

	if *benchmark == "writepipe" {
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
	log.Error(s.Serve(*listenAddr))
	time.Sleep(time.Second)
}
