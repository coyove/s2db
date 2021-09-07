package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
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

	masterAddr        = flag.String("master", "", "connect to master server, form: master_name@ip:port")
	masterPassword    = flag.String("mp", "", "")
	listenAddr        = flag.String("l", ":6379", "listen address")
	pprofListenAddr   = flag.String("pprof", ":16379", "pprof listen address")
	dataDir           = flag.String("d", "test", "data directory")
	showLogTail       = flag.String("logtail", "", "")
	dumpOverWire      = flag.String("dow", "", "")
	checkDumpOverWire = flag.String("check-dow", "", "")
	showVersion       = flag.Bool("v", false, "print s2db version")
	readOnly          = flag.Bool("ro", false, "start server as read-only")
	masterMode        = flag.Bool("M", false, "tag server as master, so it knows its role when losing connections to slaves")
	calcShard         = flag.String("calc-shard", "", "simple utility to calc the shard number of the given value")
	benchmark         = flag.String("bench", "", "")
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
		Filename:   "x_s2db.log",
		MaxSize:    100, // megabytes
		MaxBackups: 16,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
	}))

	if *dumpOverWire != "" {
		parts := strings.Split(*dumpOverWire, "->")
		if len(parts) != 3 {
			log.Panic("dump over wire format: server_address->shard->local_path")
		}
		shard, _ := strconv.Atoi(parts[1])
		requestDumpShardOverWire(parts[0], parts[2], shard)
		return
	}

	if *checkDumpOverWire != "" {
		checkDumpWireFile(*checkDumpOverWire)
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
	opened := make(chan bool)
	go func() {
		select {
		case <-opened:
		case <-time.After(time.Second * 30):
			log.Panic("failed to open database, locked by others?")
		}
		log.Info("serving pprof at ", *pprofListenAddr)
		log.Error("pprof: ", http.ListenAndServe(*pprofListenAddr, nil))
	}()

	s, err := Open(*dataDir, opened)
	if err != nil {
		log.Panic(err)
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

func requestDumpShardOverWire(remote, output string, shard int) {
	log.Infof("request dumping shard #%d from %s to %s", shard, remote, output)

	of, err := os.Create(output)
	if err != nil {
		log.Panic("output: ", err)
	}
	defer of.Close()

	conn, err := net.Dial("tcp", remote)
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte(fmt.Sprintf("DUMPSHARD %d WIRE\r\n", shard)))
	if err != nil {
		log.Panic("write command: ", err)
	}

	buf := make([]byte, 32*1024)
	rd, err := gzip.NewReader(conn)
	if err != nil {
		log.Panic("read gzip header: ", err)
	}
	written := 0
	for {
		nr, er := rd.Read(buf)
		if nr > 0 {
			nw, ew := of.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = fmt.Errorf("write output invalid")
				}
			}
			written += nw
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	if err != nil {
		log.Panic("failed to dump: ", err)
	}
	log.Info("dumping finished")
}

func checkDumpWireFile(path string) {
	log.Info("check dump-over-wire file: ", path)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		log.Error("open: ", err)
		os.Exit(1)
	}
	defer f.Close()
	_, err = f.Seek(-32, 2)
	if err != nil {
		log.Error("seek: ", err)
		os.Exit(1)
	}
	buf := make([]byte, 32)
	n, err := io.ReadFull(f, buf)
	if n != 32 {
		log.Error("read: ", n, " ", err)
		os.Exit(1)
	}

	if buf[30] != '\r' || buf[31] != '\n' {
		log.Errorf("invalid tail: %q", buf)
		os.Exit(1)
	}

	if bytes.HasSuffix(buf, []byte("-HALT\r\n")) {
		log.Errorf("remote reported error, dumping failed")
		os.Exit(2)
	}

	idx := bytes.LastIndexByte(buf, ':')
	if idx == -1 {
		log.Errorf("invalid tail: %q", buf)
		os.Exit(1)
	}

	sz, _ := strconv.ParseInt(string(buf[idx+1:30]), 10, 64)
	log.Info("reported shard size: ", sz)

	fsz, err := f.Seek(-int64(32-idx), 2)
	if err != nil {
		log.Error("seek2: ", err)
		os.Exit(1)
	}

	if fsz != sz {
		log.Errorf("unmatched size: %d and %d", fsz, sz)
		os.Exit(3)
	}

	if err := f.Truncate(fsz); err != nil {
		log.Error("failed to truncate: ", err)
		os.Exit(4)
	}
	log.Info("checking finished")
}
