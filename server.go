package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/ranges"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

const ShardLogNum = 32

type Server struct {
	ln           net.Listener
	lnLocal      net.Listener
	lnWebConsole *s2pkg.LocalListener
	rdbCache     *s2pkg.LRUCache

	LocalRedis *redis.Client
	Slave      endpoint
	PullMaster endpoint
	Master     endpoint
	Pullers    sync.Map
	DBPath     string
	DBOptions  *pebble.Options

	ServerConfig
	ReadOnly         bool
	Closed           bool // server close flag
	SelfManager      *bas.Program
	evalLock         sync.RWMutex
	switchMasterLock sync.RWMutex
	dieLock          sync.Mutex
	dumpWireLock     s2pkg.Locker

	rangeDeleteWatcher s2pkg.Survey

	Survey struct {
		StartAt            time.Time
		Connections        int64
		SysRead            s2pkg.Survey          ``
		SysReadP99Micro    s2pkg.P99SurveyMinute ``
		SysReadRTT         s2pkg.Survey          ``
		SysReadRTTP99Micro s2pkg.P99SurveyMinute ``
		SysWrite           s2pkg.Survey          ``
		SysWriteDiscards   s2pkg.Survey          ``
		CacheReq           s2pkg.Survey          `metrics:"qps"`
		CacheSize          s2pkg.Survey          `metrics:"mean"`
		CacheHit           s2pkg.Survey          `metrics:"qps"`
		BatchSize          s2pkg.Survey          ``
		BatchLat           s2pkg.Survey          ``
		BatchSizeSv        s2pkg.Survey          `metrics:"mean"`
		BatchLatSv         s2pkg.Survey          ``
		DBBatchSize        s2pkg.Survey          ``
		SlowLogs           s2pkg.Survey          ``
		RequestLogs        s2pkg.Survey          ``
		Sync               s2pkg.Survey          ``
		FirstRunSleep      s2pkg.Survey          ``
		LogCompaction      s2pkg.Survey          `metrics:"qps"`
		DSLT               s2pkg.Survey          ``
		DSLTFull           s2pkg.Survey          `metrics:"qps"`
		CacheAddConflict   s2pkg.Survey          `metrics:"qps"`
		TCPWriteError      s2pkg.Survey          `metrics:"qps"`
		TCPWriteTimeout    s2pkg.Survey          `metrics:"qps"`
		Command            sync.Map
		ReverseProxy       sync.Map
	}

	DB *pebble.DB

	shards [ShardLogNum]struct {
		Cache             *s2pkg.LRUCache
		runLogLock        s2pkg.Locker
		syncWaiter        *s2pkg.BuoySignal
		batchTx           chan *batchTask
		batchCloseSignal  chan bool
		pusherTrigger     chan bool
		pusherCloseSignal chan bool
	}
}

func Open(dbPath string) (x *Server, err error) {
	if err := os.MkdirAll(dbPath, 0777); err != nil {
		return nil, err
	}

	x = &Server{
		DBPath: dbPath,
		DBOptions: (&pebble.Options{
			Logger:       dbLogger,
			Cache:        pebble.NewCache(int64(*pebbleCacheSize) << 20),
			MemTableSize: *pebbleMemtableSize << 20,
			MaxOpenFiles: *pebbleMaxOpenFiles,
		}).EnsureDefaults(),
	}
	x.DBOptions.FS = &extdb.VFS{
		FS:       x.DBOptions.FS,
		DumpVDir: filepath.Join(os.TempDir(), strconv.FormatUint(clock.Id(), 16)),
	}
	x.DBOptions.EventListener = x.createDBListener()
	start := time.Now()
	x.DB, err = pebble.Open(dbPath, x.DBOptions)
	if err != nil {
		return nil, err
	}
	if err := x.loadConfig(); err != nil {
		return nil, err
	}
	log.Infof("open data: %s in %v", dbPath, time.Since(start))

	b := x.DB.NewBatch()
	defer b.Close()

	// Load shards
	for i := range x.shards {
		if err := x.checkLogtail(i); err != nil {
			return nil, err
		}
		d := &x.shards[i]
		d.pusherCloseSignal = make(chan bool)
		d.pusherTrigger = make(chan bool, 1)
		d.batchCloseSignal = make(chan bool)
		d.batchTx = make(chan *batchTask, 1024)
		d.syncWaiter = s2pkg.NewBuoySignal(time.Duration(x.ServerConfig.PingTimeout)*time.Millisecond,
			&x.Survey.Sync)
		if err := b.Set(append(ranges.GetShardLogKey(int16(i)), s2pkg.Uint64ToBytes(0)...), joinMultiBytesEmptyNoSig(), pebble.Sync); err != nil {
			return nil, err
		}
	}
	x.rdbCache = s2pkg.NewLRUCache(4, func(k string, v s2pkg.LRUValue) {
		log.Infof("redis client cache evicted: %s, close=%v", k, v.Value.(*redis.Client).Close())
	})
	return x, b.Commit(pebble.Sync)
}

func (s *Server) Close() (err error) {
	s.dieLock.Lock()

	log.Info("server closing flush")
	if err := s.DB.Flush(); err != nil {
		log.Info("server closing: failed to flush: ", err)
		return err
	}

	log.Info("server closing jobs")
	s.waitSlave()
	s.Closed = true
	s.ReadOnly = true
	s.DBOptions.Cache.Unref()
	errs := make(chan error, 100)
	errs <- s.ln.Close()
	errs <- s.lnLocal.Close()
	errs <- s.lnWebConsole.Close()
	errs <- s.Slave.Close()
	errs <- s.LocalRedis.Close()
	s.rdbCache.Range(func(k string, v s2pkg.LRUValue) bool {
		errs <- v.Value.(*redis.Client).Close()
		return true
	})

	wg := sync.WaitGroup{}
	for i := range s.shards {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db := &s.shards[i]
			db.syncWaiter.Close()
			close(db.batchTx)
			<-db.batchCloseSignal
			close(db.pusherTrigger)
			<-db.pusherCloseSignal
		}(i)
	}
	wg.Wait()

	errs <- s.DB.Close()

	close(errs)
	for e := range errs {
		if e != nil {
			err = errors.CombineErrors(err, e)
		}
	}
	log.Info("server closed, err=", err)
	return err
}

func (s *Server) Serve(addr string) (err error) {
	lc := net.ListenConfig{
		Control: func(network, address string, conn syscall.RawConn) error {
			var operr error
			if *netTCPWbufSize > 0 {
				if err := conn.Control(func(fd uintptr) {
					operr = syscall.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF, *netTCPWbufSize)
				}); err != nil {
					return err
				}
			}
			return operr
		},
	}
	s.ln, err = lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return err
	}
	s.lnLocal, err = net.Listen("unix", filepath.Join(os.TempDir(),
		fmt.Sprintf("_s2db_%d_%d_sock", os.Getpid(), time.Now().Unix())))
	if err != nil {
		return err
	}
	s.lnWebConsole = s2pkg.NewLocalListener()
	s.LocalRedis = redis.NewClient(&redis.Options{Network: "unix", Addr: s.lnLocal.Addr().String(), Password: s.Password})
	s.Survey.StartAt = time.Now()

	log.Infof("listening on: redis=%v, local=%v", s.ln.Addr(), s.lnLocal.Addr())

	for i := range s.shards {
		go s.batchWorker(i)
		go s.logPusher(i)
		go s.logPullerAsync(i, time.Second)
	}
	go s.startCronjobs()
	go s.webConsoleHandler()
	go s.acceptor(s.lnLocal)
	s.acceptor(s.ln)
	return nil
}

func (s *Server) acceptor(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if !s.Closed {
				log.Error("accept: ", err, " current connections: ", s.Survey.Connections)
				continue
			} else {
				return
			}
		}
		go func() {
			c := s2pkg.NewBufioConn(conn, time.Duration(s.TCPWriteTimeout)*time.Millisecond, &s.Survey.Connections)
			switch buf, _ := c.Peek(4); *(*string)(unsafe.Pointer(&buf)) {
			case "GET ", "POST", "HEAD":
				s.lnWebConsole.Feed(c)
			default:
				s.handleConnection(c)
			}
		}()
	}
}

func (s *Server) handleConnection(conn s2pkg.BufioConn) {
	defer conn.Close()
	parser := wire.NewParser(conn)
	writer := wire.NewWriter(conn, log.StandardLogger())
	var ew error
	for auth := false; ; {
		command, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*wire.ProtocolError)
			if ok {
				ew = writer.WriteError(err.Error())
			} else {
				if err != io.EOF {
					log.Info(err, " closed connection to ", conn.RemoteAddr())
				}
				break
			}
		} else {
			if !command.IsLast() {
				writer.EnablePipelineMode()
			}
			if s.Password != "" && !auth {
				if command.StrEqFold(0, "AUTH") && command.Str(1) == s.Password {
					auth = true
					writer.WriteSimpleString("OK")
				} else {
					writer.WriteError(wire.ErrNoAuth.Error())
				}
				writer.Flush()
				continue
			}
			startTime := time.Now()
			cmd := strings.ToUpper(command.Str(0))
			ew = s.runCommand(startTime, cmd, writer, s2pkg.GetRemoteIP(conn.RemoteAddr()), command)()
			if isReadCommand[cmd] {
				diff := time.Since(startTime)
				s.Survey.SysReadRTT.Incr(diff.Milliseconds())
				s.Survey.SysReadRTTP99Micro.Incr(diff.Microseconds())
			}
		}
		if command.IsLast() {
			writer.Flush()
		}
		if ew != nil {
			log.Info("connection closed: ", ew)
			if err, ok := ew.(net.Error); ok && err.Timeout() {
				s.Survey.TCPWriteTimeout.Incr(1)
			}
			s.Survey.TCPWriteError.Incr(1)
			break
		}
	}
}

func (s *Server) runCommand(startTime time.Time, cmd string, w *wire.Writer, src net.IP, K *wire.Command) (
	out func() error,
) {
	type any = interface{}
	isRev := strings.HasPrefix(cmd, "ZREV")
	key := K.Str(1)

	for _, nw := range blacklistIPs {
		if nw.Contains(src) {
			return func() error { return w.WriteError(wire.ErrBlacklistedIP.Error() + src.String()) }
		}
	}

	if s.ReverseProxy != 0 && (isWriteCommand[cmd] || isReadCommand[cmd]) {
		defer s2pkg.Recover(func() { w.WriteError("reverseproxy: fatal log") })

		list := s.getUpstreamList()
		keyHash := s.getUpstreamShard(key)
		idx := sort.Search(len(list), func(i int) bool { return int(list[i].Score) >= keyHash })
		if idx >= len(list) {
			return func() error {
				return w.WriteError(fmt.Sprintf("reverseproxy: can't find upstream for %q (%d)", key, keyHash))
			}
		}
		upip := strings.Split(string(list[idx].Data), ";")
		if isReadCommand[cmd] {
			upip[0] = upip[rand.Intn(len(upip))]
		}

		v, err := s.getRedis(upip[0]).Do(context.Background(), K.ArgsRef()...).Result()
		elapsed := int64(time.Since(startTime).Milliseconds())

		sv, _ := s.Survey.ReverseProxy.LoadOrStore("RP"+strconv.Itoa(idx)+"_"+cmd, new(s2pkg.Survey))
		sv.(*s2pkg.Survey).Incr(elapsed)
		sv, _ = s.Survey.ReverseProxy.LoadOrStore("RP"+cmd, new(s2pkg.Survey))
		sv.(*s2pkg.Survey).Incr(elapsed)

		if err != nil && err != redis.Nil {
			return func() error { return w.WriteError("reverseproxy: " + err.Error()) }
		}
		return func() error { return w.WriteObject(v) }
	}

	if strings.HasSuffix(cmd, "!") {
		cmd = cmd[:len(cmd)-1]
	}

	if isWriteCommand[cmd] {
		if key == "" {
			return func() error { return w.WriteError("invalid empty key name") }
		}
		if strings.Contains(key, "\x00") {
			return func() error { return w.WriteError("invalid key name, null bytes (0x00) found") }
		}
		if err := s.checkWritable(); err != nil {
			return func() error { return w.WriteError(err.Error()) }
		}
	}

	defer func(start time.Time) {
		if r := recover(); r != nil {
			if testFlag {
				fmt.Println(r, string(debug.Stack()))
			}
			out = func() error { return w.WriteError(fmt.Sprintf("fatal error (%d): %v", shardIndex(key), r)) }
		} else {
			diff := time.Since(start)
			if diff > time.Duration(s.SlowLimit)*time.Millisecond && cmd != "PUSHLOGS" {
				slowLogger.Infof("#%d\t% 4.3f\t%s\t%v", shardIndex(key), diff.Seconds(), src, K)
				s.Survey.SlowLogs.Incr(diff.Milliseconds())
			}
			if isReadCommand[cmd] {
				s.Survey.SysRead.Incr(diff.Milliseconds())
				s.Survey.SysReadP99Micro.Incr(diff.Microseconds())
				x, _ := s.Survey.Command.LoadOrStore(cmd, new(s2pkg.Survey))
				x.(*s2pkg.Survey).Incr(diff.Milliseconds())
			}
			if cmd == "REQUESTLOGS" {
				s.Survey.RequestLogs.Incr(diff.Milliseconds())
			}
		}
	}(startTime)

	var p []s2pkg.Pair
	var err error

	// General commands
	switch cmd {
	case "DIE":
		s.Close()
		os.Exit(0)
	case "AUTH": // AUTH command is processed before runComamnd, so always return OK here
		return func() error { return w.WriteSimpleString("OK") }
	case "EVAL", "EVALRO":
		if cmd == "EVALRO" {
			s.evalLock.RLock()
			defer s.evalLock.RUnlock()
		} else {
			s.evalLock.Lock()
			defer s.evalLock.Unlock()
		}
		v := nj.MustRun(nj.LoadString(key, s.getScriptEnviron(K.Argv[2:]...)))
		return func() error { return w.WriteBulkString(v.String()) }
	case "PING":
		if key == "" {
			return func() error { return w.WriteSimpleString("PONG " + s.ServerName + " " + Version) }
		}
		return func() error { return w.WriteSimpleString(key) }
	case "CONFIG":
		switch strings.ToUpper(key) {
		case "GET":
			v, _ := s.GetConfig(K.Str(2))
			return func() error { return w.WriteBulkStrings([]string{K.Str(2), v}) }
		case "SET":
			found, err := s.UpdateConfig(K.Str(2), K.Str(3), false)
			if err != nil {
				return func() error { return w.WriteError(err.Error()) }
			} else if found {
				return func() error { return w.WriteSimpleString("OK") }
			}
			return func() error { return w.WriteError("field not found") }
		case "COPY":
			if err := s.CopyConfig(K.Str(2), K.Str(3)); err != nil {
				return func() error { return w.WriteError(err.Error()) }
			}
			return func() error { return w.WriteSimpleString("OK") }
		default:
			return func() error { return w.WriteBulkStrings(s.listConfigCommand()) }
		}
	case "INFO":
		lkey := strings.ToLower(key)
		if strings.HasPrefix(lkey, "metrics.") {
			var sv *s2pkg.Survey
			if rv := reflect.ValueOf(&s.Survey).Elem().FieldByName(key[8:]); rv.IsValid() {
				sv = rv.Addr().Interface().(*s2pkg.Survey)
			} else {
				x, ok := s.Survey.Command.Load(key[8:])
				if !ok {
					x, ok = s.Survey.ReverseProxy.Load(key[8:])
					if !ok {
						return func() error { return w.WriteError("invalid metrics key: " + key[8:]) }
					}
				}
				sv = x.(*s2pkg.Survey)
			}
			m := sv.Metrics()
			return func() error {
				return w.WriteObjects(m.QPS[0], m.QPS[1], m.QPS[2],
					m.Mean[0], m.Mean[1], m.Mean[2],
					m.Max[0], m.Max[1], m.Max[2])
			}
		}
		if strings.HasPrefix(lkey, "log") {
			index := s2pkg.MustParseInt(key[3:])
			return func() error {
				return w.WriteBulkString(strings.Join(s.ShardLogInfoCommand(index), "\r\n"))
			}
		}
		return func() error { return w.WriteBulkString(strings.Join(s.InfoCommand(lkey), "\r\n")) }
	case "DUMPDB":
		go func(start time.Time) {
			log.Infof("start dumping to %s", key)
			err := s.DB.Checkpoint(key, pebble.WithFlushedWAL())
			log.Infof("dumped to %s in %v: %v", key, time.Since(start), err)
		}(startTime)
		return func() error { return w.WriteSimpleString("STARTED") }
	case "COMPACTDB":
		go s.DB.Compact(nil, []byte{0xff}, true)
		return func() error { return w.WriteSimpleString("STARTED") }
	case "DUMPWIRE":
		go s.DumpWire(key)
		return func() error { return w.WriteSimpleString("STARTED") }
	case "SSDISKSIZE":
		end := K.Str(2)
		if end == "" {
			end = key + "\xff"
		}
		startKey, startKey2, _ := ranges.GetZSetRangeKey(key)
		endKey, endKey2, _ := ranges.GetZSetRangeKey(end)
		startSetKey, _ := ranges.GetSetRangeKey(key)
		endSetKey, _ := ranges.GetSetRangeKey(end)
		startKVKey := ranges.GetKVKey(key)
		endKVKey := ranges.GetKVKey(end)
		zsetKeyScore, _ := s.DB.EstimateDiskUsage(startKey, s2pkg.IncBytesInplace(endKey))
		zsetScoreKey, _ := s.DB.EstimateDiskUsage(startKey2, s2pkg.IncBytesInplace(endKey2))
		setAll, _ := s.DB.EstimateDiskUsage(startSetKey, s2pkg.IncBytesInplace(endSetKey))
		kvAll, _ := s.DB.EstimateDiskUsage(startKVKey, s2pkg.IncBytesInplace(endKVKey))
		return func() error { return w.WriteObjects(zsetKeyScore, zsetScoreKey, setAll, kvAll) }
	case "SLOW.LOG":
		return func() error {
			return slowLogger.Formatter.(*s2pkg.LogFormatter).LogFork(w.Conn.(s2pkg.BufioConn))
		}
	case "DB.LOG":
		return func() error {
			return dbLogger.Formatter.(*s2pkg.LogFormatter).LogFork(w.Conn.(s2pkg.BufioConn))
		}
	case "RUNTIME.LOG":
		return func() error {
			return log.StandardLogger().Formatter.(*s2pkg.LogFormatter).LogFork(w.Conn.(s2pkg.BufioConn))
		}
	case "PUSHLOGS": // <Shard> <LogsBytes>
		s.switchMasterLock.RLock()
		defer s.switchMasterLock.RUnlock()
		if s.MarkMaster == 1 {
			return func() error { return w.WriteError(wire.ErrRejectedByMaster.Error()) }
		}
		shard := s2pkg.MustParseInt(key)
		logs := &s2pkg.Logs{}
		if err := logs.UnmarshalBytes(K.BytesRef(2)); err != nil {
			return func() error { return w.WriteError(err.Error()) }
		}
		names, logtail, err := s.runLog(shard, logs)
		if err != nil {
			return func() error { return w.WriteError(err.Error()) }
		}
		for n := range names {
			s.removeCache(n)
		}
		s.Survey.BatchLatSv.Incr(time.Since(startTime).Milliseconds())
		s.Survey.BatchSizeSv.Incr(int64(len(names)))
		s.ReadOnly = true
		s.Master.RemoteIP = src.String()
		s.Master.LastUpdate = startTime.UnixNano()
		if len(names) > 0 {
			select {
			case s.shards[shard].pusherTrigger <- true:
			default:
			}
		}
		return func() error { return w.WriteInt64(int64(logtail)) }
	case "REQUESTLOGS": // <Shard> <LogStart>
		shard, logtail := s2pkg.MustParseInt(key), uint64(K.Int64(2))
		logs, err := s.respondLog(shard, logtail)
		if err != nil {
			return func() error { return w.WriteError(err.Error()) }
		}
		ip := src.String()
		x, _ := s.Pullers.LoadOrStore(ip, new(endpoint))
		x.(*endpoint).RemoteIP = ip
		x.(*endpoint).Logtails[shard] = logtail
		x.(*endpoint).LastUpdate = startTime.UnixNano()
		return func() error { return w.WriteBulk(logs.MarshalBytes()) }
	case "SWITCH":
		s.switchMasterLock.Lock()
		defer s.switchMasterLock.Unlock()
		if strings.EqualFold(key, "master") {
			if _, err := s.UpdateConfig("markmaster", "1", false); err != nil {
				return func() error { return w.WriteError(err.Error()) }
			}
			s.ReadOnly = false
		} else {
			s.ReadOnly = true
		}
		return func() error { return w.WriteSimpleString("OK") }
	}

	// Write commands
	deferred := parseRunFlag(K)
	switch cmd {
	case "DEL", "ZREM":
		return func() error {
			return s.runPreparedTxWrite(cmd, key, deferred, s.parseDel(cmd, key, K, dd(K)), w)
		}
	case "ZADD":
		return func() error {
			return s.runPreparedTxWrite(cmd, key, deferred, s.parseZAdd(cmd, key, K, dd(K)), w)
		}
	case "ZINCRBY":
		return func() error {
			return s.runPreparedTxWrite(cmd, key, deferred, s.parseZIncrBy(cmd, key, K, dd(K)), w)
		}
	case "SADD", "SREM":
		return func() error {
			return s.runPreparedTxWrite(cmd, key, deferred, s.parseSAddRem(cmd, key, K, dd(K)), w)
		}
	case "SET", "SETNX":
		return func() error {
			return s.runPreparedTxWrite(cmd, key, deferred, s.parseSet(cmd, key, K, dd(K)), w)
		}
	}

	parseWeakFlag(K)

	// Read commands
	switch cmd {
	case "GET":
		out, err := s.readCache(K, func() (any, error) { return s.Get(key) })
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteBulk(out.([]byte)) }
	case "MGET":
		out, err := s.readCache(K, func() (any, error) { return s.MGet(ssRef(K.Argv[1:])...) })
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteBulks(out.([][]byte)) }
	case "ZSCORE":
		out, err := s.readCache(K, func() (any, error) { return s.ZMScore(key, K.StrRef(2)) })
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteBulk(s2pkg.FormatFloatBulk(out.([]float64)[0])) }
	case "ZMSCORE":
		out, err := s.readCache(K, func() (any, error) { return s.ZMScore(key, ssRef(K.Argv[2:])...) })
		if err != nil {
			return w.ReturnError(err)
		}
		var data [][]byte
		for _, s := range out.([]float64) {
			data = append(data, s2pkg.FormatFloatBulk(s))
		}
		return func() error { return w.WriteBulks(data) }
	case "ZDATABM16":
		out, err := s.readCache(K, func() (any, error) {
			return s.ZDataBM16(key, K.Str(2), uint16(K.Int64(3)), uint16(K.Int64(4)))
		})
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteBulks(out.([][]byte)) }
	case "ZDATA":
		out, err := s.readCache(K, func() (any, error) { return s.ZMData(key, K.StrRef(2)) })
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteBulk(out.([][]byte)[0]) }
	case "ZMDATA":
		out, err := s.readCache(K, func() (any, error) { return s.ZMData(key, ssRef(K.Argv[2:])...) })
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteBulks(out.([][]byte)) }
	case "SISMEMBER":
		out, err := s.readCache(K, func() (any, error) { return s.SMIsMember(key, K.StrRef(2))[0], nil })
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteInt64(int64(out.(int))) }
	case "SMISMEMBER":
		out, err := s.readCache(K, func() (any, error) { return s.SMIsMember(key, ssRef(K.Argv[2:])...), nil })
		if err != nil {
			return w.ReturnError(err)
		}
		itfs := make([]interface{}, len(out.([]int)))
		for i := range itfs {
			itfs[i] = out.([]int)[i]
		}
		return func() error { return w.WriteObjectsSlice(itfs) }
	case "SMEMBERS":
		out, err := s.readCache(K, func() (any, error) { return s.SMembers(key, K.Flags(2)), nil })
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteBulks(out.([][]byte)) }
	case "SCARD":
		out, err := s.readCache(K, func() (any, error) { return s.SCard(key), nil })
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteInt64(out.(int64)) }
	case "ZCARD":
		out, err := s.readCache(K, func() (any, error) { return s.ZCard(key), nil })
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteInt64(out.(int64)) }
	case "ZCOUNT", "ZCOUNTBYLEX": // key start end [MATCH X]
		out, err := s.readCache(K, func() (any, error) {
			return s.ZCount(cmd == "ZCOUNTBYLEX", key, K.Str(2), K.Str(3), K.Flags(4))
		})
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteInt64(int64(out.(int))) }
	case "ZRANK", "ZREVRANK": // key member [COUNT X]
		out, err := s.readCache(K, func() (any, error) {
			return s.ZRank(cmd == "ZREVRANK", key, K.Str(2), K.Flags(3).Count)
		})
		if err != nil {
			return w.ReturnError(err)
		}
		if out.(int) == -1 {
			return func() error { return w.WriteBulk(nil) }
		}
		return func() error { return w.WriteInt64(int64(out.(int))) }
	case "ZRANGE", "ZREVRANGE": // key start end FLAGS ...
		flags := K.Flags(4)
		out, err := s.readCache(K, func() (any, error) {
			return s.ZRange(cmd == "ZREVRANGE", key, K.Int(2), K.Int(3), flags)
		})
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteBulks(flags.ConvertPairs(out.([]s2pkg.Pair))) }
	case "ZRANGEBYLEX", "ZREVRANGEBYLEX": // key start end FLAGS ...
		flags := K.Flags(4)
		out, err := s.readCache(K, func() (any, error) {
			v, err := s.ZRangeByLex(cmd == "ZREVRANGEBYLEX", key, K.Str(2), K.Str(3), flags)
			if err == nil && flags.IsSpecial() {
				return v, os.ErrInvalid
			}
			return v, err
		})
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteBulks(flags.ConvertPairs(out.([]s2pkg.Pair))) }
	case "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
		flags := K.Flags(4)
		out, err := s.readCache(K, func() (p any, err error) {
			if len(flags.Union) > 0 {
				p, err = s.ZRangeByScore2D(isRev, append(flags.Union, key), K.Str(2), K.Str(3), flags)
			} else {
				p, err = s.ZRangeByScore(isRev, key, K.Str(2), K.Str(3), flags)
			}
			if err == nil && flags.IsSpecial() {
				return p, os.ErrInvalid
			}
			return p, err
		})
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteBulks(flags.ConvertPairs(out.([]s2pkg.Pair))) }
	case "ZRANGERANGEBYSCORE", "ZREVRANGERANGEBYSCORE": // key start end start2 end2 count2
		flags := K.Flags(7)
		p, err = s.ZRangeRangeByScore(isRev, key, K.Str(2), K.Str(3), K.Str(4), K.Str(5), K.Int(6), flags)
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteObjectsSlice(flags.ConvertNestedPairs(p)) }
	case "ZRI": // N term1 ... termN options
		N := K.Int(1)
		p, err := s.RI(toStrings(K.Argv[2:2+N]), K.Flags(2+N))
		if err != nil {
			return w.ReturnError(err)
		}
		return func() error { return w.WriteBulks((wire.Flags{WithScores: true}).ConvertPairs(p)) }
	case "SCAN":
		flags := K.Flags(2)
		p, next := s.Scan(key, flags)
		return func() error { return w.WriteObjects(next, flags.ConvertPairs(p)) }
	case "SSCAN":
		flags := K.Flags(3)
		p, next := s.SScan(key, K.Str(2), flags)
		return func() error { return w.WriteObjects(next, flags.ConvertPairs(p)) }
	}

	return func() error { return w.WriteError(wire.ErrUnknownCommand.Error()) }
}
