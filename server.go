package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
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
)

const ShardLogNum = 32

type Server struct {
	ln           net.Listener
	lnLocal      net.Listener
	lnWebConsole *s2pkg.LocalListener
	rdbCache     *s2pkg.MasterLRU

	LocalRedis *redis.Client
	Slave      endpoint
	Master     struct {
		IP      string
		LastAck time.Time
	}
	DBPath    string
	DBOptions *pebble.Options

	ServerConfig
	ReadOnly         bool
	Closed           bool // server close flag
	SelfManager      *bas.Program
	Cache            *s2pkg.MasterLRU
	WeakCache        *s2pkg.MasterLRU
	evalLock         sync.RWMutex
	switchMasterLock sync.RWMutex
	dieLock          sync.Mutex
	dumpWireLock     s2pkg.Locker

	rangeDeleteWatcher s2pkg.Survey

	Survey struct {
		StartAt          time.Time
		Connections      int64
		SysRead          s2pkg.Survey ``
		SysWrite         s2pkg.Survey ``
		SysWriteDiscards s2pkg.Survey ``
		CacheReq         s2pkg.Survey `metrics:"qps"`
		CacheSize        s2pkg.Survey `metrics:"mean"`
		CacheHit         s2pkg.Survey `metrics:"qps"`
		WeakCacheHit     s2pkg.Survey `metrics:"qps"`
		BatchSize        s2pkg.Survey ``
		BatchLat         s2pkg.Survey ``
		BatchSizeSv      s2pkg.Survey `metrics:"mean"`
		BatchLatSv       s2pkg.Survey ``
		DBBatchSize      s2pkg.Survey ``
		SlowLogs         s2pkg.Survey ``
		Sync             s2pkg.Survey ``
		Passthrough      s2pkg.Survey ``
		FirstRunSleep    s2pkg.Survey ``
		LogCompaction    s2pkg.Survey `metrics:"qps"`
		DSLT             s2pkg.Survey ``
		DSLTFull         s2pkg.Survey `metrics:"qps"`
		CacheAddConflict s2pkg.Survey `metrics:"qps"`
		Command          sync.Map
	}

	DB *pebble.DB

	shards [ShardLogNum]struct {
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
	x.rdbCache = s2pkg.NewMasterLRU(4, func(kv s2pkg.LRUKeyValue) {
		log.Info("rdbCache(", kv.SlaveKey, ") close: ", kv.Value.(*redis.Client).Close())
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
	s.rdbCache.Range(func(kv s2pkg.LRUKeyValue) bool { errs <- kv.Value.(*redis.Client).Close(); return true })

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
	s.ln, err = net.Listen("tcp", addr)
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
			c := s2pkg.NewBufioConn(conn, &s.Survey.Connections)
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
				if command.EqualFold(0, "AUTH") && command.Get(1) == s.Password {
					auth = true
					writer.WriteSimpleString("OK")
				} else {
					writer.WriteError(wire.ErrNoAuth.Error())
				}
				writer.Flush()
				continue
			}
			ew = s.runCommand(writer, conn.RemoteAddr(), command)
		}
		if command.IsLast() {
			writer.Flush()
		}
		if ew != nil {
			log.Info("connection closed: ", ew)
			break
		}
	}
}

func (s *Server) runCommand(w *wire.Writer, remoteAddr net.Addr, command *wire.Command) error {
	var (
		cmd   = strings.ToUpper(command.Get(0))
		isRev = strings.HasPrefix(cmd, "ZREV")
		key   = command.Get(1)
		start = time.Now()
	)

	if s.Passthrough != "" && (isWriteCommand[cmd] || isReadCommand[cmd]) {
		defer s2pkg.Recover(func() { w.WriteError("passthrough: failed to relay") })
		v, err := s.getRedis(s.Passthrough).Do(context.Background(), command.Args()...).Result()
		s.Survey.Passthrough.Incr(int64(time.Since(start).Milliseconds()))
		if err != nil && err != redis.Nil {
			return w.WriteError(err.Error())
		}
		return w.WriteObject(v)
	}

	if isWriteCommand[cmd] {
		if key == "" {
			return w.WriteError("invalid empty key name")
		}
		if strings.Contains(key, "\x00") {
			return w.WriteError("invalid key name, null bytes (0x00) found")
		}
		if err := s.checkWritable(); err != nil {
			return w.WriteError(err.Error())
		}
	}

	defer func(start time.Time) {
		if r := recover(); r != nil {
			if testFlag {
				fmt.Println(r, string(debug.Stack()))
			}
			w.WriteError(fmt.Sprintf("fatal error (%d): %v", shardIndex(key), r))
		} else {
			diff := time.Since(start)
			diffMs := diff.Milliseconds()
			if diff > time.Duration(s.SlowLimit)*time.Millisecond && cmd != "PUSHLOGS" {
				slowLogger.Infof("#%d\t% 4.3f\t%s\t%v", shardIndex(key), diff.Seconds(),
					s2pkg.GetRemoteIP(remoteAddr), command)
				s.Survey.SlowLogs.Incr(diffMs)
			}
			if isReadCommand[cmd] {
				if !strings.HasPrefix(cmd, "SCAN") {
					s.Survey.SysRead.Incr(diffMs)
				}
				x, _ := s.Survey.Command.LoadOrStore(cmd, new(s2pkg.Survey))
				x.(*s2pkg.Survey).Incr(diffMs)
			}
		}
	}(start)

	var p []s2pkg.Pair
	var err error

	// General commands
	switch cmd {
	case "DIE":
		s.Close()
		os.Exit(0)
	case "AUTH":
		// AUTH command is processed before runComamnd, always OK here
		return w.WriteSimpleString("OK")
	case "EVAL", "EVALRO":
		if cmd == "EVALRO" {
			s.evalLock.RLock()
			defer s.evalLock.RUnlock()
		} else {
			s.evalLock.Lock()
			defer s.evalLock.Unlock()
		}
		v := nj.MustRun(nj.LoadString(key, s.getScriptEnviron(command.Argv[2:]...)))
		return w.WriteBulkString(v.String())
	case "PING":
		if key == "" {
			return w.WriteSimpleString("PONG " + s.ServerName + " " + Version)
		}
		return w.WriteSimpleString(key)
	case "CONFIG":
		switch strings.ToUpper(key) {
		case "GET":
			v, _ := s.GetConfig(command.Get(2))
			return w.WriteBulkStrings([]string{command.Get(2), v})
		case "SET":
			found, err := s.UpdateConfig(command.Get(2), command.Get(3), false)
			if err != nil {
				return w.WriteError(err.Error())
			} else if found {
				return w.WriteSimpleString("OK")
			}
			return w.WriteError("field not found")
		case "COPY":
			if err := s.CopyConfig(command.Get(2), command.Get(3)); err != nil {
				return w.WriteError(err.Error())
			}
			return w.WriteSimpleString("OK")
		default:
			return w.WriteBulkStrings(s.listConfigCommand())
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
					return w.WriteError("invalid metrics key: " + key[8:])
				}
				sv = x.(*s2pkg.Survey)
			}
			m := sv.Metrics()
			return w.WriteObjects(m.QPS[0], m.QPS[1], m.QPS[2], m.Mean[0], m.Mean[1], m.Mean[2], m.Max[0], m.Max[1], m.Max[2])
		}
		if strings.HasPrefix(lkey, "log") {
			return w.WriteBulkString(strings.Join(s.ShardLogInfoCommand(s2pkg.MustParseInt(key[3:])), "\r\n"))
		}
		return w.WriteBulkString(strings.Join(s.InfoCommand(lkey), "\r\n"))
	case "DUMPDB":
		go func(start time.Time) {
			log.Infof("start dumping to %s", key)
			err := s.DB.Checkpoint(key, pebble.WithFlushedWAL())
			log.Infof("dumped to %s in %v: %v", key, time.Since(start), err)
		}(start)
		return w.WriteSimpleString("STARTED")
	case "DUMPWIRE":
		go s.DumpWire(key)
		return w.WriteSimpleString("STARTED")
	case "SSDISKSIZE":
		end := command.Get(2)
		if end == "" {
			end = key + "\xff"
		}
		startKey, startKey2, _ := ranges.GetZSetRangeKey(key)
		endKey, endKey2, _ := ranges.GetZSetRangeKey(end)
		keyScore, _ := s.DB.EstimateDiskUsage(startKey, s2pkg.IncBytes(endKey))
		scoreKey, _ := s.DB.EstimateDiskUsage(startKey2, s2pkg.IncBytes(endKey2))
		return w.WriteObjects(keyScore, scoreKey)
	case "SLOW.LOG":
		return slowLogger.Formatter.(*s2pkg.LogFormatter).LogFork(w.Conn.(s2pkg.BufioConn))
	case "DB.LOG":
		return dbLogger.Formatter.(*s2pkg.LogFormatter).LogFork(w.Conn.(s2pkg.BufioConn))
	case "RUNTIME.LOG":
		return log.StandardLogger().Formatter.(*s2pkg.LogFormatter).LogFork(w.Conn.(s2pkg.BufioConn))
	case "PUSHLOGS":
		// PUSHLOGS <Shard> <LogsBytes>
		s.switchMasterLock.RLock()
		defer s.switchMasterLock.RUnlock()
		if s.MarkMaster == 1 {
			return w.WriteError(wire.ErrRejectedByMaster.Error())
		}
		shard := s2pkg.MustParseInt(key)
		logs := &s2pkg.Logs{}
		if err := logs.UnmarshalBytes(command.At(2)); err != nil {
			return w.WriteError(err.Error())
		}
		names, logtail, err := s.runLog(shard, logs)
		if err != nil {
			return w.WriteError(err.Error())
		}
		for n := range names {
			s.removeCache(n)
		}
		s.Survey.BatchLatSv.Incr(time.Since(start).Milliseconds())
		s.Survey.BatchSizeSv.Incr(int64(len(names)))
		s.ReadOnly = true
		s.Master.IP = s2pkg.GetRemoteIP(remoteAddr).String()
		s.Master.LastAck = start
		if len(names) > 0 {
			select {
			case s.shards[shard].pusherTrigger <- true:
			default:
			}
		}
		return w.WriteInt(int64(logtail))
	case "SWITCH":
		s.switchMasterLock.Lock()
		defer s.switchMasterLock.Unlock()
		if strings.EqualFold(key, "master") {
			if _, err := s.UpdateConfig("markmaster", "1", false); err != nil {
				return w.WriteError(err.Error())
			}
			s.ReadOnly = false
		} else {
			s.ReadOnly = true
		}
		return w.WriteSimpleString("OK")
	}

	// Write commands
	deferred := parseRunFlag(command)
	switch cmd {
	case "DEL":
		return s.runPreparedTxAndWrite(cmd, key, deferred, s.parseDel(cmd, key, command, dd(command)), w)
	case "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
		return s.runPreparedTxAndWrite(cmd, key, deferred, s.parseDel(cmd, key, command, dd(command)), w)
	case "ZADD":
		return s.runPreparedTxAndWrite(cmd, key, deferred, s.parseZAdd(cmd, key, command, dd(command)), w)
	case "ZINCRBY":
		return s.runPreparedTxAndWrite(cmd, key, deferred, s.parseZIncrBy(cmd, key, command, dd(command)), w)
	}

	cmdHash := s2pkg.HashMultiBytes(command.Argv)
	weak := parseWeakFlag(command)
	mwm := s.Cache.GetWatermark(key)
	// Read commands
	switch cmd {
	case "ZSCORE", "ZMSCORE":
		x, cached := s.getCache(cmdHash, weak).([]float64)
		if !cached {
			x, err = s.ZMScore(key, toStrings(command.Argv[2:]))
			if err != nil {
				return w.WriteError(err.Error())
			}
			s.addCache(key, cmdHash, x, mwm)
		}
		if cmd == "ZSCORE" {
			return w.WriteBulk(s2pkg.FormatFloatBulk(x[0]))
		}
		var data [][]byte
		for _, s := range x {
			data = append(data, s2pkg.FormatFloatBulk(s))
		}
		return w.WriteBulks(data...)
	case "ZDATA", "ZMDATA", "ZDATABM16":
		x, cached := s.getCache(cmdHash, weak).([][]byte)
		if !cached {
			if cmd == "ZDATABM16" {
				x, err = s.ZDataBM16(key, command.Get(2), uint16(command.Int64(3)), uint16(command.Int64(4)))
			} else {
				x, err = s.ZMData(key, toStrings(command.Argv[2:]))
			}
			if err != nil {
				return w.WriteError(err.Error())
			}
			s.addCache(key, cmdHash, x, mwm)
		}
		if cmd == "ZDATA" {
			return w.WriteBulk(x[0])
		}
		return w.WriteBulks(x...)
	case "ZCARD":
		return w.WriteInt(s.ZCard(key))
	case "ZCOUNT", "ZCOUNTBYLEX":
		if v := s.getCache(cmdHash, weak); v != nil {
			return w.WriteInt(int64(v.(int)))
		}
		// ZCOUNT name start end [MATCH X]
		count, err := s.ZCount(cmd == "ZCOUNTBYLEX", key, command.Get(2), command.Get(3), command.Flags(4))
		if err == nil {
			s.addCache(key, cmdHash, count, mwm)
		}
		return w.WriteIntOrError(count, err)
	case "ZRANK", "ZREVRANK":
		c, cached := s.getCache(cmdHash, weak).(int)
		if !cached {
			// COMMAND name key COUNT X
			c, err = s.ZRank(isRev, key, command.Get(2), command.Flags(3).Count)
			if err != nil {
				return w.WriteError(err.Error())
			}
			s.addCache(key, cmdHash, c, mwm)
		}
		if c == -1 {
			return w.WriteBulk(nil)
		}
		return w.WriteInt(int64(c))
	case "ZRANGE", "ZREVRANGE", "ZRANGEBYLEX", "ZREVRANGEBYLEX":
		// COMMAND name start end FLAGS ...
		flags := command.Flags(4)
		if v := s.getCache(cmdHash, weak); v != nil {
			return w.WriteBulkStrings(redisPairs(v.([]s2pkg.Pair), flags))
		}
		start, end := command.Get(2), command.Get(3)
		if end == "" {
			end = start
		}
		switch cmd {
		case "ZRANGE", "ZREVRANGE":
			p, err = s.ZRange(isRev, key, s2pkg.MustParseInt(start), s2pkg.MustParseInt(end), flags)
		case "ZRANGEBYLEX", "ZREVRANGEBYLEX":
			p, err = s.ZRangeByLex(isRev, key, start, end, flags)
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.addCache(key, cmdHash, p, mwm)
		return w.WriteBulkStrings(redisPairs(p, flags))
	case "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
		pf, flags := parseNormFlag(isRev, command)
		if v := s.getCache(cmdHash, weak); v != nil {
			p = v.([]s2pkg.Pair)
		} else {
			start, end := command.Get(2), command.Get(3)
			var wms []int64
			if len(flags.Union) > 0 {
				wms = s.Cache.GetWatermarks(flags.Union)
				p, err = s.ZRangeByScore2D(isRev, append(flags.Union, key), start, end, flags)
				pf = defaultNorm
			} else {
				p, err = s.ZRangeByScore(isRev, key, start, end, flags)
			}
			if err != nil {
				return w.WriteError(err.Error())
			}
			s.addCache(key, cmdHash, p, mwm)
			if len(flags.Union) > 0 {
				s.addCacheMultiKeys(flags.Union, cmdHash, p, wms)
			}
		}
		return w.WriteBulkStrings(redisPairs(pf(p), flags))
	case "SCAN":
		flags := command.Flags(2)
		p, next := s.Scan(key, flags)
		return w.WriteObjects(next, redisPairs(p, flags))
	}

	return w.WriteError(wire.ErrUnknownCommand.Error())
}
