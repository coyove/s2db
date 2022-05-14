package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/redisproto"
	"github.com/coyove/s2db/s2pkg"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

const ShardLogNum = 32
const rejectedByMasterMsg = "rejected by master"

type Server struct {
	ln           net.Listener
	lnLocal      net.Listener
	lnWebConsole *s2pkg.LocalListener
	rdbCache     *s2pkg.MasterLRU

	LocalRedis *redis.Client
	Slave      endpoint
	MasterIP   string
	DBPath     string
	DBOptions  *pebble.Options

	ServerConfig
	ReadOnly         bool
	Closed           bool // server close flag
	SelfManager      *bas.Program
	Cache            *s2pkg.MasterLRU
	WeakCache        *s2pkg.MasterLRU
	EvalLock         sync.RWMutex
	SwitchMasterLock sync.RWMutex

	Survey struct {
		StartAt          time.Time
		Connections      int64
		SysRead          s2pkg.Survey ``
		SysWrite         s2pkg.Survey ``
		SysWriteDiscards s2pkg.Survey ``
		CacheReq         s2pkg.Survey `metrics:"qps"`
		CacheSize        s2pkg.Survey ``
		CacheHit         s2pkg.Survey `metrics:"qps"`
		WeakCacheHit     s2pkg.Survey `metrics:"qps"`
		BatchSize        s2pkg.Survey ``
		BatchLat         s2pkg.Survey ``
		BatchSizeSv      s2pkg.Survey ``
		BatchLatSv       s2pkg.Survey ``
		DBBatchSize      s2pkg.Survey ``
		SlowLogs         s2pkg.Survey ``
		Sync             s2pkg.Survey ``
		Passthrough      s2pkg.Survey ``
		FirstRunSleep    s2pkg.Survey ``
		LogCompaction    s2pkg.Survey `metrics:"qps"`
		Command          sync.Map
	}

	db *pebble.DB

	shards [ShardLogNum]struct {
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
			Cache:        pebble.NewCache(int64(getEnvOptInt("S2DB_PDB_CACHE", 1024<<20))),
			MemTableSize: getEnvOptInt("S2DB_PDB_MEMTABLESIZE", 32<<20),
		}).EnsureDefaults(),
	}
	x.DBOptions.FS = s2pkg.NoLinkFS{FS: x.DBOptions.FS}
	x.db, err = pebble.Open(dbPath, x.DBOptions)
	if err != nil {
		return nil, err
	}
	if err := x.loadConfig(); err != nil {
		return nil, err
	}

	b := x.db.NewBatch()
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
		if err := b.Set(append(getShardLogKey(int16(i)), s2pkg.Uint64ToBytes(0)...), joinCommandEmpty(), pebble.Sync); err != nil {
			return nil, err
		}
	}
	x.rdbCache = s2pkg.NewMasterLRU(4, func(kv s2pkg.LRUKeyValue) {
		log.Info("rdbCache(", kv.SlaveKey, ") close: ", kv.Value.(*redis.Client).Close())
	})
	return x, b.Commit(pebble.Sync)
}

func (s *Server) Close() error {
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
			close(db.pusherTrigger)
			<-db.pusherCloseSignal
			<-db.batchCloseSignal
		}(i)
	}
	wg.Wait()

	errs <- s.db.Close()

	p := bytes.Buffer{}
	close(errs)
	for err := range errs {
		if err != nil {
			p.WriteString(err.Error())
			p.WriteString(" ")
		}
	}
	if p.Len() > 0 {
		return fmt.Errorf(p.String())
	}

	log.Info("server closed")
	return nil
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
	if v, _ := s.LocalStorage().Get("compact_lock"); v != "" {
		s.runScriptFunc("compactnotfinished", s2pkg.MustParseInt(v))
	}

	for i := range s.shards {
		go s.batchWorker(i)
		go s.logPusher(i)
	}
	go s.startCronjobs()
	// go s.schedCompactionJob() // TODO: close signal
	go s.webConsoleServer()
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
	parser := redisproto.NewParser(conn)
	writer := redisproto.NewWriter(conn, log.StandardLogger())
	var ew error
	for auth := false; ; {
		command, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*redisproto.ProtocolError)
			if ok {
				ew = writer.WriteError(err.Error())
			} else {
				if err != io.EOF {
					log.Info(err, " closed connection to ", conn.RemoteAddr())
				}
				break
			}
		} else {
			if s.Password != "" && !auth {
				if command.EqualFold(0, "AUTH") && command.Get(1) == s.Password {
					auth = true
					writer.WriteSimpleString("OK")
				} else {
					writer.WriteError("NOAUTH")
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

func (s *Server) runCommand(w *redisproto.Writer, remoteAddr net.Addr, command *redisproto.Command) error {
	var (
		cmd   = strings.TrimSuffix(strings.ToUpper(command.Get(0)), "WEAK")
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
		if key == "" || strings.Contains(key, "\x00") {
			return w.WriteError("invalid key name, which is either empty or containing null bytes (0x00)")
		}
		if err := s.checkWritable(); err != nil {
			return w.WriteError(err.Error())
		}
	}

	defer func(start time.Time) {
		if r := recover(); r != nil {
			// log.Error(r, string(debug.Stack()))
			w.WriteError(fmt.Sprintf("fatal error (%d): %v", shardIndex(key), r))
		} else {
			diff := time.Since(start)
			diffMs := diff.Milliseconds()
			if diff > time.Duration(s.SlowLimit)*time.Millisecond && cmd != "PUSHLOGS" {
				slowLogger.Infof("#%d\t% 4.3f\t%s\t%v", shardIndex(key), diff.Seconds(), getRemoteIP(remoteAddr), command)
				s.Survey.SlowLogs.Incr(diffMs)
			}
			if isReadCommand[cmd] {
				s.Survey.SysRead.Incr(diffMs)
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
		s.waitSlave()
		log.Info(s.Close())
		os.Exit(0)
	case "AUTH":
		return w.WriteSimpleString("OK") // at this stage all AUTH can succeed
	case "EVAL", "EVALRO":
		if cmd == "EVALRO" {
			s.EvalLock.RLock()
			defer s.EvalLock.RUnlock()
		} else {
			s.EvalLock.Lock()
			defer s.EvalLock.Unlock()
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
		key = strings.ToLower(key)
		if strings.HasPrefix(key, "log") {
			return w.WriteBulkString(strings.Join(s.ShardLogInfoCommand(s2pkg.MustParseInt(key[3:])), "\r\n"))
		}
		return w.WriteBulkString(strings.Join(s.InfoCommand(key), "\r\n"))
	case "DUMP":
		go func() {
			start := time.Now()
			log.Infof("dump %s in %v: %v", key, time.Since(start),
				s.db.Checkpoint(key, pebble.WithFlushedWAL()))
		}()
		return w.WriteSimpleString("STARTED")
	}

	// Log commands
	switch cmd {
	case "PUSHLOGS": // PUSHLOGS <Shard> <LogsBytes>
		s.SwitchMasterLock.RLock()
		defer s.SwitchMasterLock.RUnlock()
		if s.MarkMaster == 1 {
			return w.WriteError(rejectedByMasterMsg)
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
		s.MasterIP = getRemoteIP(remoteAddr).String()
		if len(names) > 0 {
			select {
			case s.shards[shard].pusherTrigger <- true:
			default:
			}
		}
		return w.WriteInt(int64(logtail))
	case "SWITCHMASTER":
		s.SwitchMasterLock.Lock()
		defer s.SwitchMasterLock.Unlock()
		if _, err := s.UpdateConfig("markmaster", "1", false); err != nil {
			return w.WriteError(err.Error())
		}
		s.ReadOnly = false
		return w.WriteSimpleString("OK")
	case "SWITCHREADONLY":
		s.SwitchMasterLock.Lock()
		defer s.SwitchMasterLock.Unlock()
		s.ReadOnly = true
		return w.WriteSimpleString("OK")
	}

	// Write commands
	deferred := parseRunFlag(command)
	switch cmd {
	case "DEL":
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseDel(cmd, key, command, dumpCommand(command)), w)
	case "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseDel(cmd, key, command, dumpCommand(command)), w)
	case "ZADD":
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseZAdd(cmd, key, command, dumpCommand(command)), w)
	case "ZINCRBY":
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseZIncrBy(cmd, key, command, dumpCommand(command)), w)
	}

	h := command.HashCode()
	weak := parseWeakFlag(command)
	mwm := s.Cache.GetMasterWatermark(key)
	// Read commands
	switch cmd {
	case "ZSCORE", "ZMSCORE":
		x, cached := s.getCache(h, weak).([]float64)
		if !cached {
			x, err = s.ZMScore(key, restCommandsToKeys(2, command))
			if err != nil {
				return w.WriteError(err.Error())
			}
			s.addCache(key, command.HashCode(), x, mwm)
		}
		if cmd == "ZSCORE" {
			return w.WriteBulk(s2pkg.FormatFloatBulk(x[0]))
		}
		var data [][]byte
		for _, s := range x {
			data = append(data, s2pkg.FormatFloatBulk(s))
		}
		return w.WriteBulks(data...)
	case "ZDATA", "ZMDATA":
		x, cached := s.getCache(h, weak).([][]byte)
		if !cached {
			x, err = s.ZMData(key, restCommandsToKeys(2, command))
			if err != nil {
				return w.WriteError(err.Error())
			}
			s.addCache(key, command.HashCode(), x, mwm)
		}
		if cmd == "ZDATA" {
			return w.WriteBulk(x[0])
		}
		return w.WriteBulks(x...)
	case "ZCARD":
		return w.WriteInt(s.ZCard(key))
	case "ZCOUNT", "ZCOUNTBYLEX":
		if v := s.getCache(h, weak); v != nil {
			return w.WriteInt(int64(v.(int)))
		}
		// ZCOUNT name start end [MATCH X]
		count, err := s.ZCount(cmd == "ZCOUNTBYLEX", key, command.Get(2), command.Get(3), command.Flags(4))
		if err == nil {
			s.addCache(key, command.HashCode(), count, mwm)
		}
		return w.WriteIntOrError(count, err)
	case "ZRANK", "ZREVRANK":
		c, cached := s.getCache(h, weak).(int)
		if !cached {
			// COMMAND name key COUNT X
			c, err = s.ZRank(isRev, key, command.Get(2), command.Flags(3).COUNT)
			if err != nil {
				return w.WriteError(err.Error())
			}
			s.addCache(key, command.HashCode(), c, mwm)
		}
		if c == -1 {
			return w.WriteBulk(nil)
		}
		return w.WriteInt(int64(c))
	case "ZRANGE", "ZREVRANGE", "ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
		// COMMAND name start end FLAGS ...
		flags := command.Flags(4)
		if v := s.getCache(h, weak); v != nil {
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
		case "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
			p, err = s.ZRangeByScore(isRev, key, start, end, flags)
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.addCache(key, command.HashCode(), p, mwm)
		return w.WriteBulkStrings(redisPairs(p, flags))
	case "SCAN":
		flags := command.Flags(2)
		p, next := s.Scan(key, flags)
		return w.WriteObjects(next, redisPairs(p, flags))
	}

	return w.WriteError("unknown command: " + cmd)
}

func (s *Server) checkWritable() error {
	if s.ReadOnly {
		return fmt.Errorf("server is read-only")
	}
	return nil
}
