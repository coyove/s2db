package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/redisproto"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/s2pkg/fts"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
	"golang.org/x/time/rate"
)

const (
	ShardNum            = 32
	rejectedByMasterMsg = "rejected by master"
)

var (
	DBOptions = &bbolt.Options{
		FreelistType: bbolt.FreelistMapType,
		Timeout:      time.Second * 10,
	}
	DBReadonlyOptions = &bbolt.Options{
		FreelistType: bbolt.FreelistMapType,
		ReadOnly:     true,
	}
)

type Server struct {
	ln, lnLocal  net.Listener
	lnWebConsole *s2pkg.LocalListener
	rdbCache     *s2pkg.MasterLRU

	LocalRedis *redis.Client
	Slave      endpoint
	MasterIP   string

	ServerConfig
	ReadOnly         bool
	Closed           bool   // server close flag
	ConfigDir        string // location of config database
	SelfManager      *bas.Program
	Cache            *s2pkg.MasterLRU
	WeakCache        *s2pkg.MasterLRU
	CompactLock      s2pkg.LockBox
	EvalLock         sync.RWMutex
	SwitchMasterLock sync.RWMutex
	QAppendLimiter   *rate.Limiter

	Survey struct {
		StartAt                 time.Time
		Connections             int64
		SysRead, SysWrite       s2pkg.Survey
		SysWriteDiscards        s2pkg.Survey
		CacheReq, CacheSize     s2pkg.Survey
		CacheHit, WeakCacheHit  s2pkg.Survey
		BatchSize, BatchLat     s2pkg.Survey
		BatchSizeSv, BatchLatSv s2pkg.Survey
		SlowLogs, Sync          s2pkg.Survey
		Passthrough             s2pkg.Survey
		FirstRunSleep           s2pkg.Survey
		Command                 sync.Map
	}

	db [ShardNum]struct {
		*bbolt.DB
		syncWaiter        *s2pkg.BuoySignal
		batchTx           chan *batchTask
		batchCloseSignal  chan bool
		pusherTrigger     chan bool
		pusherCloseSignal chan bool
		compactLocker     s2pkg.Locker
	}
	ConfigDB *bbolt.DB
}

func Open(configDir string) (x *Server, err error) {
	if err := os.MkdirAll(configDir, 0777); err != nil {
		return nil, err
	}

	x = &Server{}
	x.ConfigDir = configDir
	x.ConfigDB, err = bbolt.Open(filepath.Join(configDir, "_config"), 0666, DBOptions)
	if err != nil {
		return nil, err
	}
	if err := x.loadConfig(); err != nil {
		return nil, err
	}

	// Load shards
	for i := range x.db {
		dir, fn, err := x.GetShardFilename(i)
		if err != nil {
			return nil, err
		}
		shardPath := filepath.Join(dir, fn)
		log.Info("open shard #", i, " of ", shardPath)
		deleteUnusedDataFile(dir, i, fn)
		start := time.Now()
		db, err := bbolt.Open(shardPath, 0666, DBOptions)
		if err != nil {
			return nil, err
		}
		if time.Since(start).Seconds() > 1 {
			log.Info("open slow shard #", i)
		}
		d := &x.db[i]
		d.DB = db
		d.pusherCloseSignal = make(chan bool)
		d.pusherTrigger = make(chan bool, 1)
		d.batchCloseSignal = make(chan bool)
		d.batchTx = make(chan *batchTask, 1024)
		d.syncWaiter = s2pkg.NewBuoySignal(time.Duration(x.ServerConfig.PingTimeout)*time.Millisecond,
			&x.Survey.Sync)
	}

	x.rdbCache = s2pkg.NewMasterLRU(4, func(kv s2pkg.LRUKeyValue) {
		log.Info("rdbCache(", kv.SlaveKey, ") close: ", kv.Value.(*redis.Client).Close())
	})
	fts.InitDict(x.loadDict())
	return x, nil
}

func (s *Server) GetDB(shard int) *bbolt.DB { return s.db[shard].DB }

func (s *Server) Close() error {
	s.Closed = true
	errs := make(chan error, 100)
	errs <- s.ln.Close()
	errs <- s.lnLocal.Close()
	errs <- s.lnWebConsole.Close()
	errs <- s.ConfigDB.Close()
	errs <- s.Slave.Close()
	errs <- s.LocalRedis.Close()
	s.rdbCache.Range(func(kv s2pkg.LRUKeyValue) bool { errs <- kv.Value.(*redis.Client).Close(); return true })

	wg := sync.WaitGroup{}
	for i := range s.db {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db := &s.db[i]
			db.syncWaiter.Close()
			errs <- db.Close()
			close(db.batchTx)
			close(db.pusherTrigger)
			<-db.pusherCloseSignal
			<-db.batchCloseSignal
		}(i)
	}
	wg.Wait()

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

	for i := range s.db {
		go s.batchWorker(i)
		go s.logPusher(i)
	}
	go s.startCronjobs()
	go s.schedCompactionJob() // TODO: close signal
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
		if key == "" || strings.HasPrefix(key, "score.") || strings.HasPrefix(key, "--") || strings.Contains(key, "\r\n") {
			return w.WriteError("invalid key name, which is either empty, containing '\\r\\n' or starting with 'score.' or '--'")
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
			v, _ := s.getConfig(command.Get(2))
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
		case "LOGS":
			return w.WriteBulkStrings(s.listConfigLogs(10))
		default:
			return w.WriteBulkStrings(s.listConfig())
		}
	case "INFOSHARD":
		return w.WriteBulkString(strings.Join(s.ShardInfo(s2pkg.MustParseInt(key)), "\r\n"))
	case "INFO":
		return w.WriteBulkString(strings.Join(s.Info(strings.ToLower(key)), "\r\n"))
	case "TYPE":
		return w.WriteBulk([]byte(s.TypeofKey(key)))
	}

	// Log commands
	switch cmd {
	case "PUSHLOGS": // PUSHLOGS <Shard> <LogHead> <LogPrevSig> <Log1> ...
		s.SwitchMasterLock.RLock()
		defer s.SwitchMasterLock.RUnlock()
		if s.MarkMaster == 1 {
			return w.WriteError(rejectedByMasterMsg)
		}
		shard := s2pkg.MustParseInt(key)
		loghead := uint64(command.Int64(2))
		s.db[shard].compactLocker.Lock(func() { log.Infof("bulkload %d is waiting for compactor/previous bulkload", shard) })
		defer s.db[shard].compactLocker.Unlock()
		names, logtail, err := runLog(loghead, uint32(command.Int64(3)), command.Argv[4:], s.db[shard].DB)
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
			case s.db[shard].pusherTrigger <- true:
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
	case "UNLINK":
		p := s2pkg.Pair{Member: key, Score: float64(start.UnixNano()) / 1e9}
		return w.WriteIntOrError(s.ZAdd(getPendingUnlinksKey(shardIndex(key)), RunNormal, []s2pkg.Pair{p}))
	case "DEL":
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseDel(cmd, key, command, dumpCommand(command)), w)
	case "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseDel(cmd, key, command, dumpCommand(command)), w)
	case "ZADD":
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseZAdd(cmd, key, command, dumpCommand(command)), w)
	case "ZINCRBY":
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseZIncrBy(cmd, key, command, dumpCommand(command)), w)
	case "QAPPEND":
		command.Argv = append(command.Argv, []byte("_NANOTS"), []byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
		if getRemoteIP(remoteAddr).Equal(net.IPv4(172, 31, 128, 202)) {
			waitLimiter(s.QAppendLimiter)
		}
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseQAppend(cmd, key, command, dumpCommand(command)), w)
	}

	h := command.HashCode()
	weak := parseWeakFlag(command)
	// Read commands
	switch cmd {
	case "ZSCORE", "ZMSCORE":
		x, cached := s.getCache(h, weak).([]float64)
		if !cached {
			x, err = s.ZMScore(key, restCommandsToKeys(2, command), command.Flags(-1))
			if err != nil {
				return w.WriteError(err.Error())
			}
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
			x, err = s.ZMData(key, restCommandsToKeys(2, command), command.Flags(-1))
			if err != nil {
				return w.WriteError(err.Error())
			}
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
		return w.WriteIntOrError(s.ZCount(cmd == "ZCOUNTBYLEX", key, command.Get(2), command.Get(3), command.Flags(4)))
	case "ZRANK", "ZREVRANK":
		c, cached := s.getCache(h, weak).(int)
		if !cached {
			// COMMAND name key COUNT X
			c, err = s.ZRank(isRev, key, command.Get(2), command.Flags(3))
			if err != nil {
				return w.WriteError(err.Error())
			}
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
		return w.WriteBulkStrings(redisPairs(p, flags))
	case "GEORADIUS", "GEORADIUS_RO", "GEORADIUSBYMEMBER", "GEORADIUSBYMEMBER_RO":
		byMember := cmd == "GEORADIUSBYMEMBER" || cmd == "GEORADIUSBYMEMBER_RO"
		return s.runGeoRadius(w, byMember, key, h, weak, command)
	case "GEODIST":
		return s.runGeoDist(w, key, command)
	case "GEOPOS":
		return s.runGeoPos(w, key, command)
	case "SCAN":
		flags := command.Flags(2)
		p, next, err := s.Scan(key, flags)
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteObjects(next, redisPairs(p, flags))
	case "QLEN":
		return w.WriteIntOrError(s.QLength(key))
	case "QSCAN":
		flags := command.Flags(4)
		data, cached := s.getCache(h, weak).([]s2pkg.Pair)
		if !cached {
			data, err = s.QScan(key, command.Get(2), command.Int64(3), flags)
			if err != nil {
				return w.WriteError(err.Error())
			}
		}
		return w.WriteBulkStrings(redisPairs(data, flags))
	}

	// Index commands
	switch cmd {
	case "IDXADD": // IDXADD id content key1 ... keyN
		return w.WriteInt(int64(s.IndexAdd(key, command.Get(2), restCommandsToKeys(3, command))))
	case "IDXDEL":
		return w.WriteInt(int64(s.IndexDel(key)))
	case "IDXDOCS":
		return w.WriteObjectsSlice(s.runIndexDocsInfo(restCommandsToKeys(1, command)))
	case "IDXSEARCH": // IDXSEARCH key N text1 ... textN flags
		n := command.Int64(2)
		flags := command.Flags(2 + int(n) + 1)
		command.Argv = command.Argv[:2+int(n)+1]
		return w.WriteBulkStrings(redisPairs(s.IndexSearch(key, restCommandsToKeys(3, command), flags), flags))
	}

	return w.WriteError("unknown command: " + cmd)
}

func (s *Server) checkWritable() error {
	if s.ReadOnly {
		return fmt.Errorf("server is read-only")
	}
	return nil
}
