package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
)

const (
	ShardNum = 32
)

var (
	bboltOptions = &bbolt.Options{
		FreelistType: bbolt.FreelistMapType,
	}
	bboltReadonlyOptions = &bbolt.Options{
		FreelistType: bbolt.FreelistMapType,
		ReadOnly:     true,
	}
)

type Server struct {
	ln, lnLocal, lnWeb net.Listener

	rdb      *redis.Client
	rdbCache *s2pkg.MasterLRU

	ServerConfig
	ReadOnly         bool   // server is readonly
	Closed           bool   // server close flag
	MasterMode       bool   // I AM MASTER server
	MasterAddr       string // connect master address
	MasterNameAssert string // when pulling logs, use this name to ensure the right master
	MasterPassword   string // master password
	DataPath         string // location of config database
	RedirectWrites   string
	SelfManager      *bas.Program
	Cache            *s2pkg.MasterLRU
	WeakCache        *s2pkg.MasterLRU
	Slaves           slaves
	Master           serverInfo
	CompactLock      s2pkg.LockBox
	EvalLock         sync.Mutex

	Survey struct {
		StartAt                 time.Time
		Connections             int64
		SysRead, SysWrite       s2pkg.Survey
		SysWriteDiscards        s2pkg.Survey
		CacheReq, CacheSize     s2pkg.Survey
		CacheHit, WeakCacheHit  s2pkg.Survey
		BatchSize, BatchLat     s2pkg.Survey
		BatchSizeSv, BatchLatSv s2pkg.Survey
		Proxy, SlowLogs         s2pkg.Survey
		Command                 sync.Map
	}

	db [ShardNum]struct {
		*bbolt.DB
		batchTx           chan *batchTask
		batchCloseSignal  chan bool
		pullerCloseSignal chan bool
		compactLocker     s2pkg.Locker
	}
	ConfigDB *bbolt.DB
}

func Open(path string, configOpened chan bool) (*Server, error) {
	os.MkdirAll(path, 0777)
	var err error
	x := &Server{}
	x.ConfigDB, err = bbolt.Open(filepath.Join(path, "_config"), 0666, bboltOptions)
	if err != nil {
		return nil, err
	}
	if err := x.loadConfig(); err != nil {
		return nil, err
	}
	if configOpened != nil {
		configOpened <- true // indicate the opening process is running normally
	}
	x.DataPath = path
	for i := range x.db {
		fn, err := x.GetShardFilename(i)
		if err != nil {
			return nil, err
		}
		shardPath := filepath.Join(path, fn)
		log.Info("open shard #", i, " of ", shardPath)
		start := time.Now()
		db, err := bbolt.Open(shardPath, 0666, bboltOptions)
		if err != nil {
			return nil, err
		}
		if time.Since(start).Seconds() > 1 {
			log.Info("open slow shard #", i)
		}
		d := &x.db[i]
		d.DB = db
		d.pullerCloseSignal = make(chan bool)
		d.batchCloseSignal = make(chan bool)
		d.batchTx = make(chan *batchTask, 101)
	}
	x.rdbCache = s2pkg.NewMasterLRU(1, func(kv s2pkg.LRUKeyValue) {
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
	errs <- s.ConfigDB.Close()
	if s.rdb != nil {
		errs <- s.rdb.Close()
	}
	s.rdbCache.Range(func(kv s2pkg.LRUKeyValue) bool { errs <- kv.Value.(*redis.Client).Close(); return true })

	wg := sync.WaitGroup{}
	for i := range s.db {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db := &s.db[i]
			errs <- db.Close()
			if s.rdb != nil {
				<-db.pullerCloseSignal
			}
			close(db.batchTx)
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

func (s *Server) Serve(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error(err)
		return err
	}

	listenerLocal, err := net.Listen("unix", filepath.Join(os.TempDir(),
		fmt.Sprintf("_s2db_%d_%d.sock", os.Getpid(), time.Now().Unix())))
	if err != nil {
		log.Error(err)
		return err
	}

	s.ln = listener
	s.lnLocal = listenerLocal
	s.Survey.StartAt = time.Now()

	log.Info("listening on ", listener.Addr(), " and ", listenerLocal.Addr())
	if s.MasterAddr != "" {
		log.Info("contacting master ", s.MasterAddr)
		s.rdb = redis.NewClient(&redis.Options{
			Addr:     s.MasterAddr,
			Password: s.MasterPassword,
		})
		for i := range s.db {
			go s.requestLogPuller(i)
		}
	}

	s.startCronjobs()
	for i := range s.db {
		go s.batchWorker(i)
	}
	go s.schedCompactionJob() // TODO: close signal

	if v, _ := s.LocalStorage().Get("compact_lock"); v != "" {
		s.runInspectFunc("compactonresume", s2pkg.MustParseInt(v))
	}

	runner := func(ln net.Listener) {
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
				rd := bufio.NewReader(conn)
				if s.lnWeb != nil {
					buf, _ := rd.Peek(4)
					switch *(*string)(unsafe.Pointer(&buf)) {
					case "GET ", "POST", "HEAD":
						req, err := http.ReadRequest(rd)
						if err != nil {
							log.Errorf("httpmux: invalid request: %q, %v", buf, err)
							return
						}
						req.URL, _ = url.Parse(req.RequestURI)
						req.URL.Scheme = "http"
						req.URL.Host = s.lnWeb.Addr().String()
						req.RequestURI = ""
						resp, err := http.DefaultClient.Do(req)
						if err != nil {
							log.Error("httpmux: invalid response: ", err)
							return
						}
						resp.Write(conn)
						conn.Close()
						return
					}
				}
				s.handleConnection(struct {
					io.Reader
					io.WriteCloser
				}{rd, conn}, conn.RemoteAddr())
			}()
		}
	}
	go runner(s.lnLocal)
	runner(s.ln)
	return nil
}

func (s *Server) handleConnection(conn io.ReadWriteCloser, remoteAddr net.Addr) {
	defer func() {
		conn.Close()
		atomic.AddInt64(&s.Survey.Connections, -1)
	}()
	atomic.AddInt64(&s.Survey.Connections, 1)
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
					log.Info(err, " closed connection to ", remoteAddr)
				}
				break
			}
		} else {
			if s.Password != "" && !auth {
				if command.EqualFold(0, "AUTH") && command.Get(1) == s.Password {
					auth = true
					writer.WriteSimpleString("OK")
				} else if command.EqualFold(0, "INFO") {
					goto RUN
				} else {
					writer.WriteError("NOAUTH")
				}
				writer.Flush()
				continue
			}
		RUN:
			ew = s.runCommand(writer, remoteAddr, command)
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
		weak  = parseWeakFlag(command) // weak parsing comes first
		start = time.Now()
	)

	if isWriteCommand[cmd] {
		if key == "" || strings.HasPrefix(key, "score.") || strings.HasPrefix(key, "--") || strings.Contains(key, "\r\n") {
			return w.WriteError("invalid key name, which is either empty, containing '\\r\\n' or starting with 'score.' or '--'")
		}
		if s.RedirectWrites != "" {
			v, err := s.getRedis(s.RedirectWrites).Do(context.Background(), command.Args()...).Result()
			s.Survey.Proxy.Incr(int64(time.Since(start).Milliseconds()))
			if err != nil {
				return w.WriteError(err.Error())
			}
			return w.WriteObject(v)
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
			if diff > time.Duration(s.SlowLimit)*time.Millisecond {
				slowLogger.Infof("#%d\t% 4.3f\t%s\t%v", shardIndex(key), diff.Seconds(), remoteAddr.(*net.TCPAddr).IP, command)
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
		log.Info(s.Close())
		os.Exit(100)
	case "AUTH":
		return w.WriteSimpleString("OK") // at this stage all AUTH can succeed
	case "EVAL":
		s.EvalLock.Lock()
		defer s.EvalLock.Unlock()
		v := nj.MustRun(nj.LoadString(key, s.getScriptEnviron(command.Argv[2:]...)))
		return w.WriteBulkString(v.String())
	case "PING":
		if key == "" {
			return w.WriteSimpleString("PONG " + s.ServerName + " " + Version)
		}
		if strings.EqualFold(key, "FROM") {
			s.Slaves.Update(getRemoteIP(remoteAddr).String(), func(info *serverInfo) {
				info.ListenAddr = command.Get(2)
				info.ServerName = command.Get(3)
				info.Version = command.Get(4)
			})
			return w.WriteSimpleString("PONG " + s.ServerName + " " + Version)
		}
		return w.WriteSimpleString(key)
	case "CONFIG":
		switch strings.ToUpper(key) {
		case "GET":
			v, _ := s.getConfig(command.Get(2))
			return w.WriteBulkString(v)
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
	case "DUMPSHARD":
		path := command.Get(2)
		if path == "" {
			path = s.db[s2pkg.MustParseInt(key)].DB.Path() + ".bak"
		}
		return w.WriteIntOrError(s.db[s2pkg.MustParseInt(key)].DB.Dump(path, s.DumpSafeMargin*1024*1024))
	case "COMPACTSHARD":
		if err := s.CompactShard(s2pkg.MustParseInt(key), true); err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteSimpleString("STARTED")
	}

	// Log related commands
	switch cmd {
	case "LOGTAIL":
		if key != "" {
			return w.WriteIntOrError(s.myLogTail(s2pkg.MustParseInt(key)))
		}
		return w.WriteIntOrError(s.myLogTail(-1))
	case "REQUESTLOG":
		start := s2pkg.ParseUint64(command.Get(2))
		if start == 0 {
			return w.WriteError("request at zero offset")
		}
		logs, err := s.respondLog(s2pkg.MustParseInt(key), start, false)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.Slaves.Update(getRemoteIP(remoteAddr).String(), func(info *serverInfo) {
			info.LogTails[s2pkg.MustParseInt(key)] = start - 1
		})
		return w.WriteBulkStrings(logs)
	}

	// Write commands
	deferred := parseDeferFlag(command)
	switch cmd {
	case "UNLINK":
		p := s2pkg.Pair{Member: key, Score: float64(start.UnixNano()) / 1e6}
		return w.WriteIntOrError(s.ZAdd(getPendingUnlinksKey(shardIndex(key)), false, []s2pkg.Pair{p}))
	case "DEL":
		return s.runPreparedTxAndWrite(cmd, key, false, parseDel(cmd, key, command), w)
	case "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseDel(cmd, key, command), w)
	case "ZADD":
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseZAdd(cmd, key, command), w)
	case "ZINCRBY":
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseZIncrBy(cmd, key, command), w)
	case "QAPPEND":
		return s.runPreparedTxAndWrite(cmd, key, deferred, parseQAppend(cmd, key, command), w)
	}

	h := command.HashCode()
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
	case "ZMDATA":
		if v := s.getCache(h, weak); v != nil {
			return w.WriteBulks(v.([][]byte)...)
		}
		data, err := s.ZMData(key, restCommandsToKeys(2, command), command.Flags(-1))
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulks(data...)
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
		p, next, err := s.Scan(key, flags.MATCH, flags.SHARD, flags.COUNT)
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
	case "IDXSEARCH":
		flags := command.Flags(3)
		return w.WriteBulkStrings(redisPairs(s.IndexSearch(key,
			strings.Split(command.Get(2), flags.SEPARATOR), flags), flags))
	}

	return w.WriteError("unknown command: " + cmd)
}

func (s *Server) checkWritable() error {
	if s.ReadOnly {
		return fmt.Errorf("server is read-only, master mode is " + strconv.FormatBool(s.MasterMode))
	} else if s.ServerName == "" {
		return fmt.Errorf("server name not set, writes are omitted")
	}
	return nil
}
