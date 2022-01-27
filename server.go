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
	rdbCache *s2pkg.LRUCache

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
	Cache            *s2pkg.KeyedLRUCache
	WeakCache        *s2pkg.LRUCache
	Slaves           slaves
	Master           serverInfo
	CompactLock      s2pkg.LockBox

	Survey struct {
		StartAt                 time.Time
		Connections             int64
		SysRead, SysWrite       s2pkg.Survey
		SysWriteDiscards        s2pkg.Survey
		CacheReq, WeakCacheReq  s2pkg.Survey
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
	x.rdbCache = s2pkg.NewLRUCache(1)
	x.rdbCache.OnEvicted = func(k s2pkg.LRUKey, v interface{}) {
		log.Info("rdbCache(", k, ") close: ", v.(*redis.Client).Close())
	}
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
	s.rdbCache.Info(func(k s2pkg.LRUKey, v interface{}, a, b int64) { errs <- v.(*redis.Client).Close() })

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
		cmd         = strings.ToUpper(command.Get(0))
		isRev       = strings.HasPrefix(cmd, "ZREV")
		key         = command.Get(1)
		weak        = parseWeakFlag(command) // weak parsing comes first
		isReadWrite byte
	)
	cmd = strings.TrimSuffix(cmd, "WEAK")

	if cmd == "UNLINK" || cmd == "DEL" || cmd == "QAPPEND" || strings.HasPrefix(cmd, "Z") || strings.HasPrefix(cmd, "GEO") {
		if key == "" || strings.HasPrefix(key, "score.") || strings.HasPrefix(key, "--") || strings.Contains(key, "\r\n") {
			return w.WriteError("invalid name which is either empty, containing '\\r\\n' or starting with 'score.' or '--'")
		}
		// UNLINK can be executed on slaves because it's a maintenance command,
		// but it will introduce unconsistency when slave and master have different compacting time window (where unlinks will happen)
		if cmd == "DEL" || cmd == "ZADD" || cmd == "ZINCRBY" || cmd == "QAPPEND" || strings.HasPrefix(cmd, "ZREM") {
			if s.RedirectWrites != "" {
				start := time.Now()
				v, err := s.getRedis(s.RedirectWrites).Do(context.Background(), command.Args()...).Result()
				s.Survey.Proxy.Incr(int64(time.Since(start).Milliseconds()))
				if err != nil {
					return w.WriteError(err.Error())
				}
				return w.WriteObject(v)
			} else if s.ReadOnly {
				return w.WriteError("server is read-only, master mode is " + strconv.FormatBool(s.MasterMode))
			} else if s.ServerName == "" {
				return w.WriteError("server name not set, writes are omitted")
			}
			isReadWrite = 'w'
		} else {
			isReadWrite = 'r'
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
				log.Info("slowLog(", shardIndex(key), "): ", diff, " ", command)
				s.Survey.SlowLogs.Incr(diffMs)
			}
			if isReadWrite == 'r' {
				s.Survey.SysRead.Incr(diffMs)
			} else if isReadWrite == 'w' {
				s.Survey.SysWrite.Incr(diffMs)
			}
			if isCommand[cmd] {
				x, _ := s.Survey.Command.LoadOrStore(cmd, new(s2pkg.Survey))
				x.(*s2pkg.Survey).Incr(diffMs)
			}
		}
	}(time.Now())

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
		p, err := nj.LoadString(key, s.getScriptEnviron(command.Argv[2:]...))
		if err != nil {
			return w.WriteError(err.Error())
		}
		v, err := p.Run()
		if err != nil {
			return w.WriteError(err.Error())
		}
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
			found, err := s.updateConfig(command.Get(2), command.Get(3), false)
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
	case "INFO":
		switch n := strings.ToLower(key); {
		case (n >= "0" && n <= "9") || (n >= "10" && n <= "31"):
			return w.WriteBulkString(strings.Join(s.ShardInfo(s2pkg.MustParseInt(n)), "\r\n"))
		default:
			return w.WriteBulkString(strings.Join(s.Info(n), "\r\n"))
		}
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
		var c uint64
		if key != "" {
			c, err = s.myLogTail(s2pkg.MustParseInt(key))
		} else {
			c, err = s.myLogTail(-1)
		}
		return w.WriteIntOrError(int64(c), err)
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

	// Special
	switch cmd {
	case "UNLINK":
		if err := s.pick(key).Update(func(tx *bbolt.Tx) error {
			bk, err := tx.CreateBucketIfNotExists([]byte("unlink"))
			if err != nil {
				return err
			}
			return bk.Put([]byte(key), []byte("unlink"))
		}); err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteSimpleString("OK")
	}

	// Client space write commands
	deferred := parseDeferFlag(command)
	switch cmd {
	case "DEL":
		return s.runPreparedTxAndWrite(key, false, parseDel(cmd, key, command), w)
	case "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
		return s.runPreparedTxAndWrite(key, deferred, parseDel(cmd, key, command), w)
	case "ZADD":
		return s.runPreparedTxAndWrite(key, deferred, parseZAdd(cmd, key, command), w)
	case "ZINCRBY":
		return s.runPreparedTxAndWrite(key, deferred, parseZIncrBy(cmd, key, command), w)
	case "QAPPEND":
		return s.runPreparedTxAndWrite(key, deferred, parseQAppend(cmd, key, command), w)
	}

	h := command.HashCode()
	// Client space read commands
	switch cmd {
	case "ZSCORE", "ZMSCORE":
		s, err := s.ZMScore(key, restCommandsToKeys(2, command), weak)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if cmd == "ZSCORE" {
			return w.WriteBulk(s2pkg.FormatFloatBulk(s[0]))
		}
		var data [][]byte
		for _, s := range s {
			data = append(data, s2pkg.FormatFloatBulk(s))
		}
		return w.WriteBulks(data...)
	case "ZMDATA":
		if v := s.getCache(h, weak); v != nil {
			return w.WriteBulks(v.([][]byte)...)
		}
		data, err := s.ZMData(key, restCommandsToKeys(2, command), redisproto.Flags{Command: *command})
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.addWeakCache(h, data, s2pkg.SizeBytes(data))
		return w.WriteBulks(data...)
	case "ZCARD":
		return w.WriteInt(s.ZCard(key, command.Flags(2).MATCH))
	case "ZCOUNT", "ZCOUNTBYLEX":
		if v := s.getCache(h, weak); v != nil {
			return w.WriteInt(int64(v.(int)))
		}
		// ZCOUNT name start end [MATCH X]
		c, err := s.ZCount(cmd == "ZCOUNTBYLEX", key, command.Get(2), command.Get(3), command.Flags(4))
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.addWeakCache(h, c, 1)
		return w.WriteInt(int64(c))
	case "ZRANK", "ZREVRANK":
		var c int
		if v := s.getCache(h, weak); v != nil {
			c = v.(int)
		} else {
			// COMMAND name key COUNT X
			c, err = s.ZRank(isRev, key, command.Get(2), command.Flags(3))
			if err != nil {
				return w.WriteError(err.Error())
			}
			s.addWeakCache(h, c, 1)
		}
		if c == -1 {
			return w.WriteBulk(nil)
		}
		return w.WriteInt(int64(c))
	case "ZRANGE", "ZREVRANGE", "ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
		// COMMAND name start end FLAGS ...
		flags := command.Flags(4)
		if v := s.getCache(h, weak); v != nil {
			return writePairs(v.([]s2pkg.Pair), w, flags)
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
		s.addWeakCache(h, p, s2pkg.SizePairs(p))
		return writePairs(p, w, flags)
	case "GEORADIUS", "GEORADIUS_RO":
		return s.runGeoRadius(w, false, key, h, weak, command)
	case "GEORADIUSBYMEMBER", "GEORADIUSBYMEMBER_RO":
		return s.runGeoRadius(w, true, key, h, weak, command)
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
		keys := []interface{}{}
		for _, p := range p {
			keys = append(keys, p.Member)
			if flags.WITHSCORES {
				keys = append(keys, s2pkg.FormatFloat(p.Score))
			}
		}
		return w.WriteObjects(next, keys)
	case "QLEN":
		return w.WriteIntOrError(s.QLength(key))
	case "QHEAD":
		return w.WriteIntOrError(s.QHead(key))
	case "QINDEX":
		v, err := s.QGet(key, command.Int64(2))
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulk(v)
	case "QSCAN":
		flags := command.Flags(4)
		if c := s.getStaticCache(h); c != nil {
			return writePairs(c.([]s2pkg.Pair), w, flags)
		}
		data, err := s.QScan(key, command.Int64(2), command.Int64(3), flags)
		if err != nil {
			return w.WriteError(err.Error())
		}
		return writePairs(data, w, flags)
	default:
		return w.WriteError("unknown command: " + cmd)
	}
}
