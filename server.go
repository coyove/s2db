package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coyove/common/lru"
	"github.com/coyove/common/sched"
	"github.com/coyove/s2db/redisproto"
	"github.com/coyove/script"
	_ "github.com/coyove/script/lib"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

const ShardNum = 32

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
	ln, lnLocal net.Listener
	rdb         *redis.Client

	ServerConfig
	ReadOnly         bool
	Closed           bool
	MasterMode       bool
	MasterAddr       string
	MasterNameAssert string // when pulling logs from master, we use its servername to ensure this is the right source
	MasterPassword   string
	DataPath         string
	Inspector        *script.Program
	DieKey           sched.SchedKey
	Cache            *keyedCache
	WeakCache        *lru.Cache
	Slaves           slaves
	Master           serverInfo
	CompactLock      locker

	Survey struct {
		StartAt                 time.Time
		Connections             int64
		SysRead, SysWrite       Survey
		SysReadLat, SysWriteLat Survey
		Cache, WeakCache        Survey
		BatchSize, BatchLat     Survey
		BatchSizeSv, BatchLatSv Survey
	}

	db [ShardNum]struct {
		*bbolt.DB
		writeWatermark    int64
		batchTx           chan *batchTask
		batchCloseSignal  chan bool
		pullerCloseSignal chan bool
		compactReplacing  bool
	}
	ConfigDB *bbolt.DB
}

func Open(path string, out chan bool) (*Server, error) {
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
	if out != nil {
		out <- true // indicate the opening process is running normally
	}
	x.DataPath = path
	for i := range x.db {
		start := time.Now()
		var shardPath string
		if remap, _ := x.getConfig("ShardPath" + strconv.Itoa(i)); remap != "" {
			os.MkdirAll(remap, 0777)
			shardPath = filepath.Join(remap, "shard"+strconv.Itoa(i))
			log.Info("remap shard #", i, " to ", shardPath)
		} else {
			shardPath = filepath.Join(path, "shard"+strconv.Itoa(i))
		}
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
	go s.schedPurge() // TODO: close signal

	if v, _ := s.LocalStorage().Get("compact_lock"); v != "" {
		s.runInspectFuncRet("compactonresume", atoip(v))
	}

	runner := func(ln net.Listener) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if !s.Closed {
					log.Error("accept: ", err)
				}
				return
			}
			go s.handleConnection(conn, conn.RemoteAddr())
		}
	}
	go runner(s.ln)
	go runner(s.lnLocal)
	select {}
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
			ew = s.runCommand(writer, command)
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

func (s *Server) runCommand(w *redisproto.Writer, command *redisproto.Command) error {
	var (
		cmd         = strings.ToUpper(command.Get(0))
		isRev       = strings.HasPrefix(cmd, "ZREV")
		name        = command.Get(1)
		weak        = parseWeakFlag(command) // weak parsing comes first
		wm          = s.Cache.nextWatermark()
		isReadWrite byte
	)
	cmd = strings.TrimSuffix(cmd, "WEAK")

	if cmd == "UNLINK" || cmd == "DEL" || cmd == "QAPPEND" || strings.HasPrefix(cmd, "Z") || strings.HasPrefix(cmd, "GEO") {
		if name == "" || strings.HasPrefix(name, "score.") || strings.HasPrefix(name, "--") || strings.Contains(name, "\r\n") {
			return w.WriteError("invalid name which is either empty, containing '\\r\\n' or starting with 'score.' or '--'")
		}
		// UNLINK can be executed on slaves because it's a maintenance command,
		// but it will introduce unconsistency when slave and master have different compacting time window (where unlinks will happen)
		if cmd == "DEL" || cmd == "ZADD" || cmd == "ZINCRBY" || cmd == "QAPPEND" || strings.HasPrefix(cmd, "ZREM") {
			if s.ReadOnly {
				return w.WriteError("server is read-only")
			} else if s.ServerName == "" {
				return w.WriteError("server name not set, writes are omitted")
			}
			s.Survey.SysWrite.Incr(1)
			isReadWrite = 'w'
		} else {
			s.Survey.SysRead.Incr(1)
			isReadWrite = 'r'
		}
	}

	defer func(start time.Time) {
		if r := recover(); r != nil {
			// log.Error(r, string(debug.Stack()))
			w.WriteError(fmt.Sprintf("fatal error (%d): %v", shardIndex(name), r))
		} else {
			diff := time.Since(start)
			if diff > time.Duration(s.SlowLimit)*time.Millisecond {
				buf := "slowLog(" + strconv.Itoa(shardIndex(name)) + "): " + diff.String() + " " + command.String()
				if diff := len(buf) - 256; diff > 0 {
					buf = buf[:256] + " ...[" + strconv.Itoa(diff) + " bytes truncated]..."
				}
				log.Info(buf)
			}
			if isReadWrite == 'r' {
				s.Survey.SysReadLat.Incr(int64(diff.Seconds() * 1000))
			} else if isReadWrite == 'w' {
				s.Survey.SysWriteLat.Incr(int64(diff.Seconds() * 1000))
			}
		}
	}(time.Now())

	var p []Pair
	var err error

	// General commands
	switch cmd {
	case "DIE":
		quit := func() {
			log.Info(s.Close())
			os.Exit(100)
		}
		sec := time.Now().Second()
		if sec > 58 || strings.EqualFold(name, "now") {
			quit()
		}
		s.DieKey.Reschedule(quit, time.Duration(59-sec)*time.Second)
		return w.WriteInt(59 - int64(sec))
	case "AUTH":
		return w.WriteSimpleString("OK") // at this stage all AUTH can succeed
	case "EVAL":
		if strings.HasPrefix(name, "=") {
			v, err := atof(name)
			if err != nil {
				return w.WriteError(err.Error())
			}
			return w.WriteSimpleString(ftoa(v))
		}
		p, err := script.LoadString(name, s.getCompileOptions(command.Argv[2:]...))
		if err != nil {
			return w.WriteError(err.Error())
		}
		v, err := p.Run()
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulkString(v.String())
	case "PING":
		if name == "" {
			return w.WriteSimpleString("PONG " + s.ServerName + " " + Version)
		}
		if strings.EqualFold(name, "FROM") {
			s.Slaves.Update(w.RemoteIP().String(), func(info *serverInfo) {
				info.ListenAddr = command.Get(2)
				info.ServerName = command.Get(3)
				info.Version = command.Get(4)
			})
			return w.WriteSimpleString("PONG " + s.ServerName + " " + Version)
		}
		return w.WriteSimpleString(name)
	case "CONFIG":
		switch strings.ToUpper(name) {
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
		switch n := strings.ToLower(name); {
		case (n >= "0" && n <= "9") || (n >= "10" && n <= "31"):
			return w.WriteBulkString(s.shardInfo(atoip(n)))
		default:
			return w.WriteBulkString(s.Info(n))
		}
	case "DUMPSHARD":
		path := command.Get(2)
		if path == "" {
			path = s.db[atoip(name)].DB.Path() + ".bak"
		}
		return w.WriteIntOrError(s.db[atoip(name)].DB.Dump(path))
	case "COMPACTSHARD":
		if err := s.CompactShardAsync(atoip(name)); err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteSimpleString("STARTED")
	case "REMAPSHARD":
		if err := s.remapShard(atoip(name), command.Get(2)); err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteSimpleString("OK")
	}

	// Log related commands
	switch cmd {
	case "LOGTAIL":
		var c uint64
		if name != "" {
			c, err = s.myLogTail(atoip(name))
		} else {
			c, err = s.myLogTail(-1)
		}
		return w.WriteIntOrError(int64(c), err)
	case "REQUESTLOG":
		start := atoi64(command.Get(2))
		if start == 0 {
			return w.WriteError("request at zero offset")
		}
		logs, err := s.responseLog(atoip(name), start, false)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.Slaves.Update(w.RemoteIP().String(), func(info *serverInfo) { info.LogTails[atoip(name)] = start - 1 })
		return w.WriteBulkStrings(logs)
	}

	// Special
	switch cmd {
	case "UNLINK":
		if err := s.pick(name).Update(func(tx *bbolt.Tx) error {
			bk, err := tx.CreateBucketIfNotExists([]byte("unlink"))
			if err != nil {
				return err
			}
			return bk.Put([]byte(name), []byte("unlink"))
		}); err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteSimpleString("OK")
	}

	// Client space write commands
	deferred := parseDeferFlag(command)
	switch cmd {
	case "DEL":
		return s.runPreparedTxAndWrite(name, false, parseDel(cmd, name, command), w)
	case "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
		return s.runPreparedTxAndWrite(name, deferred, parseDel(cmd, name, command), w)
	case "ZADD":
		return s.runPreparedTxAndWrite(name, deferred, parseZAdd(cmd, name, command), w)
	case "ZINCRBY":
		return s.runPreparedTxAndWrite(name, deferred, parseZIncrBy(cmd, name, command), w)
	case "QAPPEND":
		return s.runPreparedTxAndWrite(name, deferred, parseQAppend(cmd, name, command), w)
	}

	h := hashCommands(command)
	// Client space read commands
	switch cmd {
	case "ZSCORE":
		s, err := s.ZMScore(name, restCommandsToKeys(2, command)...)
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulk(ftob(s[0]))
	case "ZMSCORE":
		s, err := s.ZMScore(name, restCommandsToKeys(2, command)...)
		if err != nil {
			return w.WriteError(err.Error())
		}
		data := [][]byte{}
		for _, s := range s {
			data = append(data, ftob(s))
		}
		return w.WriteBulks(data...)
	case "ZMDATA":
		if v := s.getCache(h); v != nil {
			return w.WriteBulks(v.([][]byte)...)
		}
		if v := s.getWeakCache(h, weak); v != nil {
			return w.WriteBulks(v.([][]byte)...)
		}
		data, err := s.ZMData(name, restCommandsToKeys(2, command)...)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.addCache(wm, name, h, data)
		s.WeakCache.AddWeight(h, &weakCacheItem{Data: data, Time: time.Now().Unix()}, int64(sizeBytes(data)))
		return w.WriteBulks(data...)
	case "ZCARD", "ZCARDMATCH":
		return w.WriteIntOrError(s.ZCard(name, cmd == "ZCARDMATCH"))
	case "ZCOUNT", "ZCOUNTBYLEX":
		if v := s.getCache(h); v != nil {
			return w.WriteInt(int64(v.(int)))
		}
		if v := s.getWeakCache(h, weak); v != nil {
			return w.WriteInt(int64(v.(int)))
		}
		// ZCOUNT name start end [MATCH X]
		match := command.Get(5) // maybe empty
		c, err := s.ZCount(cmd == "ZCOUNTBYLEX", name, command.Get(2), command.Get(3), match)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.addCache(wm, name, h, c)
		s.WeakCache.Add(h, &weakCacheItem{Data: c, Time: time.Now().Unix()})
		return w.WriteInt(int64(c))
	case "ZRANK", "ZREVRANK":
		var c int
		if v := s.getCache(h); v != nil {
			c = v.(int)
		} else if v := s.getWeakCache(h, weak); v != nil {
			c = v.(int)
		} else {
			// ZRANK name key LIMIT X
			limit := atoi(command.Get(4))
			c, err = s.ZRank(isRev, name, command.Get(2), limit)
			if err != nil {
				return w.WriteError(err.Error())
			}
			s.addCache(wm, name, h, c)
			s.WeakCache.Add(h, &weakCacheItem{Data: c, Time: time.Now().Unix()})
		}
		if c == -1 {
			return w.WriteBulk(nil)
		}
		return w.WriteInt(int64(c))
	case "ZRANGE", "ZREVRANGE", "ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
		if v := s.getCache(h); v != nil {
			return writePairs(v.([]Pair), w, command)
		}
		if v := s.getWeakCache(h, weak); v != nil {
			return writePairs(v.([]Pair), w, command)
		}
		start, end := command.Get(2), command.Get(3)
		if end == "" {
			end = start
		}
		limit, match, matchData, withData := -1, "", "", false

		// Parse command flags and remove "LIMIT 0 X" and "MATCH X"
		for i := 3; i < command.ArgCount(); i++ {
			if command.EqualFold(i, "LIMIT") {
				if atoi(command.Get(i+1)) != 0 {
					return w.WriteError("non-zero limit offset not supported")
				}
				limit = atoi(command.Get(i + 2))
				command.Argv = append(command.Argv[:i], command.Argv[i+3:]...)
				i--
			} else if command.EqualFold(i, "WITHDATA") {
				withData = true
			} else if command.EqualFold(i, "MATCH") {
				match = command.Get(i + 1)
				command.Argv = append(command.Argv[:i], command.Argv[i+2:]...)
				i--
			} else if command.EqualFold(i, "MATCHDATA") {
				matchData = command.Get(i + 1)
				command.Argv = append(command.Argv[:i], command.Argv[i+2:]...)
				i--
			}
		}

		switch cmd {
		case "ZRANGE", "ZREVRANGE":
			p, err = s.ZRange(isRev, name, atoip(start), atoip(end), withData)
		case "ZRANGEBYLEX", "ZREVRANGEBYLEX":
			p, err = s.ZRangeByLex(isRev, name, start, end, match, limit, withData)
		case "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
			p, err = s.ZRangeByScore(isRev, name, start, end, match, matchData, limit, withData)
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.addCache(wm, name, h, p)
		s.WeakCache.AddWeight(h, &weakCacheItem{Data: p, Time: time.Now().Unix()}, int64(sizePairs(p)))
		return writePairs(p, w, command)
	case "GEORADIUS", "GEORADIUS_RO":
		return s.runGeoRadius(w, false, name, h, wm, weak, command)
	case "GEORADIUSBYMEMBER", "GEORADIUSBYMEMBER_RO":
		return s.runGeoRadius(w, true, name, h, wm, weak, command)
	case "GEODIST":
		return s.runGeoDist(w, name, command)
	case "GEOPOS":
		return s.runGeoPos(w, name, command)
	case "SCAN":
		limit, match, withScores, inShard := HardLimit, "", false, -1
		for i := 2; i < command.ArgCount(); i++ {
			if command.EqualFold(i, "COUNT") {
				limit = atoip(command.Get(i + 1))
				i++
			} else if command.EqualFold(i, "SHARD") {
				inShard = atoip(command.Get(i + 1))
				i++
			} else if command.EqualFold(i, "MATCH") {
				match = command.Get(i + 1)
				i++
			} else if command.EqualFold(i, "WITHSCORES") {
				withScores = true
			}

		}
		p, next, err := s.Scan(name, match, inShard, limit)
		if err != nil {
			return w.WriteError(err.Error())
		}
		keys := []interface{}{}
		for _, p := range p {
			keys = append(keys, p.Key)
			if withScores {
				keys = append(keys, ftoa(p.Score))
			}
		}
		return w.WriteObjects(next, keys)
	case "QLEN":
		return w.WriteIntOrError(s.QLength(name))
	case "QHEAD":
		return w.WriteIntOrError(s.QHead(name))
	case "QINDEX":
		v, err := s.QGet(name, int64(atoip(command.Get(2))))
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulk(v)
	case "QSCAN":
		if c := s.getCache(h); c != nil {
			return w.WriteBulksSlice(c.([][]byte))
		}
		start := int64(atoip(command.Get(2)))
		n := int64(atoip(command.Get(3)))
		withIndexes := command.EqualFold(4, "WITHINDEXES")
		data, err := s.QScan(name, start, n, withIndexes)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.addCache(wm, name, h, data)
		return w.WriteBulksSlice(data)
	default:
		return w.WriteError("unknown command: " + cmd)
	}
}
