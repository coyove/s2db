package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coyove/common/lru"
	"github.com/coyove/common/sched"
	"github.com/coyove/s2db/redisproto"
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
	ReadOnly         bool
	MasterMode       bool
	MasterAddr       string
	MasterNameAssert string // when pulling logs from master, we use its servername to ensure this is the right source
	MasterPassword   string

	ServerConfig
	configMu sync.Mutex

	ln        net.Listener
	cache     *keyedCache
	weakCache *lru.Cache
	rdb       *redis.Client
	closed    bool
	slaves    slaves
	master    serverInfo
	dieKey    sched.SchedKey

	survey struct {
		startAt                 time.Time
		connections             int64
		sysRead, sysWrite       Survey
		sysReadLat, sysWriteLat Survey
		cache, weakCache        Survey
		batchSize, batchLat     Survey
		batchSizeSv, batchLatSv Survey
	}

	db [ShardNum]struct {
		*bbolt.DB
		writeWatermark    int64
		batchTx           chan *batchTask
		batchCloseSignal  chan bool
		pullerCloseSignal chan bool
		compactReplacing  bool
	}
	configDB *bbolt.DB
}

func Open(path string, out chan bool) (*Server, error) {
	var shards [ShardNum]string
	if idx := strings.Index(path, ":"); idx > 0 {
		parts := strings.Split(path, ":")
		for i := range shards {
			shards[i] = filepath.Join(parts[0], "shard"+strconv.Itoa(i))
		}
		for i := 1; i < len(parts); i++ {
			p := strings.Split(parts[i], "=")
			if len(p) == 1 {
				return nil, fmt.Errorf("invalid mapping path: %q", parts[i])
			}
			si := atoi(p[0])
			if si == 0 || si > 31 {
				return nil, fmt.Errorf("invalid mapping index: %v", si)
			}
			shards[si] = filepath.Join(p[1], "shard"+strconv.Itoa(si))
			log.Infof("shard data %d remapped to %q", si, shards[si])
		}
	} else {
		for i := range shards {
			shards[i] = filepath.Join(path, "shard"+strconv.Itoa(i))
		}
	}

	for _, p := range shards {
		os.MkdirAll(filepath.Dir(p), 0777)
	}

	var err error
	x := &Server{}
	x.configDB, err = bbolt.Open(filepath.Join(filepath.Dir(shards[0]), "_config"), 0666, bboltOptions)
	if err != nil {
		return nil, err
	}
	if out != nil {
		out <- true // indicate the opening process is running normally
	}
	for i := range x.db {
		start := time.Now()
		db, err := bbolt.Open(shards[i], 0666, bboltOptions)
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

func (s *Server) Close() error {
	s.closed = true
	errs := make(chan error, 100)
	errs <- s.ln.Close()
	errs <- s.configDB.Close()
	if s.rdb != nil {
		errs <- s.rdb.Close()
	}

	wg := sync.WaitGroup{}
	for i := range s.db {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db := &s.db[i]
			if db.NoFreelistSync {
				db.NoFreelistSync = false
				db.Update(func(tx *bbolt.Tx) error { return nil })
			}
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
	if err := s.loadConfig(); err != nil {
		log.Error(err)
		return err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error(err)
		return err
	}

	s.ln = listener
	s.survey.startAt = time.Now()

	log.Info("listening on ", addr)
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

	for i := range s.db {
		go s.batchWorker(i)
	}
	go s.schedPurge() // TODO: close signal

	for {
		conn, err := listener.Accept()
		if err != nil {
			if !s.closed {
				log.Error("accept: ", err)
			}
			return err
		}
		go s.handleConnection(conn, conn.RemoteAddr())
	}
}

func (s *Server) handleConnection(conn io.ReadWriteCloser, remoteAddr net.Addr) {
	defer func() {
		conn.Close()
		atomic.AddInt64(&s.survey.connections, -1)
	}()
	atomic.AddInt64(&s.survey.connections, 1)
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
				} else {
					writer.WriteError("NOAUTH")
				}
				writer.Flush()
				continue
			}
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
		h           = hashCommands(command)  // then hashCommands
		wm          = s.cache.nextWatermark()
		isReadWrite byte
	)
	cmd = strings.TrimSuffix(cmd, "WEAK")

	if cmd == "UNLINK" || cmd == "DEL" || strings.HasPrefix(cmd, "Z") || strings.HasPrefix(cmd, "GEO") {
		if name == "" || strings.HasPrefix(name, "score.") || strings.HasPrefix(name, "--") || strings.Contains(name, "\r\n") {
			return w.WriteError("invalid name which is either empty, containing '\\r\\n' or starting with 'score.' or '--'")
		}
		// UNLINK can be executed on slaves because it's a maintenance command
		if cmd == "DEL" || cmd == "ZADD" || cmd == "ZINCRBY" || cmd == "QAPPEND" || strings.HasPrefix(cmd, "ZREM") {
			if s.ReadOnly {
				return w.WriteError("server is read-only")
			} else if s.ServerName == "" {
				return w.WriteError("server name not set, writes are omitted")
			}
			s.survey.sysWrite.Incr(1)
			isReadWrite = 'w'
		} else {
			s.survey.sysRead.Incr(1)
			isReadWrite = 'r'
		}
	}

	defer func(start time.Time) {
		if r := recover(); r != nil {
			log.Error(r, string(debug.Stack()))
			w.WriteError("fatal error: " + fmt.Sprint(r))
		} else {
			diff := time.Since(start)
			if diff > time.Duration(s.SlowLimit)*time.Millisecond {
				buf := "[slow log] " + diff.String() + " " + command.String()
				if diff := len(buf) - 512; diff > 0 {
					buf = buf[:512] + " ...[" + strconv.Itoa(diff) + " bytes truncated]..."
				}
				log.Info(buf)
			}
			if isReadWrite == 'r' {
				s.survey.sysReadLat.Incr(int64(diff.Seconds() * 1000))
			} else if isReadWrite == 'w' {
				s.survey.sysWriteLat.Incr(int64(diff.Seconds() * 1000))
			}
		}
	}(time.Now())

	var p []Pair
	var err error

	// General commands
	switch cmd {
	case "DIE":
		sec := time.Now().Second()
		if sec > 58 || strings.EqualFold(name, "now") {
			log.Panic(s.Close())
		}
		s.dieKey.Reschedule(func() { log.Panic(s.Close()) }, time.Duration(59-sec)*time.Second)
		return w.WriteInt(59 - int64(sec))
	case "AUTH":
		return w.WriteSimpleString("OK") // at this stage all AUTH can succeed
	case "PING":
		if name == "" {
			return w.WriteSimpleString("PONG " + s.ServerName + " " + Version)
		}
		if strings.EqualFold(name, "FROM") {
			s.slaves.Update(w.RemoteIP().String(), func(info *serverInfo) {
				info.ListenAddr = command.Get(2)
				info.ServerName = command.Get(3)
				info.Version = command.Get(4)
			})
			return w.WriteSimpleString("PONG " + s.ServerName + " " + Version)
		}
		if name[0] == '=' {
			v, err := atof(name)
			if err != nil {
				return w.WriteError(err.Error())
			}
			return w.WriteSimpleString(ftoa(v))
		}
		return w.WriteSimpleString(name)
	case "CONFIG":
		switch strings.ToUpper(name) {
		case "GET":
			v, _ := s.getConfig(command.Get(2))
			return w.WriteBulkString(v)
		case "SET":
			s.configMu.Lock()
			defer s.configMu.Unlock()
			found, err := s.updateConfig(command.Get(2), command.Get(3))
			if err != nil {
				return w.WriteError(err.Error())
			} else if found {
				return w.WriteSimpleString("OK")
			}
			return w.WriteError("field not found")
		default:
			return w.WriteError("invalid operation")
		}
	case "RESETCACHE":
		weight := s.cache.curWeight + s.weakCache.Weight()
		s.cache.Clear()
		s.weakCache.Clear()
		return w.WriteInt(int64(weight))
	case "INFO":
		switch n := strings.ToLower(name); {
		case (n >= "0" && n <= "9") || (n >= "10" && n <= "31"):
			return w.WriteBulkString(s.shardInfo(atoip(n)))
		case n == "bigkeys":
			if command.Get(3) == "" {
				return w.WriteBulkString(s.bigKeys(atoip(command.Get(2)), -1))
			}
			return w.WriteBulkString(s.bigKeys(atoip(command.Get(2)), atoip(command.Get(3))))
		case n == "cachestat":
			name = command.Get(2)
			length, size, hits := s.cache.KeyInfo(name)
			return w.WriteBulkString(fmt.Sprintf("# cache[%q]\r\nlength:%d\r\nsize:%d\r\nhits:%d\r\n", name, length, size, hits))
		case n == "weakcachestat":
			command.Argv = command.Argv[2:]
			h := hashCommands(command)
			hits, size, _ := s.weakCache.GetEx(h)
			return w.WriteBulkString(fmt.Sprintf("# weakcache%x\r\nsize:%d\r\nhits:%d\r\n", h, size, hits))
		case n == "config":
			return w.WriteBulkString(s.listConfig())
		default:
			return w.WriteBulkString(s.info(n))
		}
	case "DUMPSHARD":
		if !strings.EqualFold(name, "ALL") {
			path := command.Get(2)
			if path == "WIRE" {
				gw := gzip.NewWriter(w.Conn)
				var c int64
				err := s.db[atoip(name)].View(func(tx *bbolt.Tx) error {
					c, err = tx.WriteTo(gw)
					return err
				})
				if err != nil {
					log.Error("dump shard over wire: ", err)
					gw.Write([]byte("-HALT\r\n"))
				} else {
					gw.Write([]byte(":" + strconv.Itoa(int(c)) + "\r\n"))
				}
				gw.Close()
				return io.EOF
			}
			if path == "" {
				path = s.db[atoip(name)].DB.Path() + ".bak"
			}
			return w.WriteIntOrError(s.dumpShard(atoip(name), path))
		}
		var total int64
		for i := range s.db {
			c, err := s.dumpShard(i, s.db[i].Path()+".bak")
			if err != nil {
				return w.WriteError(err.Error())
			}
			total += c
		}
		return w.WriteInt(total)
	case "COMPACTSHARD":
		go s.compactShard(atoip(name))
		return w.WriteSimpleString("STARTED")
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
		s.slaves.Update(w.RemoteIP().String(), func(info *serverInfo) { info.LogTails[atoip(name)] = start - 1 })
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
	case "UNLINKP":
		var names []string
		for i := 0; i < 32; i++ {
			n, err := getPendingUnlinks(s.db[i].DB)
			if err != nil {
				return w.WriteError(err.Error())
			}
			names = append(names, n...)
		}
		return w.WriteBulkStrings(names)
	}

	// Client space write commands
	switch cmd {
	case "DEL", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
		return s.runPreparedTxAndWrite(name, false, parseDel(cmd, name, command), w)
	case "ZADD":
		deferred := parseDeferFlag(command) // ZADD name --DEFER-- arg1 arg2 ...
		return s.runPreparedTxAndWrite(name, deferred, parseZAdd(cmd, name, s.FillPercent, command), w)
	case "ZINCRBY":
		return s.runPreparedTxAndWrite(name, false, parseZIncrBy(cmd, name, command), w)
	case "QAPPEND":
		deferred := parseDeferFlag(command) // QAPPEND name --DEFER-- arg1 arg2 ...
		return s.runPreparedTxAndWrite(name, deferred, parseQAppend(cmd, name, command), w)
	}

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
		s.weakCache.AddWeight(h, &weakCacheItem{Data: data, Time: time.Now().Unix()}, int64(sizeBytes(data)))
		return w.WriteBulks(data...)
	case "ZCARD", "ZCARDMATCH":
		return w.WriteIntOrError(s.ZCard(name, cmd == "ZCARDMATCH"))
	case "ZCOUNT":
		if v := s.getCache(h); v != nil {
			return w.WriteInt(int64(v.(int)))
		}
		if v := s.getWeakCache(h, weak); v != nil {
			return w.WriteInt(int64(v.(int)))
		}
		// ZCOUNT name start end [MATCH X]
		match := command.Get(5) // maybe empty
		c, err := s.ZCount(name, command.Get(2), command.Get(3), match)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.addCache(wm, name, h, c)
		s.weakCache.Add(h, &weakCacheItem{Data: c, Time: time.Now().Unix()})
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
			s.weakCache.Add(h, &weakCacheItem{Data: c, Time: time.Now().Unix()})
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
		limit, match, withData := -1, "", false

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
			}
		}

		switch cmd {
		case "ZRANGE", "ZREVRANGE":
			p, err = s.ZRange(isRev, name, atoip(start), atoip(end), withData)
		case "ZRANGEBYLEX", "ZREVRANGEBYLEX":
			p, err = s.ZRangeByLex(isRev, name, start, end, match, limit, withData)
		case "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
			p, err = s.ZRangeByScore(isRev, name, start, end, match, limit, withData)
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.addCache(wm, name, h, p)
		s.weakCache.AddWeight(h, &weakCacheItem{Data: p, Time: time.Now().Unix()}, int64(sizePairs(p)))
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
		p, next, err := s.scan(name, match, inShard, limit)
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
		return w.WriteIntOrError(s.qLength(name))
	case "QHEAD":
		return w.WriteIntOrError(s.qHead(name))
	case "QINDEX":
		v, err := s.qGet(name, int64(atoip(command.Get(2))))
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
		data, err := s.qScan(name, start, n)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.addCache(wm, name, h, data)
		return w.WriteBulksSlice(data)
	default:
		return w.WriteError("unknown command: " + cmd)
	}
}
