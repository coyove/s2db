package main

import (
	"bytes"
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
	"github.com/go-redis/redis/v8"
	"github.com/secmask/go-redisproto"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

const ShardNum = 32

var bboltOptions = &bbolt.Options{
	FreelistType: bbolt.FreelistMapType,
}

type Server struct {
	MasterAddr string
	ServerConfig

	ln        net.Listener
	cache     *Cache
	weakCache *lru.Cache
	rdb       *redis.Client
	closed    bool
	slaves    slaves
	master    serverInfo
	dieKey    sched.SchedKey

	survey struct {
		startAt                    time.Time
		connections                int64
		sysRead, sysWrite          Survey
		sysReadLat, sysWriteLat    Survey
		cache, weakCache           Survey
		addBatchSize, addBatchDrop Survey
	}

	db [ShardNum]struct {
		*bbolt.DB
		readonly          bool
		writeWatermark    int64
		deferAdd          chan *addTask
		deferCloseSignal  chan bool
		pullerCloseSignal chan bool
	}
}

type Pair struct {
	Key   string
	Score float64
	Data  []byte
}

func Open(path string) (*Server, error) {
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

	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}

	x := &Server{}
	for i := range x.db {
		db, err := bbolt.Open(shards[i], 0666, bboltOptions)
		if err != nil {
			return nil, err
		}
		d := &x.db[i]
		d.DB = db
		d.pullerCloseSignal = make(chan bool)
		d.deferCloseSignal = make(chan bool)
		d.deferAdd = make(chan *addTask, 101)
	}
	return x, nil
}

func (s *Server) SetReadOnly(v bool) {
	for i := range s.db {
		s.db[i].readonly = v
	}
}

func (s *Server) ReadOnly() (x [ShardNum]bool) {
	for i := range s.db {
		x[i] = s.db[i].readonly
	}
	return
}

func (s *Server) Close() error {
	s.closed = true
	errs := make(chan error, 100)
	errs <- s.ln.Close()
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
			close(db.deferAdd)
			<-db.deferCloseSignal
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

	if s.MasterAddr != "" {
		s.rdb = redis.NewClient(&redis.Options{
			Addr: s.MasterAddr,
		})
		for i := range s.db {
			go s.requestLogPuller(i)
		}
	}
	for i := range s.db {
		go s.deferAddWorker(i)
	}

	log.Info("listening on ", addr, " master=", s.MasterAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if !s.closed {
				log.Error("Error on accept: ", err)
			}
			return err
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		atomic.AddInt64(&s.survey.connections, -1)
	}()
	atomic.AddInt64(&s.survey.connections, 1)
	parser := redisproto.NewParser(conn)
	writer := redisproto.NewWriter(conn, log.StandardLogger())
	var ew error
	for {
		command, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*redisproto.ProtocolError)
			if ok {
				ew = writer.WriteError(err.Error())
			} else {
				if err != io.EOF {
					log.Println(err, " closed connection to ", conn.RemoteAddr())
				}
				break
			}
		} else {
			ew = s.runCommand(writer, command, false)
		}
		if command.IsLast() {
			writer.Flush()
		}
		if ew != nil {
			log.Println("Connection closed", ew)
			break
		}
	}
}

func (s *Server) runCommand(w *redisproto.Writer, command *redisproto.Command, isBulk bool) error {
	cmd := strings.ToUpper(string(command.Get(0)))
	h := hashCommands(command)
	wm := s.cache.nextWatermark()
	name := string(command.Get(1))
	isReadWrite := '\x00'

	if cmd == "DEL" || strings.HasPrefix(cmd, "Z") || strings.HasPrefix(cmd, "GEO") {
		if name == "" || name == "--" || strings.HasPrefix(name, "score.") || strings.Contains(name, "\r\n") {
			return w.WriteError("invalid name which is either empty, equals to '--', starts with 'score.' or contains '\\r\\n'")
		}
		if cmd == "DEL" || strings.HasPrefix(cmd, "ZADD") || cmd == "ZINCRBY" || strings.HasPrefix(cmd, "ZREM") {
			if !isBulk && s.db[shardIndex(name)%ShardNum].readonly {
				return w.WriteError("readonly")
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
				buf := bytes.NewBufferString("[slow log] " + diff.String())
				for i := 0; i < command.ArgCount(); i++ {
					buf.WriteString(" '")
					buf.Write(command.Get(i))
					buf.WriteString("'")
				}
				if diff := buf.Len() - 512; diff > 0 {
					buf.Truncate(512)
					buf.WriteString(" ...[" + strconv.Itoa(diff) + " bytes truncated]...")
				}
				log.Info(buf.String())
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
	switch cmd {
	case "DIE":
		sec := time.Now().Second()
		if sec > 58 || strings.EqualFold(name, "now") {
			log.Panic(s.Close())
		}
		s.dieKey.Reschedule(func() { log.Panic(s.Close()) }, time.Duration(59-sec)*time.Second)
		return w.WriteInt(59 - int64(sec))
	case "PING":
		if name == "" {
			return w.WriteSimpleString("PONG " + s.ServerName + " " + Version)
		}
		if strings.EqualFold(name, "from") {
			s.slaves.Update(w.RemoteIP().String(), func(info *serverInfo) {
				info.ListenAddr = string(command.Get(2))
				info.ServerName = string(command.Get(3))
				info.Version = string(command.Get(4))
			})
			return w.WriteSimpleString("PONG " + s.ServerName + " " + Version)
		}
		return w.WriteSimpleString(name)
	case "CONFIG":
		switch strings.ToUpper(name) {
		case "GET":
			v, _ := s.getConfig(string(command.Get(2)))
			return w.WriteBulkString(v)
		case "SET":
			s.updateConfig(string(command.Get(2)), string(command.Get(3)))
			fallthrough
		default:
			return w.WriteBulkStrings(s.listConfig())
		}
	case "RESETCACHE":
		weight := s.cache.curWeight + s.weakCache.Weight()
		s.cache.Clear()
		s.weakCache.Clear()
		return w.WriteInt(int64(weight))
	case "INFO":
		switch n := strings.ToLower(name); {
		case n >= "shard0" && n <= "shard9":
			return w.WriteBulkString(s.shardInfo(atoip(name[5:])))
		case n == "bigkeys":
			return w.WriteBulkString(s.bigKeys(atoi(string(command.Get(2)))))
		case n == "cache":
			name = string(command.Get(2))
			length, size, hits := s.cache.KeyInfo(name)
			return w.WriteBulkString(fmt.Sprintf("# cache[%q]\r\nlength:%d\r\nsize:%d\r\nhits:%d\r\n", name, length, size, hits))
		case n == "weakcache":
			command.Argv = command.Argv[2:]
			h := hashCommands(command)
			hits, size, _ := s.weakCache.GetEx(h)
			return w.WriteBulkString(fmt.Sprintf("# weakcache%x\r\nsize:%d\r\nhits:%d\r\n", h, size, hits))
		case n == "slaves":
			data := bytes.NewBufferString("# slaves\r\n")
			for _, p := range s.slaves.Take(time.Minute) {
				data.WriteString(p.Key + ":")
				data.Write(p.Data)
				data.WriteString("\r\n")
			}
			return w.WriteBulkString(data.String())
		default:
			return w.WriteBulkString(s.info())
		}
	case "DUMPSHARD":
		x := &s.db[atoip(name)]
		path := string(command.Get(2))
		if path == "" {
			path = x.DB.Path() + ".bak"
		}
		of, err := os.Create(path)
		if err != nil {
			return w.WriteError(err.Error())
		}
		defer of.Close()
		var c int64
		err = x.DB.View(func(tx *bbolt.Tx) error {
			c, err = tx.WriteTo(of)
			return err
		})
		return w.WriteIntOrError(int64(c), err)
	case "COMPACTSHARD":
		err := s.compactShard(atoip(name))
		if err != nil {
			return w.WriteError(err.Error()) // keep shard readonly until we find the cause
		}
		return w.WriteSimpleString("OK")

		// -----------------------
		//  Log related commands
		// -----------------------
	case "LOGTAIL":
		var c uint64
		if name != "" {
			c, err = s.walProgress(atoip(name))
		} else {
			c, err = s.walProgress(-1)
		}
		return w.WriteIntOrError(int64(c), err)
	case "REQUESTLOG":
		start := atoi64(string(command.Get(2)))
		if start == 0 {
			return w.WriteError("request at zero offset")
		}
		logs, err := s.responseLog(atoip(name), start)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.slaves.Update(w.RemoteIP().String(), func(info *serverInfo) { info.KnownLogTails[atoip(name)] = start - 1 })
		return w.WriteBulkStrings(logs)
	case "PURGELOG":
		c, err := s.purgeLog(atoip(name), atoi64(string(command.Get(2))))
		return w.WriteIntOrError(int64(c), err)

		// -----------------------
		//  Client space write commands
		// -----------------------
	case "DEL": // TODO: multiple keys
		return s.runDel(w, name, command)
	case "ZADD":
		return s.runZAdd(w, name, command)
	case "ZADDBATCH":
		return s.runZAddBatchShard(w, name, command)
	case "ZINCRBY":
		return s.runZIncrBy(w, name, command)
	case "ZREM":
		return s.runZRem(w, name, command)
	case "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
		return s.runZRemRange(w, cmd, name, command)

		// -----------------------
		//  Client space read commands
		// -----------------------
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
	case "ZMDATA", "ZMDATAWEAK":
		if v := s.getCache(h); v != nil {
			return w.WriteBulks(v.([][]byte)...)
		}
		if cmd == "ZMDATAWEAK" {
			if v := s.getWeakCache(h); v != nil {
				return w.WriteBulks(v.([][]byte)...)
			}
		}
		data, err := s.ZMData(name, restCommandsToKeys(2, command)...)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if s.canUpdateCache(name, wm) {
			s.cache.Add(&CacheItem{Key: name, CmdHash: h, Data: data})
		}
		sz := int64(1)
		for _, b := range data {
			sz += int64(len(b))
		}
		s.weakCache.AddWeight(h, &WeakCacheItem{Data: data, Time: time.Now().Unix()}, sz)
		return w.WriteBulks(data...)
	case "ZCARD":
		return w.WriteIntOrError(s.ZCard(name))
	case "ZCOUNT":
		if v := s.getCache(h); v != nil {
			return w.WriteInt(int64(v.(int)))
		}
		if cmd == "ZCOUNTWEAK" {
			if v := s.getWeakCache(h); v != nil {
				return w.WriteInt(int64(v.(int)))
			}
		}
		match := ""
		for i := 4; i < command.ArgCount(); i++ {
			if strings.EqualFold(string(command.Get(i)), "MATCH") {
				match = string(command.Get(i + 1))
				i++
			}
		}
		c, err := s.ZCount(name, string(command.Get(2)), string(command.Get(3)), match)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if s.canUpdateCache(name, wm) {
			s.cache.Add(&CacheItem{Key: name, CmdHash: h, Data: c})
		}
		s.weakCache.Add(h, &WeakCacheItem{Data: c, Time: time.Now().Unix()})
		return w.WriteInt(int64(c))
	case "ZRANK", "ZREVRANK", "ZRANKWEAK", "ZREVRANKWEAK":
		var c int
		if v := s.getCache(h); v != nil {
			c = v.(int)
		} else {
			if strings.HasSuffix(cmd, "WEAK") {
				if v := s.getWeakCache(h); v != nil {
					c = v.(int)
					goto RANK_RES
				}
				cmd = cmd[:len(cmd)-4]
			}
			limit := atoi(string(command.Get(3)))
			if cmd == "ZRANK" {
				c, err = s.ZRank(name, string(command.Get(2)), limit)
			} else {
				c, err = s.ZRevRank(name, string(command.Get(2)), limit)
			}
			if err != nil {
				return w.WriteError(err.Error())
			}
			if s.canUpdateCache(name, wm) {
				s.cache.Add(&CacheItem{Key: name, CmdHash: h, Data: c})
			}
			s.weakCache.Add(h, &WeakCacheItem{Data: c, Time: time.Now().Unix()})
		}
	RANK_RES:
		if c == -1 {
			return w.WriteBulk(nil)
		}
		return w.WriteInt(int64(c))
	case "ZRANGE", "ZREVRANGE", "ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE",
		"ZRANGEWEAK", "ZREVRANGEWEAK", "ZRANGEBYLEXWEAK", "ZREVRANGEBYLEXWEAK", "ZRANGEBYSCOREWEAK", "ZREVRANGEBYSCOREWEAK":
		if v := s.getCache(h); v != nil {
			return writePairs(v.([]Pair), w, command)
		}
		if strings.HasSuffix(cmd, "WEAK") {
			if v := s.getWeakCache(h); v != nil {
				return writePairs(v.([]Pair), w, command)
			}
			cmd = cmd[:len(cmd)-4]
		}
		start, end, limit, match, withData := string(command.Get(2)), string(command.Get(3)), -1, "", false
		for i := 3; i < command.ArgCount(); i++ {
			if strings.EqualFold(string(command.Get(i)), "LIMIT") {
				if atoi(string(command.Get(i+1))) != 0 {
					return w.WriteError("non-zero limit offset not supported")
				}

				limit = atoi(string(command.Get(i + 2)))
				command.Argv = append(command.Argv[:i], command.Argv[i+3:]...)
				i--
			} else if strings.EqualFold(string(command.Get(i)), "WITHDATA") {
				withData = true
			} else if strings.EqualFold(string(command.Get(i)), "MATCH") {
				match = string(command.Get(i + 1))
				i++
			}
		}

		switch cmd {
		case "ZRANGE":
			p, err = s.ZRange(name, atoip(start), atoip(end), withData)
		case "ZREVRANGE":
			p, err = s.ZRevRange(name, atoip(start), atoip(end), withData)
		case "ZRANGEBYLEX", "ZREVRANGEBYLEX":
			if cmd == "ZRANGEBYLEX" {
				p, err = s.ZRangeByLex(name, start, end, match, limit)
			} else {
				p, err = s.ZRevRangeByLex(name, start, end, match, limit)
			}
			if withData {
				if err := s.fillPairsData(name, p); err != nil {
					return w.WriteError(err.Error())
				}
			}
		case "ZRANGEBYSCORE":
			p, err = s.ZRangeByScore(name, start, end, match, limit, withData)
		case "ZREVRANGEBYSCORE":
			p, err = s.ZRevRangeByScore(name, start, end, match, limit, withData)
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		if s.canUpdateCache(name, wm) {
			s.cache.Add(&CacheItem{Key: name, CmdHash: h, Data: p})
		}
		s.weakCache.AddWeight(h, &WeakCacheItem{Data: p, Time: time.Now().Unix()}, int64(sizePairs(p)))
		return writePairs(p, w, command)
	case "GEORADIUS", "GEORADIUS_RO", "GEORADIUSWEAK":
		return s.runGeoRadius(w, false, name, h, wm, strings.HasSuffix(cmd, "WEAK"), command)
	case "GEORADIUSBYMEMBER", "GEORADIUSBYMEMBER_RO", "GEORADIUSBYMEMBERWEAK":
		return s.runGeoRadius(w, true, name, h, wm, strings.HasSuffix(cmd, "WEAK"), command)
	case "GEODIST":
		return s.runGeoDist(w, name, command)
	case "GEOPOS":
		return s.runGeoPos(w, name, command)
	case "SCAN":
		limit, match, withScores, inShard := s.HardLimit, "", false, -1
		for i := 2; i < command.ArgCount(); i++ {
			if strings.EqualFold(string(command.Get(i)), "COUNT") {
				limit = atoi(string(command.Get(i + 1)))
				i++
			} else if strings.EqualFold(string(command.Get(i)), "SHARD") {
				inShard = atoip(string(command.Get(i + 1)))
				i++
			} else if strings.EqualFold(string(command.Get(i)), "MATCH") {
				match = string(command.Get(i + 1))
				i++
			} else if strings.EqualFold(string(command.Get(i)), "WITHSCORES") {
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
	default:
		return w.WriteError("Command not support: " + cmd)
	}
}
