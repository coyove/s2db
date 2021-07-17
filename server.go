package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coyove/common/lru"
	"github.com/go-redis/redis/v8"
	"github.com/secmask/go-redisproto"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/wal"
	"go.etcd.io/bbolt"
)

func init() {
	redisproto.MaxBulkSize = 1 << 20
	redisproto.MaxNumArg = 10000
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
}

type Server struct {
	SlaveAddr string
	HardLimit int
	WeakTTL   time.Duration

	ln        net.Listener
	cache     *Cache
	weakCache *lru.Cache
	rdb       *redis.Client
	closed    bool

	survey struct {
		sysRead, sysWrite, cache, weakCache Survey
	}

	db [32]struct {
		*bbolt.DB
		writeWatermark int64
		bulking        int64
		walIn          chan [][]byte
		wal            *wal.Log
		readOnly       bool
		walCloseSignal chan bool
		rdCloseSignal  chan bool
	}
}

type Pair struct {
	Key   string
	Score float64
	Data  []byte
}

func Open(path string) (*Server, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}
	//last, _ := w.LastIndex()
	//first, _ := w.FirstIndex()
	//for i := first; i <= last; i++ {
	//	buf, _ := w.Read(i)
	//	c, _ := splitCommand(buf)
	//	fmt.Printf("%q\n", c.Get(0))
	//}
	//os.Exit(0)
	sz, _ := strconv.ParseInt(os.Getenv("CACHE"), 10, 64)
	if sz == 0 {
		sz = 1024 // 1G
	}
	x := &Server{
		cache:     NewCache(sz * 1024 * 1024),
		weakCache: lru.NewCache(sz * 1024 * 1024),
	}

	for i := range x.db {
		db, err := bbolt.Open(filepath.Join(path, "shard"+strconv.Itoa(i)), 0666, &bbolt.Options{
			FreelistType: bbolt.FreelistMapType,
		})
		if err != nil {
			return nil, err
		}
		w, err := wal.Open(filepath.Join(path, "wal"+strconv.Itoa(i)), wal.DefaultOptions)
		if err != nil {
			return nil, err
		}
		x.db[i].DB = db
		x.db[i].wal = w
		x.db[i].walIn = make(chan [][]byte, 1e3)
		x.db[i].walCloseSignal = make(chan bool)
		x.db[i].rdCloseSignal = make(chan bool)
	}
	return x, nil
}

func (s *Server) SetReadOnly(v bool) {
	for i := range s.db {
		s.db[i].readOnly = v
	}
}

func (s *Server) isReadOnly(key string) bool { return s.db[hashStr(key)%uint64(len(s.db))].readOnly }

func (s *Server) shardIndex(key string) int { return int(hashStr(key) % uint64(len(s.db))) }

func (s *Server) canUpdateCache(key string, wm int64) bool {
	return wm >= s.db[s.shardIndex(key)].writeWatermark
}

func (s *Server) getCache(h [2]uint64) interface{} {
	v, ok := s.cache.Get(h)
	if !ok {
		return nil
	}
	s.survey.cache.Incr(1)
	return v.Data
}

func (s *Server) getWeakCache(h [2]uint64) interface{} {
	v, ok := s.weakCache.Get(h)
	if !ok {
		return nil
	}
	if i := v.(*WeakCacheItem); time.Since(time.Unix(i.Time, 0)) <= s.WeakTTL {
		s.survey.weakCache.Incr(1)
		return i.Data
	}
	return nil
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
			db.readOnly = true
			errs <- db.Close()
			close(db.walIn)
			<-db.walCloseSignal
			if s.rdb != nil {
				<-db.rdCloseSignal
			}
			errs <- db.wal.Close()
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

func (s *Server) pickWal(name string) chan [][]byte { return s.db[s.shardIndex(name)].walIn }

func (s *Server) walTotalProgress() (total uint64, err error) {
	for i := range s.db {
		idx, err := s.db[i].wal.LastIndex()
		if err != nil {
			return 0, err
		}
		total += idx
	}
	return
}

func (s *Server) Serve(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.ln = listener
	if s.SlaveAddr != "" {
		s.rdb = redis.NewClient(&redis.Options{
			Addr: s.SlaveAddr,
		})
	}

	for i := range s.db {
		go s.writeWalCommand(i)
		if s.SlaveAddr != "" {
			go s.readWalCommand(i, s.SlaveAddr)
		}
	}

	if s.HardLimit <= 0 {
		s.HardLimit = 10000
	}

	if s.WeakTTL <= 0 {
		s.WeakTTL = time.Minute * 5
	}

	log.Info("listening on ", addr, " slave=", s.SlaveAddr)
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
	defer conn.Close()
	parser := redisproto.NewParser(conn)
	writer := redisproto.NewWriter(conn)
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
			cmd := strings.ToUpper(string(command.Get(0)))
			if cmd == "BULK" {
				ew = s.runBulk(writer, command)
			} else {
				ew = s.runCommand(writer, cmd, command, false)
			}
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

func (s *Server) runBulk(w *redisproto.Writer, command *redisproto.Command) error {
	x := string(command.Get(1))
	if x == "" {
		return w.WriteError("missing wal shard number")
	}

	walShard := atoi(x)
	walIndex := atoi(string(command.Get(2)))
	if walIndex == 0 {
		return w.WriteError("missing wal index")
	}

	buf := &bytes.Buffer{}
	dummy := redisproto.NewWriter(buf)
	if atomic.LoadInt64(&s.db[walShard].bulking) == 1 || !atomic.CompareAndSwapInt64(&s.db[walShard].bulking, 0, 1) {
		return w.WriteError("concurrent bulk write")
	}
	defer func() { atomic.StoreInt64(&s.db[walShard].bulking, 0) }()

	ww := s.db[walShard].wal
	if myIndex, _ := ww.LastIndex(); walIndex != int(myIndex)+1 {
		return w.WriteError(fmt.Sprintf("invalid wal index, want %d, gave %d", myIndex+1, walIndex))
	}

	for i := 3; i < command.ArgCount(); i++ {
		cmd, err := splitCommand(command.Get(i))
		if err != nil {
			log.Error("BULK: invalid payload: ", string(command.Get(i)))
			break
		}
		buf.Reset()
		s.runCommand(dummy, "", cmd, true)
		if buf.Len() > 0 && buf.Bytes()[0] == '-' {
			log.Error("BULK: ", strings.TrimSpace(buf.String()[1:]))
			break
		}
	}

	last, err := ww.LastIndex()
	if err != nil {
		return w.WriteError(err.Error())
	}
	return w.WriteInt(int64(last))
}

func (s *Server) runCommand(w *redisproto.Writer, cmd string, command *redisproto.Command, isBulk bool) error {
	if cmd == "" {
		cmd = strings.ToUpper(string(command.Get(0)))
	}

	h := hashCommands(command)
	wm := s.cache.nextWatermark()
	name := string(command.Get(1))
	if cmd == "DEL" || strings.HasPrefix(cmd, "Z") {
		if name == "" {
			return w.WriteError("command: empty name")
		}
		if strings.HasPrefix(name, "score") {
			return w.WriteError("command: invalid name starts with 'score'")
		}
		if cmd == "DEL" || cmd == "ZADD" || strings.HasPrefix(cmd, "ZREM") {
			s.survey.sysWrite.Incr(1)
		} else {
			s.survey.sysRead.Incr(1)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			log.Error(r, string(debug.Stack()))
		}
	}()

	var p []Pair
	var err error
	switch cmd {
	case "DIE":
		log.Panic(s.Close())
		panic("out")
	case "PING":
		if name == "" {
			return w.WriteSimpleString("PONG")
		}
		return w.WriteSimpleString(name)
	case "WALLAST":
		var c uint64
		if name != "" {
			c, err = s.db[atoi(name)].wal.LastIndex()
		} else {
			c, err = s.walTotalProgress()
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt(int64(c))
	case "WALTRUNCHEAD":
		index, _ := strconv.ParseUint(name, 10, 64)
		if err := s.db[atoi(name)].wal.TruncateFront(index); err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt(int64(index))
	case "CACHELEN":
		if name == "" {
			return w.WriteInt(int64(s.cache.CacheLen()))
		}
		return w.WriteInt(int64(s.cache.KeyCacheLen(name)))
	case "CACHESIZE":
		return w.WriteInt(int64(s.cache.curWeight + s.weakCache.Weight()))
	case "CACHERESET":
		weight := s.cache.curWeight + s.weakCache.Weight()
		s.cache.Clear()
		s.weakCache.Clear()
		return w.WriteInt(int64(weight))
	case "BIGKEYS":
		v, err := s.BigKeys(atoi(name))
		if err != nil {
			return w.WriteError(err.Error())
		}
		return writePairs(v, w, command)
	case "SYSLOAD":
		return w.WriteBulkStrings([]string{
			fmt.Sprintf("sys read: %v", s.survey.sysRead),
			fmt.Sprintf("sys write: %v", s.survey.sysWrite),
			fmt.Sprintf("cache: %v", s.survey.cache),
			fmt.Sprintf("weak cache: %v", s.survey.weakCache),
		})
	case "SHARDCALC":
		return w.WriteInt(int64(s.shardIndex(name)))
	case "SHARDRO":
		// if name == "" {
		a := []string{}
		for _, x := range s.db {
			a = append(a, strconv.FormatBool(x.readOnly))
		}
		return w.WriteBulkStrings(a)

		// -----------------------
		//  Client space write commands
		// -----------------------
	case "DEL": // TODO: multiple keys
		if !isBulk && s.isReadOnly(name) {
			return w.WriteError("readonly")
		}
		return s.runDel(w, name, command)
	case "ZADD":
		if !isBulk && s.isReadOnly(name) {
			return w.WriteError("readonly")
		}
		return s.runZAdd(w, name, command)
	case "ZINCRBY":
		if !isBulk && s.isReadOnly(name) {
			return w.WriteError("readonly")
		}
		return s.runZIncrBy(w, name, command)
	case "ZREM":
		if !isBulk && s.isReadOnly(name) {
			return w.WriteError("readonly")
		}
		return s.runZRem(w, name, command)
	case "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
		if !isBulk && s.isReadOnly(name) {
			return w.WriteError("readonly")
		}
		return s.runZRemRange(w, cmd, name, command)

		// -----------------------
		//  Client space read commands
		// -----------------------
	case "ZSCORE":
		s, err := s.ZMScore(name, restCommandsToKeys(2, command)...)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if math.IsNaN(s[0]) {
			return w.WriteBulk(nil)
		}
		return w.WriteBulkString(ftoa(s[0]))
	case "ZMSCORE":
		s, err := s.ZMScore(name, restCommandsToKeys(2, command)...)
		if err != nil {
			return w.WriteError(err.Error())
		}
		data := [][]byte{}
		for _, s := range s {
			if math.IsNaN(s) {
				data = append(data, nil)
			} else {
				data = append(data, []byte(ftoa(s)))
			}
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
		s.weakCache.Add(h, &WeakCacheItem{Data: data, Time: time.Now().Unix()})
		return w.WriteBulks(data...)
	case "ZCARD":
		c, err := s.ZCard(name)
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt(c)
	case "ZCOUNT":
		if v := s.getCache(h); v != nil {
			return w.WriteInt(int64(v.(int)))
		}
		if cmd == "ZCOUNTWEAK" {
			if v := s.getWeakCache(h); v != nil {
				return w.WriteInt(int64(v.(int)))
			}
		}
		c, err := s.ZCount(name, string(command.Get(2)), string(command.Get(3)))
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
		start, end := string(command.Get(2)), string(command.Get(3))
		switch cmd {
		case "ZRANGE":
			p, err = s.ZRange(name, atoi(start), atoi(end))
		case "ZREVRANGE":
			p, err = s.ZRevRange(name, atoi(start), atoi(end))
		case "ZRANGEBYLEX":
			p, err = s.ZRangeByLex(name, start, end)
		case "ZREVRANGEBYLEX":
			p, err = s.ZRevRangeByLex(name, start, end)
		case "ZRANGEBYSCORE":
			p, err = s.ZRangeByScore(name, start, end)
		case "ZREVRANGEBYSCORE":
			p, err = s.ZRevRangeByScore(name, start, end)
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		if s.canUpdateCache(name, wm) {
			s.cache.Add(&CacheItem{Key: name, CmdHash: h, Data: p})
		}
		s.weakCache.Add(h, &WeakCacheItem{Data: p, Time: time.Now().Unix()})
		return writePairs(p, w, command)
	default:
		//for i := 0; i < command.ArgCount(); i++ {
		//	fmt.Println(string(command.Get(i)))
		//}
		return w.WriteError("Command not support: " + cmd)
	}
}
