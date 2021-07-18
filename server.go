package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coyove/common/lru"
	"github.com/go-redis/redis/v8"
	"github.com/secmask/go-redisproto"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

type Server struct {
	MasterAddr string
	ServerConfig

	ln        net.Listener
	cache     *Cache
	weakCache *lru.Cache
	rdb       *redis.Client
	closed    bool

	survey struct {
		startAt                             time.Time
		sysRead, sysWrite, cache, weakCache Survey
	}

	db [32]struct {
		*bbolt.DB
		readOnly       bool
		writeWatermark int64
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

	x := &Server{}
	for i := range x.db {
		db, err := bbolt.Open(filepath.Join(path, "shard"+strconv.Itoa(i)), 0666, &bbolt.Options{
			FreelistType: bbolt.FreelistMapType,
		})
		if err != nil {
			return nil, err
		}
		x.db[i].DB = db
		x.db[i].rdCloseSignal = make(chan bool)
	}
	return x, nil
}

func (s *Server) SetReadOnly(v bool) {
	for i := range s.db {
		s.db[i].readOnly = v
	}
}

func (s *Server) isReadOnly(key string) bool { return s.db[s.shardIndex(key)].readOnly }

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
			if s.rdb != nil {
				<-db.rdCloseSignal
			}
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

func (s *Server) walProgress(shard int) (total uint64, err error) {
	f := func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("wal"))
		if bk != nil {
			k, _ := bk.Cursor().Last()
			if len(k) == 8 {
				total += binary.BigEndian.Uint64(k)
			}
		}
		return nil
	}
	if shard == -1 {
		for i := range s.db {
			if err := s.db[i].View(f); err != nil {
				return 0, err
			}
		}
	} else {
		if err := s.db[shard].View(f); err != nil {
			return 0, err
		}
	}
	return
}

func (s *Server) Serve(addr string) error {
	if err := s.loadConfig(); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.ln = listener
	s.survey.startAt = time.Now()

	if s.MasterAddr != "" {
		s.rdb = redis.NewClient(&redis.Options{
			Addr: s.MasterAddr,
		})
		for i := range s.db {
			go s.requestLogWorker(i)
		}
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
			ew = s.runCommand(writer, cmd, command, false)
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

func (s *Server) runBulk(walShard int, cmds []string) {
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
		if strings.HasPrefix(name, "score.") {
			return w.WriteError("command: invalid name starts with 'score.'")
		}
		if cmd == "DEL" || cmd == "ZADD" || strings.HasPrefix(cmd, "ZREM") {
			s.survey.sysWrite.Incr(1)
		} else {
			s.survey.sysRead.Incr(1)
		}
	}

	defer func(start time.Time) {
		if r := recover(); r != nil {
			log.Error(r, string(debug.Stack()))
		} else {
			if diff := time.Since(start); diff > s.SlowLimit {
				buf := bytes.NewBufferString("[slow log] " + diff.String() + " ")
				for i := 0; i < command.ArgCount(); i++ {
					buf.Write(command.Get(i))
					buf.WriteByte(' ')
				}
				log.Info(buf.String())
			}
		}
	}(time.Now())

	var p []Pair
	var err error
	switch cmd {
	case "DIE":
		sec := time.Now().Second()
		if sec > 58 {
			log.Panic(s.Close())
		}
		go func() {
			time.Sleep(time.Duration(60-sec) * time.Second)
			log.Panic(s.Close())
		}()
		return w.WriteInt(60 - int64(sec))
	case "PING":
		if name == "" {
			return w.WriteSimpleString("PONG " + Version)
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
			buf, _ := json.Marshal(s.ServerConfig)
			return w.WriteBulk(buf)
		}
	case "WALLAST":
		var c uint64
		if name != "" {
			c, err = s.walProgress(atoi(name))
		} else {
			c, err = s.walProgress(-1)
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt(int64(c))
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
			fmt.Sprintf("uptime: %v", time.Since(s.survey.startAt)),
			fmt.Sprintf("sys read: %v", s.survey.sysRead),
			fmt.Sprintf("sys write: %v", s.survey.sysWrite),
			fmt.Sprintf("cache: %v", s.survey.cache),
			fmt.Sprintf("weak cache: %v", s.survey.weakCache),
		})
	case "SHARDCALC":
		return w.WriteInt(int64(s.shardIndex(name)))
	case "SHARDRO":
		a := []string{}
		for _, x := range s.db {
			a = append(a, strconv.FormatBool(x.readOnly))
		}
		return w.WriteBulkStrings(a)
	case "SHARDDUMP":
		x := &s.db[atoi(name)]
		x.readOnly = true
		defer func() { x.readOnly = false }()

		x.DB.Update(func(tx *bbolt.Tx) error { return nil }) // wait all other writes on this shard to finish
		if err := x.DB.Sync(); err != nil {
			return w.WriteError(err.Error())
		}
		if err := CopyFile(x.DB.Path(), x.DB.Path()+".bak"); err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteSimpleString("OK")
	case "REQUESTLOG":
		start, _ := strconv.ParseUint(string(command.Get(2)), 10, 64)
		logs, err := s.responseLog(atoi(name), start)
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulkStrings(logs)

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
		start, end, limit := string(command.Get(2)), string(command.Get(3)), -1
		if strings.EqualFold(string(command.Get(command.ArgCount()-3)), "limit") {
			limit = atoi(string(command.Get(command.ArgCount() - 1)))
			command.Argv = command.Argv[:len(command.Argv)-3]
		}

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
			p, err = s.ZRangeByScore(name, start, end, limit)
		case "ZREVRANGEBYSCORE":
			p, err = s.ZRevRangeByScore(name, start, end, limit)
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
