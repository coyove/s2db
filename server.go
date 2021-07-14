package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

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
	ReadOnly  bool
	HardLimit int

	ln    net.Listener
	cache *Cache

	db [32]struct {
		*bbolt.DB
		bulking int64
		walIn   chan [][]byte
		wal     *wal.Log
	}
}

type Pair struct {
	Key   string
	Score float64
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
		cache: NewCache(sz * 1024 * 1024),
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
	}
	return x, nil
}

func (s *Server) Close() error {
	var errs []error
	for _, db := range s.db {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := db.wal.Close(); err != nil {
			errs = append(errs, err)
		}
		close(db.walIn)
	}
	if len(errs) > 0 {
		return fmt.Errorf("close: %v", errs)
	}

	log.Info("server closed")
	return s.ln.Close()
}

func (s *Server) pickWal(name string) chan [][]byte {
	h := hashStr(name) % uint64(len(s.db))
	return s.db[h].walIn
}

func (s *Server) Serve(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.ln = listener
	for i := range s.db {
		go s.writeWalCommand(i)
		if s.SlaveAddr != "" {
			go s.readWalCommand(i, s.SlaveAddr)
		}
	}

	if s.HardLimit <= 0 {
		s.HardLimit = 1000
	}

	log.Info("listening on ", addr, " slave=", s.SlaveAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if !strings.Contains(err.Error(), "use of closed") {
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
				ew = s.runCommand(writer, cmd, command)
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
			log.Error("BULK: invalid payload")
			break
		}
		buf.Reset()
		s.runCommand(dummy, "", cmd)
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

func (s *Server) runCommand(w *redisproto.Writer, cmd string, command *redisproto.Command) error {
	if cmd == "" {
		cmd = strings.ToUpper(string(command.Get(0)))
	}
	name := string(command.Get(1))
	if strings.HasPrefix(cmd, "Z") && name == "" {
		return w.WriteError("ZSet: empty name")
	}

	var p []Pair
	var err error
	switch cmd {
	case "PING":
		if name == "" {
			return w.WriteSimpleString("PONG")
		}
		return w.WriteSimpleString(name)
	case "WALLAST":
		idx, err := s.db[atoi(name)].wal.LastIndex()
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt(int64(idx))
	case "WALTRUNCHEAD":
		if s.ReadOnly {
			w.WriteError("readonly")
		}
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
		return w.WriteInt(int64(s.cache.curWeight))
	case "DEL":
		if s.ReadOnly {
			w.WriteError("readonly")
		}
		keys := restCommandsToKeys(1, command)
		c, err := s.Del(keys...)
		if err != nil {
			return w.WriteError(err.Error())
		}
		for _, key := range keys {
			s.cache.Remove(key)
			s.pickWal(key) <- [][]byte{[]byte("DEL"), []byte(key)}
		}
		return w.WriteInt(int64(c))
	case "ZADD":
		if s.ReadOnly {
			w.WriteError("readonly")
		}
		xx, nx, ch, idx := false, false, false, 2
		for ; ; idx++ {
			switch strings.ToUpper(string(command.Get(idx))) {
			case "XX":
				xx = true
				continue
			case "NX":
				nx = true
				continue
			case "CH":
				ch = true
				continue
			}
			break
		}

		pairs := []Pair{}
		for i := idx; i < command.ArgCount(); i += 2 {
			pairs = append(pairs, Pair{string(command.Get(i + 1)), atof(string(command.Get(i)))})
		}

		added, updated, err := s.ZAdd(name, pairs, nx, xx)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.cache.Remove(name)
		s.pickWal(name) <- dupCommand(command)
		if ch {
			return w.WriteInt(int64(added + updated))
		}
		return w.WriteInt(int64(added))
	case "ZINCRBY":
		if s.ReadOnly {
			w.WriteError("readonly")
		}
		v, err := s.ZIncrBy(name, string(command.Get(3)), atof(string(command.Get(2))))
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.cache.Remove(name)
		s.pickWal(name) <- dupCommand(command)
		return w.WriteBulkString(ftoa(v))
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
	case "ZREM":
		if s.ReadOnly {
			w.WriteError("readonly")
		}
		c, err := s.ZRem(name, restCommandsToKeys(2, command)...)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.cache.Remove(name)
		s.pickWal(name) <- dupCommand(command)
		return w.WriteInt(int64(c))
	case "ZCARD":
		c, err := s.ZCard(name)
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt(c)
	case "ZCOUNT":
		c, err := s.ZCount(name, string(command.Get(2)), string(command.Get(3)))
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt(int64(c))
	case "ZRANGE", "ZREVRANGE", "ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
		h := hashCommands(command)
		if v, ok := s.cache.Get(h); ok {
			return writePairs(v.Data, w, command)
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
		s.cache.Add(&CacheItem{Key: name, CmdHash: h, Data: p})
		return writePairs(p, w, command)
	case "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
		if s.ReadOnly {
			w.WriteError("readonly")
		}
		start, end := string(command.Get(2)), string(command.Get(3))
		switch cmd {
		case "ZREMRANGEBYLEX":
			p, err = s.ZRemRangeByLex(name, start, end)
		case "ZREMRANGEBYSCORE":
			p, err = s.ZRemRangeByScore(name, start, end)
		case "ZREMRANGEBYRANK":
			p, err = s.ZRemRangeByRank(name, atoi(start), atoi(end))
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.cache.Remove(name)
		s.pickWal(name) <- dupCommand(command)
		return w.WriteInt(int64(len(p)))
	default:
		// log.Error("command not support: ", cmd)
		//for i := 0; i < command.ArgCount(); i++ {
		//	fmt.Println(string(command.Get(i)))
		//}
		return w.WriteError("Command not support: " + cmd)
	}
}

func (s *Server) writeWalCommand(i int) {
	for cmd := range s.db[i].walIn {
		last, err := s.db[i].wal.LastIndex()
		if err != nil {
			log.Error("wal: ", err)
			continue
		}
		ctr := last + 1
		if err := s.db[i].wal.Write(ctr, joinCommand(cmd...)); err != nil {
			// Basically we won't reach here as long as the filesystem is okay
			// otherwise we are totally screwed up
			log.Error("wal fatal: ", err)
		}
	}
	log.Info("#", i, " wal worker exited")
}

func (s *Server) readWalCommand(shard int, slaveAddr string) {
	ctx := context.TODO()
	rdb := redis.NewClient(&redis.Options{
		Addr: slaveAddr,
	})

	for {
		cmd := redis.NewIntCmd(ctx, "WALLAST", shard)
		err := rdb.Process(ctx, cmd)
		if err != nil && cmd.Err() != nil {
			log.Error("#", shard, " getting wal index from slave: ", slaveAddr, " err=", err)
			time.Sleep(time.Second * 5)
			continue
		}

		slaveWalIndex := uint64(cmd.Val())
		masterWalIndex, err := s.db[shard].wal.LastIndex()
		if err != nil {
			if err != wal.ErrClosed {
				log.Error("#", shard, "read local wal index: ", err)
			}
			goto EXIT
		}

		if slaveWalIndex == masterWalIndex {
			time.Sleep(time.Second)
			continue
		}

		if slaveWalIndex > masterWalIndex {
			log.Error("#", shard, " fatal: slave index surpass master index: ", slaveWalIndex, masterWalIndex)
			goto EXIT
		}

		cmds := []interface{}{"BULK", shard, slaveWalIndex + 1}
		sz := 0
		for i := slaveWalIndex + 1; i <= masterWalIndex; i++ {
			data, err := s.db[shard].wal.Read(i)
			if err != nil {
				log.Error("#", shard, " wal read #", i, ":", err)
				goto EXIT
			}
			cmds = append(cmds, string(data))
			sz += len(data)
			if len(cmds) == 200 || sz > 10*1024 {
				break
			}
		}

		cmd = redis.NewIntCmd(ctx, cmds...)
		if err := rdb.Process(ctx, cmd); err != nil || cmd.Err() != nil {
			if !strings.Contains(fmt.Sprint(err), "concurrent bulk write") {
				log.Error("#", shard, " slave bulk returned: ", err, " current master index: ", masterWalIndex)
			}
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second / 2)
	}

EXIT:
	log.Info("#", shard, " wal replayer exited")
}
