package main

import (
	"bufio"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/secmask/go-redisproto"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/wal"
	"go.etcd.io/bbolt"
)

type DB struct {
	cache *Cache
	wal   *wal.Log
	db    [32]*bbolt.DB
}

type Pair struct {
	Key   string
	Score float64
}

func Open(path string) (*DB, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}
	w, err := wal.Open(filepath.Join(path, "wal"), wal.DefaultOptions)
	if err != nil {
		return nil, err
	}
	x := &DB{
		cache: NewCache(2 * 1024 * 1024 * 1024),
		wal:   w,
	}
	for i := range x.db {
		db, err := bbolt.Open(filepath.Join(path, "shard"+strconv.Itoa(i)), 0666, &bbolt.Options{
			FreelistType: bbolt.FreelistMapType,
		})
		if err != nil {
			return nil, err
		}
		x.db[i] = db
	}
	return x, nil
}

func (z *DB) Close() error {
	var errs []error
	for _, db := range z.db {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close: %v", errs)
	}
	return nil
}

type Server struct {
	DB *DB

	ln    net.Listener
	walIn chan *redisproto.Command
}

func (s *Server) Close() error {
	close(s.walIn)
	return s.ln.Close()
}

func (s *Server) Serve(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = listener
	s.walIn = make(chan *redisproto.Command, 1e3)
	go s.writeWalCommand()

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
	writer := redisproto.NewWriter(bufio.NewWriter(conn))
	var ew error
	for {
		command, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*redisproto.ProtocolError)
			if ok {
				ew = writer.WriteError(err.Error())
			} else {
				log.Println(err, " closed connection to ", conn.RemoteAddr())
				break
			}
		} else {
			cmd := strings.ToUpper(string(command.Get(0)))
			ew = s.runCommand(writer, cmd, string(command.Get(1)), command)
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

func (s *Server) runCommand(w *redisproto.Writer, cmd, name string, command *redisproto.Command) error {
	if strings.HasPrefix(cmd, "Z") && name == "" {
		return fmt.Errorf("ZSet: empty name")
	}

	var p []Pair
	var err error
	switch cmd {
	case "PING":
		return w.WriteSimpleString("PONG")
	case "WALLAST":
		idx, err := s.DB.wal.LastIndex()
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt(int64(idx))
	case "WALTRUNCHEAD":
		index, _ := strconv.ParseUint(name, 10, 64)
		if err := s.DB.wal.TruncateFront(index); err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt(int64(index))
	case "DEL":
		c, err := s.DB.Del(restCommandsToKeys(1, command)...)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.DB.cache.Remove(name)
		s.walIn <- command
		return w.WriteInt(int64(c))
	case "ZADD":
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

		added, updated, err := s.DB.ZAdd(name, pairs, nx, xx)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.DB.cache.Remove(name)
		s.walIn <- command
		if ch {
			return w.WriteInt(int64(added + updated))
		}
		return w.WriteInt(int64(added))
	case "ZINCRBY":
		v, err := s.DB.ZIncrBy(name, string(command.Get(3)), atof(string(command.Get(2))))
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.DB.cache.Remove(name)
		s.walIn <- command
		return w.WriteBulkString(ftoa(v))
	case "ZSCORE":
		s, err := s.DB.ZMScore(name, restCommandsToKeys(2, command)...)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if math.IsNaN(s[0]) {
			return w.WriteBulk(nil)
		}
		return w.WriteBulkString(ftoa(s[0]))
	case "ZMSCORE":
		s, err := s.DB.ZMScore(name, restCommandsToKeys(2, command)...)
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
		c, err := s.DB.ZRem(name, restCommandsToKeys(2, command)...)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.DB.cache.Remove(name)
		s.walIn <- command
		return w.WriteInt(int64(c))
	case "ZCARD":
		c, err := s.DB.ZCard(name)
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt(c)
	case "ZCOUNT":
		c, err := s.DB.ZCount(name, string(command.Get(2)), string(command.Get(3)))
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt(int64(c))
	case "ZRANGE", "ZREVRANGE", "ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
		h := hashCommands(command)
		if v, ok := s.DB.cache.Get(h); ok {
			return writePairs(v.Data, w, command)
		}
		start, end := string(command.Get(2)), string(command.Get(3))
		switch cmd {
		case "ZRANGE":
			p, err = s.DB.ZRange(name, atoi(start), atoi(end))
		case "ZREVRANGE":
			p, err = s.DB.ZRevRange(name, atoi(start), atoi(end))
		case "ZRANGEBYLEX":
			p, err = s.DB.ZRangeByLex(name, start, end)
		case "ZREVRANGEBYLEX":
			p, err = s.DB.ZRevRangeByLex(name, start, end)
		case "ZRANGEBYSCORE":
			p, err = s.DB.ZRangeByScore(name, start, end)
		case "ZREVRANGEBYSCORE":
			p, err = s.DB.ZRevRangeByScore(name, start, end)
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.DB.cache.Add(&CacheItem{Key: name, CmdHash: h, Data: p})
		return writePairs(p, w, command)
	case "ZREMRANGEBYLEX", "ZREMRANGEBYSCORE", "ZREMRANGEBYRANK":
		start, end := string(command.Get(2)), string(command.Get(3))
		switch cmd {
		case "ZREMRANGEBYLEX":
			p, err = s.DB.ZRemRangeByLex(name, start, end)
		case "ZREMRANGEBYSCORE":
			p, err = s.DB.ZRemRangeByScore(name, start, end)
		case "ZREMRANGEBYRANK":
			p, err = s.DB.ZRemRangeByRank(name, atoi(start), atoi(end))
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.DB.cache.Remove(name)
		s.walIn <- command
		return w.WriteInt(int64(len(p)))
	default:
		log.Error("command not support: ", cmd)
		return w.WriteError("Command not support: " + cmd)
	}
}

func (s *Server) writeWalCommand() {
	for cmd := range s.walIn {
		last, err := s.DB.wal.LastIndex()
		if err != nil {
			log.Error("wal: ", err)
			continue
		}
		ctr := last + 1
		if err := s.DB.wal.Write(ctr, joinCommand(cmd)); err != nil {
			// Basically we won't reach here as long as the filesystem is okay
			// otherwise we are totally screwed up
			log.Error("wal fatal: ", err)
		}
	}
	log.Info("wal worker exited")
}
