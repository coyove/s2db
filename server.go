package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
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
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/wire"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	DB          *pebble.DB
	Peers       [future.Channels]*endpoint
	DBPath      string
	DBOptions   *pebble.Options
	Channel     int64
	ReadOnly    bool
	Closed      bool // server close flag
	SelfManager *bas.Program
	Config      ServerConfig
	Survey      ServerSurvey

	lnRESP       net.Listener
	lnHTTP       net.Listener
	dieLock      sync.Mutex
	dumpWireLock sync.Mutex
	rdbCache     *s2.LRUCache[string, *redis.Client]
	fillCache    *s2.LRUShard[struct{}]
	wmCache      *s2.LRUShard[[16]byte]
	ttlOnce      keyLock
	distinctOnce keyLock
	hashSyncOnce keyLock
	errThrot     s2.ErrorThrottler

	test struct {
		Fail         bool
		NoSetMissing bool
		IRangeCache  bool
	}
}

func Open(dbPath string) (s *Server, err error) {
	if err := os.MkdirAll(dbPath, 0777); err != nil {
		return nil, err
	}

	s = &Server{
		DBPath: dbPath,
		DBOptions: (&pebble.Options{
			Logger:       dbLogger,
			Cache:        pebble.NewCache(int64(*pebbleCacheSize) << 20),
			MemTableSize: *pebbleMemtableSize << 20,
			MaxOpenFiles: *pebbleMaxOpenFiles,
		}).EnsureDefaults(),
	}
	s.DBOptions.FS = &VFS{
		FS:       s.DBOptions.FS,
		DumpVDir: filepath.Join(os.TempDir(), strconv.FormatInt(future.UnixNano(), 16)),
	}
	s.DBOptions.Merger = s.createMerger()
	if !testFlag {
		s.DBOptions.EventListener = s.createDBListener()
	}
	start := time.Now()
	s.DB, err = pebble.Open(dbPath, s.DBOptions)
	if err != nil {
		return nil, err
	}

	// s.DB.DeleteRange([]byte("h"), []byte("i"), pebble.Sync)
	for i := range s.Peers {
		s.Peers[i] = &endpoint{server: s, index: i}
	}
	if err := s.loadConfig(); err != nil {
		return nil, err
	}
	log.Infof("open data %s in %v", dbPath, time.Since(start))

	s.rdbCache = s2.NewLRUCache(8, func(k string, v *redis.Client) {
		log.Infof("redis client cache evicted: %s, close=%v", k, v.Close())
	})
	return s, nil
}

func (s *Server) Close() (err error) {
	s.dieLock.Lock()

	start := time.Now()
	err = errors.CombineErrors(err, s.DB.Flush())
	log.Infof("server closing flush in %v", time.Since(start))

	s.Closed = true
	s.ReadOnly = true
	s.DBOptions.Cache.Unref()

	err = errors.CombineErrors(err, s.lnRESP.Close())
	err = errors.CombineErrors(err, s.lnHTTP.Close())
	for _, p := range s.Peers {
		err = errors.CombineErrors(err, p.Close())
	}

	s.rdbCache.Range(func(k string, v *redis.Client) bool {
		err = errors.CombineErrors(err, v.Close())
		return true
	})

	err = errors.CombineErrors(err, s.DB.Close())
	if err != nil {
		log.Errorf("server failed to close: %v", err)
	}

	log.Infof("server closed: %v", err)
	return err
}

func (s *Server) Serve(addr string) (err error) {
	// lc := net.ListenConfig{
	// 	Control: func(network, address string, conn syscall.RawConn) error {
	// 		var operr error
	// 		if *netTCPWbufSize > 0 {
	// 			if err := conn.Control(func(fd uintptr) {
	// 				operr = syscall.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF, *netTCPWbufSize)
	// 			}); err != nil {
	// 				return err
	// 			}
	// 		}
	// 		return operr
	// 	},
	// }
	// s.ln, err = lc.Listen(context.Background(), "tcp", addr)
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	s.lnRESP, err = net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}

	tcpAddr2 := *tcpAddr
	tcpAddr2.Port++
	s.lnHTTP, err = net.ListenTCP("tcp", &tcpAddr2)
	if err != nil {
		return err
	}
	s.Survey.StartAt = time.Now()

	log.Infof("listening on %v (RESP) and %v (HTTP)", s.lnRESP.Addr(), s.lnHTTP.Addr())

	go s.startCronjobs()
	go s.httpServer()

	for {
		conn, err := s.lnRESP.Accept()
		if err != nil {
			if !s.Closed {
				log.Errorf("failed to accept (N=%d): %v", s.Survey.Connections, err)
				continue
			}
			return err
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	atomic.AddInt64(&s.Survey.Connections, 1)
	defer func() {
		atomic.AddInt64(&s.Survey.Connections, -1)
		conn.Close()
	}()

	parser := wire.NewParser(conn)
	writer := wire.NewWriter(conn, log.StandardLogger())
	var ew error
	for auth := false; ; {
		command, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*wire.ProtocolError)
			if ok {
				ew = writer.WriteError(err.Error())
			} else {
				if err != io.EOF {
					log.Info(err, " closed connection to ", conn.RemoteAddr())
				}
				break
			}
		} else {
			if !command.IsLast() {
				writer.EnablePipelineMode()
			}
			if s.Config.Password != "" && !auth {
				if command.StrEqFold(0, "AUTH") && command.Str(1) == s.Config.Password {
					auth = true
					writer.WriteSimpleString("OK")
				} else {
					writer.WriteError(wire.ErrNoAuth.Error())
				}
				writer.Flush()
				continue
			}
			if s.test.Fail {
				ew = writer.WriteError("test: failed on purpose")
			} else {
				startTime := time.Now()
				cmd := strings.ToUpper(command.Str(0))
				ew = s.runCommand(startTime, cmd, writer, s2.GetRemoteIP(conn.RemoteAddr()), command)
			}
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

func (s *Server) runCommand(startTime time.Time, cmd string, w *wire.Writer, src net.IP, K *wire.Command) (outErr error) {
	key := K.Str(1)

	for _, nw := range blacklistIPs {
		if nw.Contains(src) {
			return w.WriteError(wire.ErrBlacklistedIP.Error() + src.String())
		}
	}

	if isWriteCommand[cmd] {
		if key == "" || strings.Contains(key, "\x00") {
			return w.WriteError("invalid key name")
		}
		if err := s.checkWritable(); err != nil {
			return w.WriteError(err.Error())
		}
	}

	defer func(start time.Time) {
		if r := recover(); r != nil {
			if testFlag || os.Getenv("PRINT_STACK") != "" {
				fmt.Println(r, string(debug.Stack()))
			}
			var fn string
			var ln int
			for i := 1; ; i++ {
				if _, fn, ln, _ = runtime.Caller(i); ln == 0 {
					fn = "<unknown>"
					break
				} else if strings.Contains(fn, "s2db") {
					break
				}
			}
			outErr = w.WriteError(fmt.Sprintf("[%s] fatal error (%s:%d): %v", cmd, filepath.Base(fn), ln, r))
			s.Survey.FatalError.Incr(1)
		} else {
			diff := time.Since(start)
			if diff > time.Duration(s.Config.SlowLimit)*time.Millisecond {
				slowLogger.Infof("%s\t% 4.3f\t%s\t%v", key, diff.Seconds(), src, K)
				s.Survey.SlowLogs.Incr(diff.Milliseconds())
			}
			if isWriteCommand[cmd] {
				s.Survey.SysWrite.Incr(diff.Milliseconds())
				s.Survey.SysWriteP99Micro.Incr(diff.Microseconds())
			}
			if isReadCommand[cmd] {
				s.Survey.SysRead.Incr(diff.Milliseconds())
				s.Survey.SysReadP99Micro.Incr(diff.Microseconds())
			}
			if isWriteCommand[cmd] || isReadCommand[cmd] {
				x, _ := s.Survey.Command.LoadOrStore(cmd, new(s2.Survey))
				x.(*s2.Survey).Incr(diff.Milliseconds())
			}
		}
	}(startTime)

	// General commands
	switch cmd {
	case "DIE":
		s.Close()
		os.Exit(0)
	case "AUTH": // AUTH command is processed before runComamnd, so always return OK here
		return w.WriteSimpleString("OK")
	case "EVAL":
		return w.WriteValue(s.mustRunCode(key, K.Argv[2:]...))
	case "PING":
		if key == "" {
			return w.WriteSimpleString("PONG " + s.Config.ServerName + " " + Version)
		}
		return w.WriteSimpleString(key)
	case "CONFIG":
		switch strings.ToUpper(key) {
		case "GET":
			v, _ := s.GetConfig(K.Str(2))
			return w.WriteBulkStrings([]string{K.Str(2), v})
		case "SET":
			found, err := s.UpdateConfig(K.Str(2), K.Str(3), false)
			if err != nil {
				return w.WriteError(err.Error())
			} else if found {
				return w.WriteSimpleString("OK")
			}
			return w.WriteError("field not found")
		case "COPY":
			if err := s.CopyConfig(K.Str(2), K.Str(3)); err != nil {
				return w.WriteError(err.Error())
			}
			return w.WriteSimpleString("OK")
		default:
			return w.WriteBulkStrings(s.listConfigCommand())
		}
	case "INFO":
		info := s.InfoCommand(key)
		return w.WriteBulkString(strings.Join(info, "\r\n"))
	case "DUMPDB":
		go func(start time.Time) {
			log.Infof("start dumping to %s", key)
			err := s.DB.Checkpoint(key, pebble.WithFlushedWAL())
			log.Infof("dumped to %s in %v: %v", key, time.Since(start), err)
		}(startTime)
		return w.WriteSimpleString("STARTED")
	case "COMPACTDB":
		go s.DB.Compact(nil, []byte{0xff}, true)
		return w.WriteSimpleString("STARTED")
	case "DUMPWIRE":
		go s.DumpWire(key)
		return w.WriteSimpleString("STARTED")
	case "SLOW.LOG":
		return slowLogger.Formatter.(*s2.LogFormatter).LogFork(w.Conn.(net.Conn))
	case "DB.LOG":
		return dbLogger.Formatter.(*s2.LogFormatter).LogFork(w.Conn.(net.Conn))
	case "RUNTIME.LOG":
		return log.StandardLogger().Formatter.(*s2.LogFormatter).LogFork(w.Conn.(net.Conn))
	}

	switch cmd {
	case "APPEND": // key data_0 [WAIT] [TTL seconds] [[AND data_1] ...] [SETID ...]
		data, ids, ttl, sync, wait := parseAPPEND(K)
		ids, err := s.Append(key, ids, data, ttl, wait)
		if err != nil {
			return w.WriteError(err.Error())
		}
		hexIds := hexEncodeBulks(ids)
		if sync && s.HasOtherPeers() {
			args := []any{"APPEND", key}
			if len(data) > 0 {
				args = append(args, s2.Bytes(data[0]), "TTL", ttl)
			} else {
				args = append(args, "", "TTL", ttl)
			}
			for i := 1; i < len(data); i++ {
				args = append(args, "AND", s2.Bytes(data[i]))
			}
			args = append(args, "SETID")
			args = append(args, bbany(ids)...)
			s.ForeachPeerSendCmd(SendCmdOptions{Async: true}, func() redis.Cmder {
				return redis.NewStringSliceCmd(context.TODO(), args...)
			}, nil)
			s.Survey.AppendSyncN.Incr(int64(len(ids)))
		}
		return w.WriteBulks(hexIds)
	case "PSELECT": // key raw_start count flag lowest key_hash => [ID 0, DATA 0, ...]
		start, flag, okh := K.BytesRef(2), K.Int(4), s2.KeyHashUnpack(K.BytesRef(6))
		wm, wmok := s.wmCache.Get16(s2.HashStr128(key))
		if lowest := K.BytesRef(5); len(lowest) == 16 {
			if wmok && bytes.Compare(wm[:], lowest) <= 0 {
				s.Survey.SelectCacheHits.Incr(1)
				return w.WriteBulks([][]byte{}) // no nil
			}
			if s.test.IRangeCache {
				panic("test: PSELECT should use watermark cache")
			}
		}
		if flag&RangeDesc == 0 && wmok && bytes.Compare(wm[:], start) < 0 {
			s.Survey.SelectCacheHits.Incr(1)
			return w.WriteBulks([][]byte{}) // no nil
		}
		data, err := s.Range(key, start, K.Int(3), flag)
		if err != nil {
			return w.WriteError(err.Error())
		}
		a := make([][]byte, 0, 2*len(data))
		for _, p := range data {
			if !s2.KeyHashContains(okh, p.ID) {
				a = append(a, p.ID, p.Data)
			}
		}
		return w.WriteBulks(a)
	case "SELECT": // key start count [ASC|DESC] [DISTINCT] [RAW] => [ID 0, TIME 0, DATA 0, ID 1, TIME 1, DATA 1 ... ]
		n, desc, distinct, raw, flag := parseSELECT(K)
		start := s.translateCursor(K.BytesRef(2), desc)
		origN := n
		if !testFlag {
			// Caller wants N Pairs, we actually fetch more than that to consolidate them better (hopefully).
			// Special case: n=1 is still n=1.
			n = int(float64(n) * (1 + rand.Float64()))
		}
		data, err := s.Range(key, start, n, flag)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if !s.HasOtherPeers() || raw {
			return s.convertPairs(w, data, origN, true)
		}
		if s2.AllPairsConsolidated(data) {
			s.Survey.AllConsolidated.Incr(1)
			return s.convertPairs(w, data, origN, true)
		}

		origData := append([]s2.Pair{}, data...)

		var lowest []byte
		if desc && len(data) > 0 && len(data) == n {
			lowest = data[len(data)-1].ID
		}
		kh := s2.KeyHashPack(data)
		s.Survey.KeyHashRatio.Incr(int64(len(kh) * 1000 / (len(data)*8 + 1)))
		_, success := s.ForeachPeerSendCmd(SendCmdOptions{},
			func() redis.Cmder {
				return redis.NewStringSliceCmd(context.TODO(), "PSELECT", key, start, n, flag, lowest, kh)
			}, func(cmd redis.Cmder) bool {
				for i, v := 0, cmd.(*redis.StringSliceCmd).Val(); i < len(v); i += 2 {
					_ = v[i][15]
					data = append(data, s2.Pair{ID: []byte(v[i]), Data: []byte(v[i+1])})
				}
				return true
			})

		data = sortPairs(data, !desc)
		if distinct {
			data = distinctPairsData(data)
		}
		if len(data) > n {
			data = data[:n]
		}
		s.setMissing(key, origData, data,
			// Only if we received acknowledgements from all other peers, we can consolidate Pairs.
			success == s.OtherPeersCount(),
			// If iterating in asc order from the beginning, the leftmost Pair is the trusted start of the list.
			*(*string)(unsafe.Pointer(&start)) == minCursor && !desc,
			// If iterating in desc order and not collecting enough Pairs, the rightmost Pair is the start.
			len(data) < n && desc,
		)
		return s.convertPairs(w, data, origN, success == s.OtherPeersCount())
	case "PLOOKUP":
		data, _, err := s.LookupID(s.translateCursor(K.BytesRef(1), false))
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulkNoNil(data)
	case "LOOKUP":
		data, err := s.wrapLookup(s.translateCursor(K.Bytes(1), false))
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulkNoNil(data)
	case "COUNT":
		add, del, err := s.getHLL(key)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if K.StrEqFold(2, "hll") {
			return w.WriteBulks([][]byte{add, del})
		}
		if s.HasOtherPeers() {
			s.ForeachPeerSendCmd(SendCmdOptions{}, func() redis.Cmder {
				return redis.NewStringSliceCmd(context.TODO(), "COUNT", key, "HLL")
			}, func(cmd redis.Cmder) bool {
				v := cmd.(*redis.StringSliceCmd).Val()
				add.Merge(s2.HyperLogLog(v[0]))
				del.Merge(s2.HyperLogLog(v[1]))
				return true
			})
		}
		x := int64(add.Count()) - int64(del.Count())
		if x < 0 {
			x = 0
		}
		return w.WriteInt64(x)
	case "SCAN": // cursor [COUNT count] [INDEX] [HASH]
		hash, index, count := parseSCAN(K)
		if hash {
			return w.WriteObjects(s.ScanHash(K.StrRef(1), count))
		}
		if index {
			return w.WriteObjects(s.ScanIndex(K.StrRef(1), count))
		}
		return w.WriteObjects(s.Scan(K.StrRef(1), count))
	case "HSET": // key member value [WAIT] [SET member_2 value_2 [SET ...]] [SETID ...]
		kvs, ids, sync, wait := parseHSET(K)
		ids, err := s.HSet(key, wait, kvs, ids)
		if err != nil {
			return w.WriteError(err.Error())
		}
		hexIds := hexEncodeBulks(ids)
		if sync && s.HasOtherPeers() {
			args := []any{"HSET", key, s2.Bytes(kvs[0]), s2.Bytes(kvs[1])}
			for i := 2; i < len(kvs); i += 2 {
				args = append(args, "SET", s2.Bytes(kvs[i]), s2.Bytes(kvs[i+1]))
			}
			args = append(args, "SETID")
			args = append(args, bbany(ids)...)
			s.ForeachPeerSendCmd(SendCmdOptions{Async: true}, func() redis.Cmder {
				return redis.NewStringSliceCmd(context.TODO(), args...)
			}, nil)
			s.Survey.HSetSyncN.Incr(int64(len(ids)))
		}
		return w.WriteBulks(hexIds)
	case "PHGETALL": // key checksum
		data, rd, err := s.DB.Get(skp(key))
		if err == pebble.ErrNotFound {
			return w.WriteBulkNoNil(nil)
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		defer rd.Close()
		if v := sha1.Sum(data); bytes.Equal(v[:], K.BytesRef(2)) {
			data = nil
		}
		return w.WriteBulkNoNil(data)
	case "HSYNC":
		if err := s.syncHashmap(key, true); err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteSimpleString("OK")
	case "HLEN": // key
		s.syncHashmap(key, false)
		res, err := s.HLen(key)
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt64(int64(res))
	case "HGET", "HTIME": // key member
		s.syncHashmap(key, false)
		res, time, err := s.HGet(key, K.BytesRef(2))
		if err != nil {
			return w.WriteError(err.Error())
		}
		if cmd == "HTIME" {
			return w.WriteInt64(time / 1e6 / 10 * 10)
		}
		return w.WriteBulkNoNil(res)
	case "HGETALL": // key [KEYSONLY] [MATCH match] [NOCOMPRESS] [TIMESTAMP]
		noCompress, ts, keysOnly, match := parseHGETALL(K)
		s.syncHashmap(key, false)
		res, err := s.HGetAll(key, match, true, !keysOnly, ts)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if !noCompress && s2.SizeOfBulksExceeds(res, s.Config.CompressLimit) {
			return w.WriteBulk(s2.CompressBulks(res))
		}
		return w.WriteBulks(res)
	case "WAIT":
		x := K.BytesRef(1)
		for i := 2; i < K.ArgCount(); i++ {
			if bytes.Compare(K.BytesRef(i), x) > 0 {
				x = K.BytesRef(i)
			}
		}
		future.Future(binary.BigEndian.Uint64(hexDecode(x))).Wait()
		return w.WriteSimpleString("OK")
	}

	return w.WriteError(wire.ErrUnknownCommand.Error())
}
