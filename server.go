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
	dumpWireLock s2.Locker
	rdbCache     *s2.LRUCache[string, *redis.Client]
	fillCache    *s2.LRUShard[struct{}]
	wmCache      *s2.LRUShard[[16]byte]
	ttlOnce      keyLock
	delOnce      keyLock
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
	// s.DB.DeleteRange([]byte("i"), []byte("j"), pebble.Sync)
	// s.DB.DeleteRange([]byte("h"), []byte("i"), pebble.Sync)
	for i := range s.Peers {
		s.Peers[i] = &endpoint{server: s}
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
		if key == "" || key == "*" || strings.Contains(key, "\x00") {
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
	case "APPEND": // key data_0 [QUORUM] [WAIT] [TTL seconds] [[AND DATA_1] ...]
		var data = [][]byte{K.BytesRef(2)}
		var ttl int64
		var wait, quorum bool
		for i := 3; i < K.ArgCount(); i++ {
			if K.StrEqFold(i, "ttl") {
				ttl = K.Int64(i + 1)
				i++
			} else if K.StrEqFold(i, "wait") {
				wait = true
			} else if K.StrEqFold(i, "quorum") {
				quorum = true
			} else if K.StrEqFold(i, "and") {
				data = append(data, K.BytesRef(i+1))
				i++
			} else {
				return w.WriteError(fmt.Sprintf("invalid flag %q", K.StrRef(i)))
			}
		}
		ids, err := s.Append(key, data, ttl, wait)
		if err != nil {
			return w.WriteError(err.Error())
		}
		hexIds := make([][]byte, len(ids))
		for i := range hexIds {
			hexIds[i] = hexEncode(ids[i])
		}
		if quorum {
			if s.HasOtherPeers() {
				args := []any{"RAWSET", key, ttl}
				for i, d := range data {
					args = append(args, ids[i], s2.Bytes(d))
				}
				recv, out := s.ForeachPeerSendCmd(func() redis.Cmder {
					return redis.NewStringCmd(context.TODO(), args...)
				})
				success := s.ProcessPeerResponse(true, recv, out, func(redis.Cmder) bool { return true })
				hexIds = append([][]byte{
					strconv.AppendInt(nil, int64(recv)+1, 10),
					strconv.AppendInt(nil, int64(success)+1, 10),
				}, hexIds...)
			} else {
				hexIds = append([][]byte{[]byte("1"), []byte("1")}, hexIds...)
			}
		}
		return w.WriteBulks(hexIds)
	case "RAWSET": // key ttl ID_0 DATA_0 ID_1 DATA_1...
		var after []s2.Pair
		for i := 3; i < K.ArgCount(); i += 2 {
			id := K.BytesRef(i)
			_ = id[15]
			after = append(after, s2.Pair{ID: id, Data: K.BytesRef(i + 1)})
		}
		n, err := s.rawSet(key, after, K.Int64(2), nil)
		if err != nil {
			return w.WriteError(err.Error())
		}
		s.Survey.RawSetN.Incr(int64(n))
		return w.WriteSimpleString("OK")
	case "PSELECT": // key raw_start count flag lowest => [ID 0, DATA 0, ...]
		start, flag := K.BytesRef(2), K.Int(4)
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
		data, partial, err := s.Range(key, start, K.Int(3), flag)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if partial {
			return w.WriteError("SELECT partial data")
		}
		a := make([][]byte, 0, 2*len(data))
		for _, p := range data {
			a = append(a, p.ID, p.Data)
		}
		return w.WriteBulks(a)
	case "SELECT": // key start count [ASC|DESC] [LOCAL] [DISTINCT] [RAW] [ALL] => [ID 0, TIME 0, DATA 0, ID 1, TIME 1, DATA 1 ... ]
		var localOnly, desc, distinct, raw, all bool
		for i := 4; i < K.ArgCount(); i++ {
			localOnly = localOnly || K.StrEqFold(i, "local")
			desc = desc || K.StrEqFold(i, "desc")
			distinct = distinct || K.StrEqFold(i, "distinct")
			raw = raw || K.StrEqFold(i, "raw")
			all = all || K.StrEqFold(i, "all")
		}

		flag := buildFlag([]bool{desc, distinct, raw}, []int{RangeDesc, RangeDistinct, RangeAll})
		start := s.translateCursor(K.BytesRef(2), desc)
		n := K.Int(3)
		origN := n
		if !testFlag {
			// Caller wants N elements, we actually fetch more than that.
			// Special case: n=1 is still n=1.
			n = int(float64(n) * (1 + rand.Float64()))
		}
		data, partial, err := s.Range(key, start, n, flag)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if !s.HasOtherPeers() || localOnly || raw {
			return s.convertPairs(w, data, origN)
		}
		if s2.AllPairsConsolidated(data) {
			s.Survey.AllConsolidated.Incr(1)
			return s.convertPairs(w, data, origN)
		}

		if partial {
			if len(data) == 0 {
				return w.WriteBulks([][]byte{})
			}
			// Range() has exited before we collected enough Pairs. Say 'n' is 10 and we have 2 Pairs, if
			// another peer returns 8 Pairs to us, we will have 10 Pairs which meet the SELECT criteria,
			// thus incorrectly insert consolidation marks into the database.
			n = len(data)
		}

		var lowest []byte
		if desc && len(data) > 0 && len(data) == n {
			lowest = data[len(data)-1].ID
		}
		recv, out := s.ForeachPeerSendCmd(func() redis.Cmder {
			return redis.NewStringSliceCmd(context.TODO(), "PSELECT", key, start, n, flag, lowest)
		})
		if recv == 0 {
			if all {
				return w.WriteError("no peer respond")
			}
			return s.convertPairs(w, data, origN)
		}

		origData := append([]s2.Pair{}, data...)
		success := s.ProcessPeerResponse(false, recv, out, func(cmd redis.Cmder) bool {
			for i, v := 0, cmd.(*redis.StringSliceCmd).Val(); i < len(v); i += 2 {
				_ = v[i][15]
				data = append(data, s2.Pair{ID: []byte(v[i]), Data: []byte(v[i+1])})
			}
			return true
		})
		if all && success != s.OtherPeersCount() {
			return w.WriteError(fmt.Sprintf("not all peers respond: %d/%d", success, s.OtherPeersCount()))
		}

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
		return s.convertPairs(w, data, origN)
	case "LOOKUP": // id [LOCAL]
		var data []byte
		var err error
		if K.StrEqFold(2, "local") {
			data, _, err = s.LookupID(s.translateCursor(K.BytesRef(1), false))
		} else {
			data, err = s.wrapLookup(s.translateCursor(K.BytesRef(1), false))
		}
		if err != nil {
			return w.WriteError(err.Error())
		}
		if data == nil {
			data = []byte{}
		}
		return w.WriteBulk(data)
	case "SELECTCOUNT":
		add, del, err := s.getHLL(key)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if K.StrEqFold(2, "hll") {
			return w.WriteBulks([][]byte{add, del})
		}
		if s.HasOtherPeers() {
			recv, out := s.ForeachPeerSendCmd(func() redis.Cmder {
				return redis.NewStringSliceCmd(context.TODO(), cmd, key, "HLL")
			})
			s.ProcessPeerResponse(false, recv, out, func(cmd redis.Cmder) bool {
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
	case "SCAN": // cursor [COUNT count] [INDEX]
		index, count := false, 10
		for i := 2; i < K.ArgCount(); i++ {
			if K.StrEqFold(i, "count") {
				count = K.Int(i + 1)
				i++
			} else {
				index = index || K.StrEqFold(i, "index")
			}
		}
		if index {
			return w.WriteObjects(s.ScanIndex(K.StrRef(1), count))
		}
		return w.WriteObjects(s.Scan(K.StrRef(1), count))
	case "HSET": // key member value [WAIT] [SET member_2 value_2 [SET ...]]
		kvs := [][]byte{K.BytesRef(2), K.BytesRef(3)}
		var wait bool
		for i := 2; i < K.ArgCount(); i++ {
			if K.StrEqFold(i, "set") {
				kvs = append(kvs, K.BytesRef(i+1), K.BytesRef(i+2))
				i += 2
			} else if K.StrEqFold(i, "wait") {
				wait = true
			}
		}
		id, err := s.HSet(key, kvs...)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if wait {
			id.Wait()
		}
		return w.WriteInt64(int64(id))
	case "PHGETALL": // key checksum
		data, err := s.Get(skp(key))
		if err != nil {
			return w.WriteError(err.Error())
		}
		if v := sha1.Sum(data); bytes.Equal(v[:], K.BytesRef(2)) {
			data = []byte{}
		}
		return w.WriteBulk(data)
	case "HLEN": // key [SYNC]
		if err := s.wrapHGetAll(key, K.StrEqFold(2, "sync")); err != nil {
			return w.WriteError(err.Error())
		}
		res, err := s.HLen(key)
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt64(int64(res))
	case "HGET": // key member [SYNC]
		if err := s.wrapHGetAll(key, K.StrEqFold(3, "sync")); err != nil {
			return w.WriteError(err.Error())
		}
		res, err := s.HGet(key, K.BytesRef(2))
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulk(res)
	case "HGETALL": // key [SYNC] [MATCH match]
		var sync bool
		var match []byte
		for i := 2; i < K.ArgCount(); i++ {
			sync = sync || K.StrEqFold(i, "sync")
			if K.StrEqFold(i, "match") {
				match = K.BytesRef(i + 1)
				i++
			}
		}
		if err := s.wrapHGetAll(key, sync); err != nil {
			return w.WriteError(err.Error())
		}
		res, err := s.HGetAll(key, match)
		if err != nil {
			return w.WriteError(err.Error())
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
