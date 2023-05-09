package main

import (
	"bytes"
	"context"
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

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

const lockShards = 32

type Server struct {
	ln, lnHTTP   net.Listener
	ServerConfig ServerConfig
	DB           *pebble.DB
	Peers        [future.Channels]*endpoint
	DBPath       string
	DBOptions    *pebble.Options
	Channel      int64
	ReadOnly     bool
	Closed       bool // server close flag
	SelfManager  *bas.Program

	dieLock      sync.Mutex
	dumpWireLock s2pkg.Locker

	Survey struct {
		StartAt         time.Time
		Connections     int64
		SysRead         s2pkg.Survey
		SysReadP99Micro s2pkg.P99SurveyMinute
		SysWrite        s2pkg.Survey
		SlowLogs        s2pkg.Survey
		PeerOnMissingN  s2pkg.Survey `metrics:"mean"`
		PeerOnMissing   s2pkg.Survey
		PeerOnOK        s2pkg.Survey `metrics:"qps"`
		AllConsolidated s2pkg.Survey `metrics:"qps"`
		SelectCacheHits s2pkg.Survey `metrics:"qps"`
		AppendExpire    s2pkg.Survey `metrics:"qps"`
		RangeDistinct   s2pkg.Survey
		PeerBatchSize   s2pkg.Survey
		PeerLatency     sync.Map
		Command         sync.Map
	}

	rdbCache  *s2pkg.LRUCache[string, *redis.Client]
	fillCache *s2pkg.LRUShard[struct{}]
	wmCache   *s2pkg.LRUShard[[16]byte]
	ttlOnce   [lockShards]sync.Map
	delOnce   [lockShards]sync.Map
	errThrot  s2pkg.ErrorThrottler

	test struct {
		Fail         bool
		MustAllPeers bool
		NoSetMissing bool
		IRangeCache  bool
	}
}

func Open(dbPath string) (x *Server, err error) {
	if err := os.MkdirAll(dbPath, 0777); err != nil {
		return nil, err
	}

	x = &Server{
		DBPath: dbPath,
		DBOptions: (&pebble.Options{
			Logger:       dbLogger,
			Cache:        pebble.NewCache(int64(*pebbleCacheSize) << 20),
			MemTableSize: *pebbleMemtableSize << 20,
			MaxOpenFiles: *pebbleMaxOpenFiles,
		}).EnsureDefaults(),
	}
	x.DBOptions.FS = &extdb.VFS{
		FS:       x.DBOptions.FS,
		DumpVDir: filepath.Join(os.TempDir(), strconv.FormatUint(clock.Id(), 16)),
	}
	if !testFlag {
		x.DBOptions.EventListener = x.createDBListener()
	}
	start := time.Now()
	x.DB, err = pebble.Open(dbPath, x.DBOptions)
	if err != nil {
		return nil, err
	}
	for i := range x.Peers {
		x.Peers[i] = &endpoint{server: x}
	}
	if err := x.loadConfig(); err != nil {
		return nil, err
	}
	log.Infof("open data: %s in %v", dbPath, time.Since(start))

	x.rdbCache = s2pkg.NewLRUCache(8, func(k string, v *redis.Client) {
		log.Infof("redis client cache evicted: %s, close=%v", k, v.Close())
	})
	return x, nil
}

func (s *Server) Close() (err error) {
	s.dieLock.Lock()

	start := time.Now()
	err = errors.CombineErrors(err, s.DB.Flush())
	log.Infof("server closing flush in %v", time.Since(start))

	s.Closed = true
	s.ReadOnly = true
	s.DBOptions.Cache.Unref()

	err = errors.CombineErrors(err, s.ln.Close())
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
		log.Info("server closed, err=", err)
	}
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
	s.ln, err = net.ListenTCP("tcp", tcpAddr)
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

	log.Infof("listening on %v (RESP) and %v (HTTP)", s.ln.Addr(), s.lnHTTP.Addr())

	go s.startCronjobs()
	go s.httpServer()

	for {
		conn, err := s.ln.Accept()
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
			if s.ServerConfig.Password != "" && !auth {
				if command.StrEqFold(0, "AUTH") && command.Str(1) == s.ServerConfig.Password {
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
				ew = s.runCommand(startTime, cmd, writer, s2pkg.GetRemoteIP(conn.RemoteAddr()), command)
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
			return w.WriteError("invalid key name (either empty or containing '\\x00'/'*')")
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
			outErr = w.WriteError(fmt.Sprintf("fatal error (%s:%d): %v", filepath.Base(fn), ln, r))
		} else {
			diff := time.Since(start)
			if diff > time.Duration(s.ServerConfig.SlowLimit)*time.Millisecond {
				slowLogger.Infof("%s\t% 4.3f\t%s\t%v", key, diff.Seconds(), src, K)
				s.Survey.SlowLogs.Incr(diff.Milliseconds())
			}
			if isWriteCommand[cmd] {
				s.Survey.SysWrite.Incr(diff.Milliseconds())
			}
			if isReadCommand[cmd] {
				s.Survey.SysRead.Incr(diff.Milliseconds())
				s.Survey.SysReadP99Micro.Incr(diff.Microseconds())
			}
			if isWriteCommand[cmd] || isReadCommand[cmd] {
				x, _ := s.Survey.Command.LoadOrStore(cmd, new(s2pkg.Survey))
				x.(*s2pkg.Survey).Incr(diff.Milliseconds())
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
	case "EVAL", "EVALRO":
		v := nj.MustRun(nj.LoadString(key, s.getScriptEnviron(K.Argv[2:]...)))
		return w.WriteValue(v)
	case "PING":
		if key == "" {
			return w.WriteSimpleString("PONG " + s.ServerConfig.ServerName + " " + Version)
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
	case "INFOMETRICS":
		switch v := s.MetricsCommand(key).(type) {
		case *s2pkg.Survey:
			m := v.Metrics()
			return w.WriteObjects(
				"qps1m", m.QPS[0], "qps5m", m.QPS[1], "qps", m.QPS[2],
				"mean1m", m.Mean[0], "mean5m", m.Mean[1], "mean", m.Mean[2],
				"max1m", m.Max[0], "max5m", m.Max[1], "max", m.Max[2],
			)
		case *s2pkg.P99SurveyMinute:
			return w.WriteObjects("p99", v.P99())
		}
		return w.WriteError("invalid metrics key: " + key)
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
	case "SSDISKSIZE":
		end := K.Str(2)
		if end == "" {
			end = key + "\xff"
		}
		startKey := extdb.GetKeyPrefix(key)
		endKey := extdb.GetKeyPrefix(end)
		list, _ := s.DB.EstimateDiskUsage(startKey, s2pkg.IncBytesInplace(endKey))
		return w.WriteInt64(int64(list))
	case "SLOW.LOG":
		return slowLogger.Formatter.(*s2pkg.LogFormatter).LogFork(w.Conn.(net.Conn))
	case "DB.LOG":
		return dbLogger.Formatter.(*s2pkg.LogFormatter).LogFork(w.Conn.(net.Conn))
	case "RUNTIME.LOG":
		return log.StandardLogger().Formatter.(*s2pkg.LogFormatter).LogFork(w.Conn.(net.Conn))
	}

	switch cmd {
	case "APPEND": // KEY DATA_0 [DEFER] [TTL SECONDS] [[AND DATA_1] ...]
		var data = [][]byte{K.BytesRef(2)}
		var ttl int64
		var wait = true
		for i := 3; i < K.ArgCount(); i++ {
			if K.StrEqFold(i, "ttl") {
				ttl = K.Int64(i + 1)
				i++
			} else if K.StrEqFold(i, "defer") {
				wait = false
			} else if K.StrEqFold(i, "and") {
				data = append(data, K.BytesRef(i+1))
				i++
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
		return w.WriteBulks(hexIds)
	case "PSELECT":
		// KEY RAW_START COUNT FLAG LOWEST => [ID 0, DATA 0, ...]
		// '*' ID_0 ID_1 ... => [ID 0, DATA 0, ...]
		if key == "*" {
			ids := K.Argv[2:]
			data, _, err := s.MGet(ids)
			if err != nil {
				return w.WriteError(err.Error())
			}
			resp := make([][]byte, 0, len(data)*2)
			for i := range data {
				resp = append(resp, ids[i], data[i])
			}
			return w.WriteBulks(resp)
		}
		if lowest := K.BytesRef(5); len(lowest) == 16 {
			if wm, ok := s.wmCache.Get16(s2pkg.HashStr128(key)); ok {
				if bytes.Compare(wm[:], lowest) <= 0 {
					s.Survey.SelectCacheHits.Incr(1)
					return w.WriteBulks([][]byte{}) // no nil
				}
			}
			if s.test.IRangeCache {
				panic("test: PSELECT should use watermark cache")
			}
		}
		data, partial, err := s.Range(key, K.BytesRef(2), K.Int(3), K.Int(4))
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
	case "SELECT": // KEY START COUNT [ASC|DESC] [LOCAL] [DISTINCT] [RAW] => [ID 0, TIME 0, DATA 0, ID 1, TIME 1, DATA 1 ... ]
		var localOnly, desc, distinct, raw bool
		for i := 4; i < K.ArgCount(); i++ {
			localOnly = localOnly || K.StrEqFold(i, "local")
			desc = desc || K.StrEqFold(i, "desc")
			distinct = distinct || K.StrEqFold(i, "distinct")
			raw = raw || K.StrEqFold(i, "raw")
		}

		flag := orFlag(0, desc, RangeDesc)
		flag = orFlag(flag, distinct, RangeDistinct)
		flag = orFlag(flag, raw, RangeAll)
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
		if !s.HasPeers() || localOnly || raw {
			return s.convertPairs(w, data, origN)
		}
		if s2pkg.AllPairsConsolidated(data) {
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
			return s.convertPairs(w, data, origN)
		}

		origData := append([]s2pkg.Pair{}, data...)
		success := s.ProcessPeerResponse(recv, out, func(cmd redis.Cmder) bool {
			for i, v := 0, cmd.(*redis.StringSliceCmd).Val(); i < len(v); i += 2 {
				_ = v[i][15]
				data = append(data, s2pkg.Pair{ID: []byte(v[i]), Data: []byte(v[i+1])})
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
		s.setMissing(key, origData, data, success == s.PeerCount())
		return s.convertPairs(w, data, origN)
	case "MGET", "GET":
		var ids [][]byte
		for i := 1; i < K.ArgCount(); i++ {
			ids = append(ids, hexDecode(K.BytesRef(i)))
		}
		data, err := s.wrapMGet(ids)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if cmd == "GET" {
			return w.WriteBulk(data[0])
		}
		return w.WriteBulks(data)
	case "HCOUNT":
		add, del, err := s.getHLL(key)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if K.StrEqFold(2, "hll") {
			return w.WriteBulks([][]byte{add, del})
		}
		if s.HasPeers() {
			recv, out := s.ForeachPeerSendCmd(func() redis.Cmder {
				return redis.NewStringSliceCmd(context.TODO(), cmd, key, "HLL")
			})
			s.ProcessPeerResponse(recv, out, func(cmd redis.Cmder) bool {
				v := cmd.(*redis.StringSliceCmd).Val()
				add.Merge(s2pkg.HyperLogLog(v[0]))
				del.Merge(s2pkg.HyperLogLog(v[1]))
				return true
			})
		}
		return w.WriteInt64(int64(add.Count()) - int64(del.Count()))
	case "SCAN": // SCAN CURSOR COUNT
		data, nextCursor := s.Scan(K.StrRef(1), K.Int(2))
		return w.WriteObjects(nextCursor, data)
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
