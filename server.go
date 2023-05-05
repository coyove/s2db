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
	"unsafe"

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
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	ln, lnHTTP net.Listener
	rdbCache   *s2pkg.LRUCache[string, *redis.Client]

	ServerConfig ServerConfig
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
		IRangeCacheHits s2pkg.Survey `metrics:"qps"`
		AppendExpire    s2pkg.Survey `metrics:"qps"`
		PeerBatchSize   s2pkg.Survey
		PeerLatency     sync.Map
		Command         sync.Map
	}

	DB *pebble.DB

	fillCache *s2pkg.LRUShard[struct{}]
	wmCache   *s2pkg.LRUShard[[16]byte]
	ttlOnce   sync.Map

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

	log.Info("server closing flush")
	if err := s.DB.Flush(); err != nil {
		log.Info("server closing: failed to flush: ", err)
		return err
	}

	log.Info("server closing jobs")
	s.Closed = true
	s.ReadOnly = true
	s.DBOptions.Cache.Unref()
	errs := make(chan error, 100)
	errs <- s.ln.Close()
	errs <- s.lnHTTP.Close()
	for _, p := range s.Peers {
		errs <- p.Close()
	}
	s.rdbCache.Range(func(k string, v *redis.Client) bool {
		errs <- v.Close()
		return true
	})

	errs <- s.DB.Close()

	close(errs)
	for e := range errs {
		if e != nil {
			err = errors.CombineErrors(err, e)
		}
	}
	log.Info("server closed, err=", err)
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
	s.acceptor(s.ln)
	return nil
}

func (s *Server) acceptor(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if !s.Closed {
				log.Error("accept: ", err, " current connections: ", s.Survey.Connections)
				continue
			} else {
				return
			}
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
	type any = interface{}
	key := K.Str(1)

	for _, nw := range blacklistIPs {
		if nw.Contains(src) {
			return w.WriteError(wire.ErrBlacklistedIP.Error() + src.String())
		}
	}

	if isWriteCommand[cmd] {
		if key == "" {
			return w.WriteError("invalid empty key name")
		}
		if strings.Contains(key, "\x00") {
			return w.WriteError("invalid key name containing null bytes (0x00)")
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
		info := s.InfoCommand(strings.ToLower(key))
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
	case "WAIT":
		x := K.BytesRef(1)
		for i := 2; i < K.ArgCount(); i++ {
			if bytes.Compare(K.BytesRef(i), x) > 0 {
				x = K.BytesRef(i)
			}
		}
		future.Future(binary.BigEndian.Uint64(hexDecode(x))).Wait()
		return w.WriteSimpleString("OK")
	case "CNFLAG":
		if _, consolidated, _ := s.MGet(K.Argv[1:]); consolidated {
			return w.WriteInt64(1)
		}
		return w.WriteInt64(0)
	}

	switch cmd {
	case "APPEND": // APPEND [DEFER] [TTL SECONDS] KEY DATA_0 DATA_1 ...
		var data [][]byte
		var ttl int64
		var wait = true
		key = ""
		for i := 1; i < K.ArgCount(); i++ {
			if K.StrEqFold(i, "ttl") {
				ttl = K.Int64(i + 1)
				i++
			} else if K.StrEqFold(i, "defer") {
				wait = false
			} else {
				key = K.StrRef(i)
				for i++; i < K.ArgCount(); i++ {
					data = append(data, K.Bytes(i))
				}
				break
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
	case "IRANGE": // IRANGE KEY RAW_START COUNT DESC_LOWEST => [ID 0, TIME 0, DATA 0, ...]
		if lowest := K.BytesRef(4); len(lowest) == 16 {
			if wm, ok := s.wmCache.Get16(s2pkg.HashStr128(key)); ok {
				if bytes.Compare(wm[:], lowest) <= 0 {
					s.Survey.IRangeCacheHits.Incr(1)
					return w.WriteBulks([][]byte{}) // no nil
				}
			}
			if s.test.IRangeCache {
				panic("test: IRANGE should use watermark cache")
			}
		}
		data, err := s.Range(key, K.BytesRef(2), K.Int(3))
		if err != nil {
			return w.WriteError(err.Error())
		}
		a := make([][]byte, 0, 3*len(data))
		for _, p := range data {
			a = append(a, p.IDHex(), nil, p.Data)
		}
		return w.WriteBulks(a)
	case "RANGE": // RANGE KEY START COUNT [ASC|DESC] [LOCAL] [MGET] => [ID 0, TIME 0, DATA 0, ID 1, TIME 1, DATA 1 ... ]
		n := K.Int(3)
		var start []byte
		var localOnly, mget bool
		switch s := K.StrRef(2); s {
		case "+", "+inf", "+INF", "+Inf":
			start = []byte(maxCursor)
		case "0":
			start = make([]byte, 16)
		default:
			if strings.HasPrefix(s, "@") {
				start = make([]byte, 16)
				if n < 0 {
					binary.BigEndian.PutUint64(start, uint64(s2pkg.MustParseFloat(s[1:])*1e9+1e9-1))
				} else {
					binary.BigEndian.PutUint64(start, uint64(s2pkg.MustParseFloat(s[1:])*1e9))
				}
			} else {
				start = hexDecode(K.BytesRef(2))
			}
		}
		if *(*string)(unsafe.Pointer(&start)) > maxCursor {
			start = []byte(maxCursor)
		}
		for i := 4; i < K.ArgCount(); i++ {
			localOnly = localOnly || K.StrEqFold(i, "local")
			mget = mget || K.StrEqFold(i, "mget")
		}

		trueN := iabs(n)
		if !testFlag {
			// Caller wants N elements, we actually fetch more than that.
			// Special case: n=1 is still n=1.
			n = int(float64(n) * (1 + rand.Float64()))
		}
		data, err := s.Range(key, start, n)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if !s.HasPeers() || localOnly {
			return s.convertPairs(w, data, trueN, mget)
		}
		if s2pkg.AllPairsConsolidated(data) {
			s.Survey.AllConsolidated.Incr(1)
			return s.convertPairs(w, data, trueN, mget)
		}
		if len(data) > trueN && s2pkg.AllPairsConsolidated(data[:trueN]) {
			s.Survey.AllConsolidated.Incr(1)
			return s.convertPairs(w, data, trueN, mget)
		}

		var lowest []byte
		if n < 0 && len(data) > 0 {
			lowest = data[len(data)-1].ID
		}
		recv, out := s.ForeachPeerSendCmd(func() redis.Cmder {
			return redis.NewStringSliceCmd(context.TODO(), "IRANGE", key, start, n, lowest)
		})
		if recv == 0 {
			return s.convertPairs(w, data, trueN, mget)
		}

		origData := append([]s2pkg.Pair{}, data...)
		success := s.ProcessPeerResponse(recv, out, func(cmd redis.Cmder) bool {
			if v, err := cmd.(*redis.StringSliceCmd).Result(); err != nil && err != redis.Nil {
				logrus.Errorf("failed to request peer: %v", err)
				if s.test.MustAllPeers {
					panic("not all peers respond")
				}
				return false
			} else {
				data = append(data, s2pkg.ConvertBulksToPairs(v)...)
				return true
			}
		})

		data = sortPairs(data, n >= 0)
		if len(data) > iabs(n) {
			data = data[:iabs(n)]
		}
		s.setMissing(key, origData, data, success == s.PeerCount())
		return s.convertPairs(w, data, trueN, mget)
	case "IMGET":
		ids := K.Argv[1:]
		data, _, err := s.MGet(ids)
		if err != nil {
			return w.WriteError(err.Error())
		}
		resp := make([][]byte, len(data)*2)
		for i := range data {
			resp[2*i] = ids[i]
			resp[2*i+1] = data[i]
		}
		return w.WriteBulks(resp)
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
	case "SCAN": // SCAN CURSOR COUNT
		data, nextCursor := s.Scan(K.StrRef(1), K.Int(2))
		return w.WriteObjects(nextCursor, data)
	}

	return w.WriteError(wire.ErrUnknownCommand.Error())
}
