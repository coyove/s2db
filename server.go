package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
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
	"syscall"
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
	"golang.org/x/sync/singleflight"
	"golang.org/x/sys/unix"
)

const ShardLogNum = 32

type Server struct {
	ln           net.Listener
	lnLocal      net.Listener
	lnWebConsole *s2pkg.LocalListener
	rdbCache     *s2pkg.LRUCache

	ServerConfig ServerConfig
	LocalRedis   *redis.Client
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
		SysRead         s2pkg.Survey          ``
		SysReadP99Micro s2pkg.P99SurveyMinute ``
		SysWrite        s2pkg.Survey          ``
		SlowLogs        s2pkg.Survey          ``
		PeerOnMissingN  s2pkg.Survey          `metrics:"mean"`
		PeerOnMissing   s2pkg.Survey          ``
		PeerOnOK        s2pkg.Survey          `metrics:"qps"`
		Consolidated    s2pkg.Survey          `metrics:"qps"`
		IExpireBefore   s2pkg.Survey
		PeerLatency     sync.Map
		Command         sync.Map
	}

	DB *pebble.DB

	fillLocks   [0x10000]sync.Mutex
	fillCache   *s2pkg.LRUCache
	expireGroup singleflight.Group
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
	x.DBOptions.EventListener = x.createDBListener()
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

	x.rdbCache = s2pkg.NewLRUCache(8, func(k string, v s2pkg.LRUValue) {
		log.Infof("redis client cache evicted: %s, close=%v", k, v.Value.(*redis.Client).Close())
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
	errs <- s.lnLocal.Close()
	errs <- s.lnWebConsole.Close()
	errs <- s.LocalRedis.Close()
	for _, p := range s.Peers {
		errs <- p.Close()
	}
	s.rdbCache.Range(func(k string, v s2pkg.LRUValue) bool {
		errs <- v.Value.(*redis.Client).Close()
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
	lc := net.ListenConfig{
		Control: func(network, address string, conn syscall.RawConn) error {
			var operr error
			if *netTCPWbufSize > 0 {
				if err := conn.Control(func(fd uintptr) {
					operr = syscall.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF, *netTCPWbufSize)
				}); err != nil {
					return err
				}
			}
			return operr
		},
	}
	s.ln, err = lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return err
	}
	s.lnLocal, err = net.Listen("unix", filepath.Join(os.TempDir(),
		fmt.Sprintf("_s2db_%d_%d_sock", os.Getpid(), time.Now().Unix())))
	if err != nil {
		return err
	}
	s.lnWebConsole = s2pkg.NewLocalListener()
	s.LocalRedis = redis.NewClient(&redis.Options{
		Network:  "unix",
		Addr:     s.lnLocal.Addr().String(),
		Password: s.ServerConfig.Password})
	s.Survey.StartAt = time.Now()

	log.Infof("listening on: redis=%v, local=%v", s.ln.Addr(), s.lnLocal.Addr())

	go s.startCronjobs()
	go s.webConsoleHandler()
	go s.acceptor(s.lnLocal)
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
		go func() {
			c := s2pkg.NewBufioConn(conn, &s.Survey.Connections)
			switch buf, _ := c.Peek(4); *(*string)(unsafe.Pointer(&buf)) {
			case "GET ", "POST", "HEAD":
				s.lnWebConsole.Feed(c)
			default:
				s.handleConnection(c)
			}
		}()
	}
}

func (s *Server) handleConnection(conn s2pkg.BufioConn) {
	defer conn.Close()
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
			startTime := time.Now()
			cmd := strings.ToUpper(command.Str(0))
			ew = s.runCommand(startTime, cmd, writer, s2pkg.GetRemoteIP(conn.RemoteAddr()), command)
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
			if testFlag {
				fmt.Println(r, string(debug.Stack()))
			}
			outErr = w.WriteError(fmt.Sprintf("fatal error (%s): %v", key, r))
		} else {
			diff := time.Since(start)
			if diff > time.Duration(s.ServerConfig.SlowLimit)*time.Millisecond && cmd != "PUSHLOGS" {
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
		startKey, _ := extdb.GetKeyPrefix(key)
		endKey, _ := extdb.GetKeyPrefix(end)
		list, _ := s.DB.EstimateDiskUsage(startKey, s2pkg.IncBytesInplace(endKey))
		return w.WriteInt64(int64(list))
	case "SLOW.LOG":
		return slowLogger.Formatter.(*s2pkg.LogFormatter).LogFork(w.Conn.(s2pkg.BufioConn))
	case "DB.LOG":
		return dbLogger.Formatter.(*s2pkg.LogFormatter).LogFork(w.Conn.(s2pkg.BufioConn))
	case "RUNTIME.LOG":
		return log.StandardLogger().Formatter.(*s2pkg.LogFormatter).LogFork(w.Conn.(s2pkg.BufioConn))
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

	switch cmd {
	case "APPEND", "APPENDWAIT": // APPEND[WAIT] KEY DATA_0 DATA_1 ...
		var data [][]byte
		for i := 2; i < K.ArgCount(); i++ {
			data = append(data, K.Bytes(i))
		}
		ids, err := s.Append(key, data, cmd == "APPENDWAIT")
		if err != nil {
			return w.WriteError(err.Error())
		}
		hexIds := make([][]byte, len(ids))
		for i := range hexIds {
			hexIds[i] = hexEncode(ids[i])
		}
		return w.WriteBulks(hexIds)
	case "IEXPIREBEFORE":
		if err := s.ExpireBefore(key, K.Int64(2)); err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteSimpleString("OK")
	case "EXPIREBEFORE":
		if K.StrEqFold(3, "get") {
			return w.WriteInt64(s.GetTombstone(key))
		}
		t := K.Int64(2)
		if err := s.ExpireBefore(key, t); err != nil {
			return w.WriteError(err.Error())
		}
		if s.HasPeers() {
			go func(start time.Time) {
				s.ForeachPeer(func(p *endpoint, cli *redis.Client) {
					cli.Do(context.TODO(), "IEXPIREBEFORE", key, t)
				})
				s.Survey.IExpireBefore.Incr(time.Since(start).Milliseconds())
			}(time.Now())
		}
		return w.WriteInt64(t)
	case "IRANGE": // IRANGE KEY RAW_START COUNT TOMBSTONE => [ID 0, TIME 0, DATA 0, ... TOMBSTONE]
		tombstone := K.Int64(4)
		if tombstone > 0 {
			localTombstone := s.GetTombstone(key)
			if tombstone > localTombstone {
				if err := s.ExpireBefore(key, tombstone); err != nil {
					logrus.Errorf("IRANGE: failed to ExpireBefore using remote tombstone: %v", err)
				}
			} else {
				tombstone = localTombstone
			}
		}
		data, err := s.Range(key, K.BytesRef(2), K.Int(3))
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulks(append(s2pkg.ConvertPairsToBulks(data),
			strconv.AppendInt(nil, tombstone, 10)))
	case "RANGE": // RANGE KEY START COUNT [LOCAL] => [ID 0, TIME 0, DATA 0, ID 1, TIME 1, DATA 1 ... ]
		n := K.Int(3)
		var start []byte
		switch s := K.StrRef(2); s {
		case "+", "+inf", "+INF", "+Inf":
			start = []byte("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xfe")
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
				start, _ = hex.DecodeString(s)
			}
		}

		data, err := s.Range(key, start, n)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if !s.HasPeers() || K.StrEqFold(4, "local") {
			return w.WriteBulks(s2pkg.ConvertPairsToBulks(data))
		}
		if s2pkg.AllPairsConsolidated(data) {
			s.Survey.Consolidated.Incr(1)
			return w.WriteBulks(s2pkg.ConvertPairsToBulks(data))
		}

		tombstone := s.GetTombstone(key)
		recv, out := s.ForeachPeerSendCmd(func() redis.Cmder {
			return redis.NewStringSliceCmd(context.TODO(), "IRANGE", key, start, n, tombstone)
		})
		if recv == 0 {
			return w.WriteBulks(s2pkg.ConvertPairsToBulks(data))
		}

		oldTombstone := tombstone

		success := s.ProcessPeerResponse(recv, out, func(cmd redis.Cmder) bool {
			if v, err := cmd.(*redis.StringSliceCmd).Result(); err != nil {
				logrus.Errorf("failed to request peer: %v", err)
				return false
			} else {
				remoteTombstone, _ := strconv.ParseInt(string(v[len(v)-1]), 10, 64)
				if remoteTombstone > tombstone {
					tombstone = remoteTombstone
				}
				data = append(data, s2pkg.ConvertBulksToPairs(v[:len(v)-1])...)
				return true
			}
		})

		if oldTombstone != tombstone {
			if err := s.ExpireBefore(key, tombstone); err != nil {
				logrus.Errorf("RANGE: failed to ExpireBefore using remote tombstone: %v", err)
			}
		}

		data = sortPairs(data, n >= 0)
		if n := int(math.Abs(float64(n))); len(data) > n {
			data = data[:n]
		}
		s.setMissing(key, data, success == s.PeerCount())
		return w.WriteBulks(s2pkg.ConvertPairsToBulks(data))
	case "IMGET":
		ids := K.Argv[1:]
		data, _, err := s.MGet(ids)
		if err != nil {
			return w.WriteError(err.Error())
		}
		resp := make([][]byte, len(data)*2)
		for i := range data {
			resp[2*i] = hexEncode(ids[i])
			resp[2*i+1] = data[i]
		}
		return w.WriteBulks(resp)
	case "MGET", "GET":
		var ids [][]byte
		for i := 1; i < K.ArgCount(); i++ {
			ids = append(ids, hexDecode(K.BytesRef(i)))
		}
		data, consolidated, err := s.MGet(ids)
		if err != nil {
			return w.WriteError(err.Error())
		}
		if consolidated {
			s.Survey.Consolidated.Incr(1)
			return w.WriteBulkOrBulks(cmd == "GET", data)
		}
		var missings []any
		for i, d := range data {
			if d == nil {
				missings = append(missings, ids[i])
			}
		}
		if len(missings) == 0 || !s.HasPeers() {
			return w.WriteBulkOrBulks(cmd == "GET", data)
		}

		missings = append([]any{"IMGET"}, missings...)
		recv, out := s.ForeachPeerSendCmd(func() redis.Cmder {
			return redis.NewStringStringMapCmd(context.TODO(), missings...)
		})
		if recv == 0 {
			return w.WriteBulkOrBulks(cmd == "GET", data)
		}

		m := map[string]string{}
		s.ProcessPeerResponse(recv, out, func(cmd redis.Cmder) bool {
			if m0, err := cmd.(*redis.StringStringMapCmd).Result(); err != nil {
				logrus.Errorf("failed to request peer: %v", err)
				return false
			} else {
				for k, v := range m0 {
					m[k] = v
				}
				return true
			}
		})
		for i, d := range data {
			if d != nil {
				continue
			}
			if v, ok := m[hex.EncodeToString(ids[i])]; ok {
				data[i] = []byte(v)
			}
		}
		return w.WriteBulkOrBulks(cmd == "GET", data)
	case "SCAN": // SCAN CURSOR COUNT
		data, nextCursor := s.Scan(K.StrRef(1), K.Int(2))
		return w.WriteObjects(nextCursor, data)
	}

	return w.WriteError(wire.ErrUnknownCommand.Error())
}
