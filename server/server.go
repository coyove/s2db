package server

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
	"sort"
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
	"github.com/coyove/s2db/s2/filelock"
	"github.com/coyove/s2db/wire"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

var Version string

type Server struct {
	Interop     interop
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
	asyncOnce    s2.KeyLock
	errThrot     s2.ErrorThrottler
	dbFile       *os.File
	pipeline     chan *dbPayload

	TestFlags struct {
		Fail         bool
		NoSetMissing bool
		IRangeCache  bool
	}
}

func Open(dbPath string, channel int64) (s *Server, err error) {
	if channel < 0 || channel >= future.Channels {
		return nil, fmt.Errorf("invalid channel")
	}
	if err := os.MkdirAll(dbPath, 0777); err != nil {
		return nil, err
	}

	dbFile, err := os.Open(dbPath)
	if err != nil {
		return nil, err
	}

	lockCh := make(chan error)
	go func() { lockCh <- filelock.Lock(dbFile) }()
	select {
	case err := <-lockCh:
		if err != nil {
			return nil, err
		}
	case <-time.After(time.Second * 3):
		return nil, fmt.Errorf("timed out to lock data")
	}

	s = &Server{
		DBPath: dbPath,
		DBOptions: (&pebble.Options{
			Logger:       dbLogger,
			Cache:        pebble.NewCache(int64(*pebbleCacheSize) << 20),
			MemTableSize: *pebbleMemtableSize << 20,
			MaxOpenFiles: *pebbleMaxOpenFiles,
		}).EnsureDefaults(),
		Channel:  channel,
		dbFile:   dbFile,
		pipeline: make(chan *dbPayload, 1e5),
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

	close(s.pipeline)

	err = errors.CombineErrors(err, filelock.Unlock(s.dbFile))
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
	for i := 0; i < runtime.NumCPU(); i++ {
		go s.pipelineWorker()
	}

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
	writer := wire.NewWriter(conn)
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
			if s.TestFlags.Fail {
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

func (s *Server) runCommand(startTime time.Time, cmd string, w wire.WriterImpl, src net.IP, K *wire.Command) (outErr error) {
	key := K.Str(1)

	if isWriteCommand[cmd] {
		if key == "" || strings.Contains(key, "\x00") {
			return w.WriteError(fmt.Sprintf("invalid key name: %q", key))
		}
		if err := s.checkWritable(); err != nil {
			return w.WriteError(err.Error())
		}
	}

	defer s.recoverLogger(startTime, cmd, w, func(diff time.Duration) {
		slowLogger.Infof("%s\t% 4.3f\t%s\t%v", key, diff.Seconds(), src, K)
	})

	// General commands
	switch cmd {
	case "DIE":
		s.Close()
		os.Exit(0)
	case "AUTH": // AUTH command is processed before runComamnd, so always return OK here
		return w.WriteSimpleString("OK")
	// case "EVAL":
	// 	return w.WriteValue(s.mustRunCode(key, K.Argv[2:]...))
	case "PING":
		if key == "" {
			return w.WriteSimpleString("PONG " + s.Config.ServerName + " " + Version)
		}
		return w.WriteSimpleString(key)
	case "CONFIG":
		switch strings.ToUpper(key) {
		case "GET":
			v, _ := s.GetConfig(K.StrRef(2))
			return w.WriteBulks([]string{K.StrRef(2), v})
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
			return w.WriteBulks(s.execListConfig())
		}
	case "INFO":
		info := s.InfoCommand(key)
		return w.WriteBulk(strings.Join(info, "\r\n"))
	case "DUMPDB":
		go func(start time.Time) {
			log.Infof("start dumping to %s", key)
			err := s.DB.Checkpoint(key, pebble.WithFlushedWAL())
			log.Infof("dumped to %s in %v: %v", key, time.Since(start), err)
		}(startTime)
		return w.WriteSimpleString("STARTED")
	case "DUMPWIRE":
		go s.DumpWire(key)
		return w.WriteSimpleString("STARTED")
	case "SLOW.LOG":
		return slowLogger.Formatter.(*logf).LogFork(w.(*wire.Writer).Sink.(net.Conn))
	case "DB.LOG":
		return dbLogger.Formatter.(*logf).LogFork(w.(*wire.Writer).Sink.(net.Conn))
	case "RUNTIME.LOG":
		return log.StandardLogger().Formatter.(*logf).LogFork(w.(*wire.Writer).Sink.(net.Conn))
	}

	switch cmd {
	case "APPEND": // key data_0 [DP dp_len] [SYNC] [NOEXP] [[AND data_1] ...] [SETID ...]
		data, ids, opts := parseAPPEND(K)
		return s.execAppend(w, key, ids, data, opts)
	case "PSELECT": // key raw_start count flag desc_lowest packed_ids => [ID 0, DATA 0, ...]
		start, flag, contains := K.BytesRef(2), s2.ParseSelectOptions(K.Int64(4)), s2.UnpackIDs(K.BytesRef(6))
		largest, ok := s.wmCache.Get16(s2.HashStr128(key))
		if descLowest := K.BytesRef(5); len(descLowest) == 16 {
			if ok && bytes.Compare(largest[:], descLowest) <= 0 {
				s.Survey.SelectCacheHits.Incr(1)
				return w.WriteBulks(nil)
			}
			if s.TestFlags.IRangeCache {
				panic("test: PSELECT should use watermark cache")
			}
		}
		if !flag.Desc && ok && bytes.Compare(largest[:], start) < 0 {
			s.Survey.SelectCacheHits.Incr(1)
			return w.WriteBulks(nil)
		}
		data, err := s.implRange(key, start, K.Int(3), flag)
		if err != nil {
			return w.WriteError(err.Error())
		}
		a := make([][]byte, 0, 2*len(data))
		for _, p := range data {
			if !contains(p.ID) {
				a = append(a, p.ID, p.Data)
			}
		}
		return w.WriteBulks(a)
	case "SELECT": // key start count [ASC|DESC] [ASYNC] [RAW] => [ID 0, TIME 0, DATA 0, ID 1, TIME 1, DATA 1 ... ]
		n, flag := parseSELECT(K)
		start := s.translateCursor(K.BytesRef(2), flag.Desc)
		return s.execSelect(w, key, start, n, flag)
	case "PLOOKUP":
		data, key, err := s.implLookupID(K.BytesRef(1))
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulks([][]byte{[]byte(key), data})
	case "LOOKUP":
		data, err := s.execLookup(s.translateCursor(K.Bytes(1), false))
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulk(data)
	case "SCAN": // cursor [COUNT count] [INDEX] [LOCAL]
		index, local, count := parseSCAN(K)
		if index {
			return w.WriteBulkBulks(s.ScanLookupIndex(K.StrRef(1), count))
		}
		return s.execScan(w, K.StrRef(1), count, local)
	case "WAITEFFECT":
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

func (s *Server) recoverLogger(start time.Time, cmd string, w wire.WriterImpl, onSlow func(time.Duration)) {
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
		w.WriteError(fmt.Sprintf("[%s] fatal error (%s:%d): %v", cmd, filepath.Base(fn), ln, r))
		s.Survey.FatalError.Incr(1)
	} else {
		diff := time.Since(start)
		if diff > time.Duration(s.Config.SlowLimit)*time.Millisecond {
			if onSlow != nil {
				onSlow(diff)
			} else {
				slowLogger.Infof("%s\t% 4.3f\tinterop\t%v", cmd, diff.Seconds(), cmd)
			}
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
}

func (s *Server) execAppend(w wire.WriterImpl, key string, ids, data [][]byte, opts s2.AppendOptions) error {
	ids, maxID, err := s.implAppend(key, ids, data, opts)
	if err != nil {
		return w.WriteError(err.Error())
	}
	hexIds := hexEncodeBulks(ids)
	if !opts.NoSync && s.HasOtherPeers() {
		args := []any{"APPEND", key, data[0]}
		for i := 1; i < len(data); i++ {
			args = append(args, "AND", data[i])
		}
		args = append(args, "SETID")
		for _, id := range ids {
			args = append(args, id)
		}
		s.ForeachPeerSendCmd(SendCmdOptions{Async: true}, func() redis.Cmder {
			return redis.NewStringSliceCmd(context.TODO(), args...)
		}, nil)
		s.Survey.AppendSyncN.Incr(int64(len(ids)))
	}
	if opts.Effect {
		maxID.Wait()
	}
	return w.WriteBulks(hexIds)
}

// func (s *Server) execHSet(w wire.WriterImpl, key string, ids, kvs [][]byte, sync, wait bool) error {
// 	ids, maxID, err := s.implHSet(key, kvs, ids)
// 	if err != nil {
// 		return w.WriteError(err.Error())
// 	}
// 	hexIds := hexEncodeBulks(ids)
// 	if sync && s.HasOtherPeers() {
// 		args := []any{"HSET", key, s2.Bytes(kvs[0]), s2.Bytes(kvs[1])}
// 		for i := 2; i < len(kvs); i += 2 {
// 			args = append(args, "SET", s2.Bytes(kvs[i]), s2.Bytes(kvs[i+1]))
// 		}
// 		args = append(args, "SETID")
// 		args = append(args, bbany(ids)...)
// 		s.ForeachPeerSendCmd(SendCmdOptions{Async: true}, func() redis.Cmder {
// 			return redis.NewStringSliceCmd(context.TODO(), args...)
// 		}, nil)
// 		s.Survey.HSetSyncN.Incr(int64(len(ids)))
// 	}
// 	if wait {
// 		maxID.Wait()
// 	}
// 	return w.WriteBulks(hexIds)
//
// }
//
// func (s *Server) execHGetAll(w wire.WriterImpl, key string, noCompress, ts, keysOnly bool, match []byte) error {
// 	s.syncHashmap(key, false)
// 	res, err := s.implHGetAll(key, match, true, !keysOnly, ts)
// 	if err != nil {
// 		return w.WriteError(err.Error())
// 	}
// 	if !noCompress && s2.SizeOfBulksExceeds(res, s.Config.CompressLimit) {
// 		return w.WriteBulk(s2.CompressBulks(res))
// 	}
// 	return w.WriteBulks(res)
// }

func (s *Server) execSelect(w wire.WriterImpl, key string, start []byte, n int, opts s2.SelectOptions) error {
	origN := n
	if !testFlag {
		// Caller wants N Pairs, we actually fetch more than that to extend the range,
		// and (hopefully) consolidate more Pairs.
		// Special case: n=1 is still n=1.
		n = int(float64(n) * (1 + rand.Float64()))
	}
	data, err := s.implRange(key, start, n, opts)
	if err != nil {
		return w.WriteError(err.Error())
	}
	if !s.HasOtherPeers() || opts.Raw {
		return s.convertPairs(w, data, origN, true)
	}
	if s2.AllPairsConsolidated(data) {
		s.Survey.AllConsolidated.Incr(1)
		return s.convertPairs(w, data, origN, true)
	}

	origData := append([]s2.Pair{}, data...)
	merge := func(async bool) error {
		var lowest []byte
		if opts.Desc && len(data) > 0 && len(data) == n {
			lowest = data[len(data)-1].ID
		}
		packedIDs := s2.PackIDs(data)
		s.Survey.KeyHashRatio.Incr(int64(len(packedIDs) * 1000 / (len(data)*8 + 1)))
		_, success := s.ForeachPeerSendCmd(SendCmdOptions{},
			func() redis.Cmder {
				return redis.NewStringSliceCmd(context.TODO(), "PSELECT", key, start, n, opts.ToInt(), lowest, packedIDs)
			}, func(cmd redis.Cmder) bool {
				for i, v := 0, cmd.(*redis.StringSliceCmd).Val(); i < len(v); i += 2 {
					_ = v[i][15]
					data = append(data, s2.Pair{ID: []byte(v[i]), Data: []byte(v[i+1])})
				}
				return true
			})

		data = sortPairs(data, !opts.Desc)
		if len(data) > n {
			data = data[:n]
		}
		s.setMissing(key, origData, data,
			// Only if we received acknowledgements from all other peers, we can consolidate Pairs.
			success == s.OtherPeersCount(),
			// If iterating in asc order from the beginning, the leftmost Pair is the trusted start of the list.
			*(*string)(unsafe.Pointer(&start)) == minCursor && !opts.Desc,
			// If iterating in desc order and not collecting enough Pairs, the rightmost Pair is the start.
			len(data) < n && opts.Desc,
		)
		if async {
			return nil
		}
		return s.convertPairs(w, data, origN, success == s.OtherPeersCount())
	}

	if opts.Async {
		if s.asyncOnce.Lock(key) {
			s.Survey.AsyncOnce.Incr(s.asyncOnce.Count())
			go func() {
				defer s.asyncOnce.Unlock(key)
				merge(true)
			}()
		}
		return s.convertPairs(w, origData, origN, true)
	}
	return merge(false)
}

func (s *Server) execScan(w wire.WriterImpl, cursor string, count int, local bool) error {
	next, res := s.Scan(cursor, count)
	if !local && s.HasOtherPeers() {
		s.ForeachPeerSendCmd(SendCmdOptions{},
			func() redis.Cmder {
				return redis.NewCmd(context.TODO(), "SCAN", cursor, "COUNT", count, "LOCAL")
			}, func(cmd redis.Cmder) bool {
				v := cmd.(*redis.Cmd).Val().([]any)
				for _, el := range v[1].([]any) {
					if el.(string) <= next || next == "" {
						res = append(res, el.(string))
					}
				}
				return true
			})
		sort.Strings(res)
		for i := len(res) - 1; i >= 1; i-- {
			if res[i] == res[i-1] {
				res = append(res[:i], res[i+1:]...)
			}
		}
		if len(res) >= count+1 {
			next = res[count]
			res = res[:count]
		}
	}
	return w.WriteBulkBulks(next, res)
}

func (s *Server) execLookup(id []byte) (data []byte, err error) {
	data, _, err = s.implLookupID(id)
	if err != nil {
		return nil, err
	}
	if len(data) > 0 || !s.HasOtherPeers() {
		return data, nil
	}
	s.ForeachPeerSendCmd(SendCmdOptions{Oneshot: true}, func() redis.Cmder {
		return redis.NewStringSliceCmd(context.TODO(), "PLOOKUP", id)
	}, func(cmd redis.Cmder) bool {
		m0 := cmd.(*redis.StringSliceCmd).Val()
		if len(m0) == 2 {
			data = []byte(m0[1])
			if err := s.DB.Set(append(kkp(m0[0]), id...), data, pebble.NoSync); err != nil {
				logrus.Errorf("LOOKUP failed to fill hole: %v", err)
			}
			return true
		}
		return false
	})
	return
}
