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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/s2/filelock"
	"github.com/coyove/s2db/s2/resp"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
	client "github.com/influxdata/influxdb1-client"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

var Version string

type Server struct {
	Interop   interop
	DB        *pebble.DB
	Peers     [future.Channels]*endpoint
	DBPath    string
	DBOptions *pebble.Options
	Channel   int64
	ReadOnly  bool
	Closed    bool // server close flag
	Config    ServerConfig
	Survey    ServerSurvey

	lnRESP       net.Listener
	lnHTTP       net.Listener
	dieLock      sync.Mutex
	dumpWireLock sync.Mutex
	rdbCache     *s2.LRUCache[string, *redis.Client]
	fillCache    *s2.LRUShard[struct{}]
	wmCache      *s2.LRUShard[[16]byte]
	asyncOnce    s2.KeyLock
	errThrot     s2.ErrorThrottler
	copying      sync.Map
	dbFile       *os.File
	pipeline     chan *dbPayload
	influxdb1    struct {
		*client.Client
		URI      string
		Database string
	}

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

	parser := resp.NewParser(conn)
	writer := resp.NewWriter(conn)
	var ew error
	for auth := false; ; {
		command, err := parser.ReadCommand()
		if err != nil {
			_, ok := err.(*resp.ProtocolError)
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
					writer.WriteError(s2.ErrNoAuth.Error())
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

func (s *Server) runCommand(startTime time.Time, cmd string, w resp.WriterImpl, src net.IP, K *resp.Command) (outErr error) {
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
	}, nil)

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
			found, err := s.UpdateConfig(K.Str(2), K.Str(3))
			if err != nil {
				return w.WriteError(err.Error())
			} else if found {
				return w.WriteSimpleString("OK")
			}
			return w.WriteError("field not found")
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
		return slowLogger.Formatter.(*logf).LogFork(w.(*resp.Writer).Sink.(net.Conn))
	case "DB.LOG":
		return dbLogger.Formatter.(*logf).LogFork(w.(*resp.Writer).Sink.(net.Conn))
	case "RUNTIME.LOG":
		return log.StandardLogger().Formatter.(*logf).LogFork(w.(*resp.Writer).Sink.(net.Conn))
	}

	switch cmd {
	case "APPEND": // key data_0 [DP dp_len] [SYNC] [NOEXP] [[AND data_1] ...] [SETID ...]
		data, ids, opts := K.GetAppendOptions()
		ids, err := s.execAppend(key, ids, data, opts)
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteBulks(s2.HexEncodeBulks(ids))
	case "PSELECT": // key raw_start count flag desc_lowest packed_ids => [ID 0, DATA 0, ...]
		start, flag, contains := K.BytesRef(2), s2.ParseSelectOptions(K.Int64(4)), s2.UnpackPairIDs(K.BytesRef(6))
		flag.NoData = false // data must be exchanged among peers to meet consolidation

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
	case "SELECT": // key start count [ASC|DESC] [ASYNC] [RAW] [UNION key_2 [UNION ...]] => [ID 0, TIME 0, DATA 0, ID 1, TIME 1, DATA 1 ... ]
		n, flag := K.GetSelectOptions()
		start := s.translateCursor(K.BytesRef(2), flag.Desc)
		data, err := s.execSelect(key, start, n, flag)
		if err != nil {
			return w.WriteError(err.Error())
		}
		return s.convertPairs(w, data, n)
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
	case "COUNT": // key start end [max]
		start, end := s.translateCursor(K.BytesRef(2), false), s.translateCursor(K.BytesRef(3), false)
		max := 0
		if K.ArgCount() >= 5 {
			max = K.Int(4)
		}
		count, err := s.execCount(key, start, end, max)
		if err != nil {
			return w.WriteError(err.Error())
		}
		return w.WriteInt64(int64(count))
	case "SCAN": // cursor [COUNT count] [INDEX] [LOCAL]
		index, local, count := K.GetScanOptions()
		if index {
			return w.WriteBulkBulks(s.ScanLookupIndex(K.StrRef(1), count))
		}
		return w.WriteBulkBulks(s.execScan(K.StrRef(1), count, local))
	case "WAITEFFECT":
		x := K.BytesRef(1)
		for i := 2; i < K.ArgCount(); i++ {
			if bytes.Compare(K.BytesRef(i), x) > 0 {
				x = K.BytesRef(i)
			}
		}
		future.Future(binary.BigEndian.Uint64(s2.HexDecode(x))).Wait()
		return w.WriteSimpleString("OK")
	}

	return w.WriteError(s2.ErrUnknownCommand.Error())
}

func (s *Server) execAppend(key string, ids, data [][]byte, opts s2.AppendOptions) ([][]byte, error) {
	ids, maxID, err := s.implAppend(key, ids, data, opts)
	if err != nil {
		return nil, err
	}
	if !opts.NoSync && s.HasOtherPeers() {
		args := []any{"APPEND", key, data[0]}
		if opts.Defer {
			args = append(args, "DEFER")
		}
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
	return ids, nil
}

func (s *Server) execSelect(key string, start []byte, n int, opts s2.SelectOptions) ([]s2.Pair, error) {
	if len(opts.Unions) > 0 {
		return s.selectUnions(append(opts.Unions, key), start, n, opts)
	}

	markPairsAll := func(data []s2.Pair) []s2.Pair {
		for i := range data {
			data[i].All = true
		}
		return data
	}

	if !testFlag {
		// Caller wants N Pairs, we actually fetch more than that to extend the range,
		// and (hopefully) consolidate more Pairs.
		// Special case: n=1 is still n=1.
		n = int(float64(n) * (1 + rand.Float64()))
	}
	data, err := s.implRange(key, start, n, opts)
	if err != nil {
		return nil, err
	}
	if !s.HasOtherPeers() || opts.Raw {
		return markPairsAll(data), nil
	}
	if s2.AllPairsConsolidated(data) {
		s.Survey.AllConsolidated.Incr(1)
		return markPairsAll(data), nil
	}

	origData := append([]s2.Pair{}, data...)
	merge := func(async bool) []s2.Pair {
		var lowest []byte
		if opts.Desc && len(data) > 0 && len(data) == n {
			lowest = data[len(data)-1].ID
		}
		packedIDs := s2.PackPairIDs(data)
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

		data = s2.SortPairs(data, !opts.Desc)
		if len(data) > n {
			data = data[:n]
		}
		if async {
			return nil
		}
		s.fillHoles(key, origData, data,
			// Only if we received acknowledgements from all other peers, we can consolidate Pairs.
			success == s.OtherPeersCount(),
			// If iterating in asc order from the beginning, the leftmost Pair is the trusted start of the list.
			*(*string)(unsafe.Pointer(&start)) == minCursor && !opts.Desc,
			// If iterating in desc order and not collecting enough Pairs, the rightmost Pair is the start.
			len(data) < n && opts.Desc,
		)
		if success == s.OtherPeersCount() {
			data = markPairsAll(data)
		}
		return data
	}

	if opts.Async {
		if s.asyncOnce.Lock(key) {
			s.Survey.AsyncOnce.Incr(s.asyncOnce.Count())
			go func() {
				defer s.asyncOnce.Unlock(key)
				merge(true)
			}()
		}
		return origData, nil // s.convertPairs(w, origData, origN, true)
	}
	return merge(false), nil
}

func (s *Server) selectUnions(keys []string, start []byte, n int, opts s2.SelectOptions) ([]s2.Pair, error) {
	opts.Unions = nil
	var (
		mu     sync.Mutex
		out    []s2.Pair
		outErr atomic.Value
		wg     sync.WaitGroup
	)
	for i, u := range keys {
		wg.Add(1)
		go func(i int, key string) {
			defer wg.Done()
			if data, err := s.execSelect(key, start, n, opts); err != nil {
				outErr.Store(err)
			} else {
				mu.Lock()
				out = append(out, data...)
				mu.Unlock()
			}
		}(i, u)
	}
	wg.Wait()

	if err, ok := outErr.Load().(error); ok {
		return nil, err
	}

	out = s2.SortPairs(out, !opts.Desc)
	if len(out) > n {
		out = out[:n]
	}
	return out, nil
}

func (s *Server) execScan(cursor string, count int, local bool) (string, []string) {
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
	return next, res
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

func (s *Server) execCount(key string, start, end []byte, max int) (int, error) {
	if bytes.Compare(start, end) > 0 {
		start, end = end, start
	}

	origStart := start

	N := 1000
	if max > 0 && max < N {
		N = max
	}

	so := s2.SelectOptions{NoData: true}
	var count int

	ddl := future.UnixNano() + int64(s.Config.TimeoutCount)*1e6
	for future.UnixNano() < ddl {
		data, err := s.execSelect(key, start, N, so)
		if err != nil {
			return 0, err
		}
		for _, p := range data {
			if bytes.Compare(p.ID, end) > 0 {
				return count, nil
			}
			count++
			start = p.ID
		}
		if max > 0 && count >= max {
			return count, nil
		}
		if len(data) < N {
			return count, nil
		}
		so.LeftOpen = true
	}

	// Since count timed out, let's estimate.

	known, err := s.DB.EstimateDiskUsage(append(kkp(key), origStart...), append(kkp(key), start...))
	if err != nil {
		return 0, err
	}

	full, err := s.DB.EstimateDiskUsage(append(kkp(key), origStart...), append(kkp(key), end...))
	if err != nil {
		return 0, err
	}

	count = int(float64(count) / float64(known+1) * float64(full+1))
	count = (count/N + 1) * N
	return count, nil
}
