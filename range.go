package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"time"
	"unsafe"

	"github.com/coyove/nj/bas"
	"github.com/coyove/nj/typ"
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

var (
	MinScoreRange = s2pkg.RangeLimit{Float: math.Inf(-1), Inclusive: true}
	MaxScoreRange = s2pkg.RangeLimit{Float: math.Inf(1), Inclusive: true}

	errSafeExit = fmt.Errorf("exit")
)

func (s *Server) ZCount(lex bool, key string, start, end string, flags redisproto.Flags) (int, error) {
	onSuccess := func(p []s2pkg.Pair, count int) { s.addStaticCache(key, flags.Command.HashCode(), count) }
	ro := s2pkg.RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		LexMatch:    flags.MATCH,
		Limit:       s2pkg.RangeHardLimit,
	}
	if lex {
		_, c, err := s.runPreparedRangeTx(key, rangeLex(key, s2pkg.NewLexRL(start), s2pkg.NewLexRL(end), ro), onSuccess)
		return c, err
	}
	_, c, err := s.runPreparedRangeTx(key, rangeScore(key, s2pkg.NewScoreRL(start), s2pkg.NewScoreRL(end), ro), onSuccess)
	return c, err
}

func (s *Server) ZRange(rev bool, key string, start, end int, flags redisproto.Flags) ([]s2pkg.Pair, error) {
	rangeStart, rangeEnd := MinScoreRange, MaxScoreRange
	if rev {
		rangeStart, rangeEnd = MaxScoreRange, MinScoreRange
	}
	p, _, err := s.runPreparedRangeTx(key, rangeScore(key, rangeStart, rangeEnd, s2pkg.RangeOptions{
		Rev:         rev,
		OffsetStart: start,
		OffsetEnd:   end,
		Limit:       flags.LIMIT,
		WithData:    flags.WITHDATA,
		Append:      s2pkg.DefaultRangeAppend,
	}), func(p []s2pkg.Pair, count int) { s.addStaticCache(key, flags.Command.HashCode(), p) })
	return p, err
}

func (s *Server) ZRangeByLex(rev bool, key string, start, end string, flags redisproto.Flags) (p []s2pkg.Pair, err error) {
	ro := s2pkg.RangeOptions{
		Rev:      rev,
		LexMatch: flags.MATCH,
	}
	p, err = s.zRangeScoreLex(key, &ro, flags, func() rangeFunc { return rangeLex(key, s2pkg.NewLexRL(start), s2pkg.NewLexRL(end), ro) })
	if flags.WITHDATA && err == nil {
		err = s.fillPairsData(key, p)
	}
	return p, err
}

func (s *Server) ZRangeByScore(rev bool, key string, start, end string, flags redisproto.Flags) (p []s2pkg.Pair, err error) {
	ro := s2pkg.RangeOptions{
		Rev:            rev,
		OffsetStart:    0,
		OffsetEnd:      math.MaxInt64,
		LexMatch:       flags.MATCH,
		ScoreMatchData: flags.MATCHDATA,
		WithData:       flags.WITHDATA,
	}
	return s.zRangeScoreLex(key, &ro, flags, func() rangeFunc { return rangeScore(key, s2pkg.NewScoreRL(start), s2pkg.NewScoreRL(end), ro) })
}

func (s *Server) zRangeScoreLex(key string, ro *s2pkg.RangeOptions, flags redisproto.Flags, f func() rangeFunc) (p []s2pkg.Pair, err error) {
	success := func(p []s2pkg.Pair, count int) { s.addStaticCache(key, flags.Command.HashCode(), p) }
	if flags.INTERSECT != nil {
		bkm, goahead, close := s.prepareIntersectBuckets(flags)
		defer close()
		if len(bkm) == 0 && !goahead { // 'goahead' example: ZRANGEBYSCORE key start end NOTINTERSECT not_existed_key
			return
		}
		ro.Limit = math.MaxInt64
		ro.Append = genIntersectFunc(bkm, flags)
		success = func([]s2pkg.Pair, int) {}
	} else if flags.TWOHOPS.ENDPOINT != "" {
		txs, close := s.openAllTx()
		defer close()
		ro.Limit = math.MaxInt64
		ro.Append = genTwoHopsFunc(s, txs, flags)
		success = func([]s2pkg.Pair, int) {}
	} else if flags.MERGE.ENDPOINTS != nil {
		bks, close := s.prepareMergeBuckets(flags)
		defer close()
		if len(bks) == 0 {
			return
		}
		ro.Limit = math.MaxInt64
		ro.Append = genMergeFunc(bks, flags)
		success = func([]s2pkg.Pair, int) {}
	} else {
		ro.Limit = flags.LIMIT
		ro.Append = s2pkg.DefaultRangeAppend
	}
	p, _, err = s.runPreparedRangeTx(key, f(), success)
	if err == errSafeExit {
		err = nil
	}
	if flags.MERGE.ENDPOINTS != nil && flags.MERGE.TOP > 0 {
		if flags.DESC {
			sort.Slice(p, func(i, j int) bool { return p[i].Score > p[j].Score })
		} else {
			sort.Slice(p, func(i, j int) bool { return p[i].Score < p[j].Score })
		}
		if len(p) > flags.MERGE.TOP {
			p = p[:flags.MERGE.TOP]
		}
	}
	return p, err
}

func rangeLex(key string, start, end s2pkg.RangeLimit, opt s2pkg.RangeOptions) rangeFunc {
	return func(tx *bbolt.Tx) (pairs []s2pkg.Pair, count int, err error) {
		bk := tx.Bucket([]byte("zset." + key))
		if bk == nil {
			return
		}

		c := bk.Cursor()
		do := func(k, sc []byte) error {
			if opt.LexMatch != "" {
				m, err := filepath.Match(opt.LexMatch, string(k))
				if err != nil {
					return err
				}
				if !m {
					return nil
				}
			}

			p := s2pkg.Pair{Member: string(k), Score: s2pkg.BytesToFloat(sc)}
			count++
			if opt.Append != nil && !opt.Append(&pairs, p) {
				return errSafeExit
			}
			return nil
		}

		startBuf, endBuf := []byte(start.Value), []byte(end.Value)
		if opt.Rev {
			endFlag := 0
			if !end.Inclusive {
				endFlag = 1
			}
			if start.Inclusive && !start.LexEnd {
				for i := len(startBuf) - 1; i >= 0; i-- {
					if startBuf[i]++; startBuf[i] <= 255 {
						break
					}
				}
			}
			var k, sc = c.Seek(startBuf)
			if len(k) == 0 {
				k, sc = c.Last()
			} else {
				k, sc = c.Prev()
			}
			for i := 0; len(pairs) < opt.Limit; i++ {
				if len(sc) > 0 && bytes.Compare(k, startBuf) <= 0 && bytes.Compare(k, endBuf) >= endFlag {
					if err := do(k, sc); err != nil {
						return pairs, 0, err
					}
					k, sc = c.Prev()
				} else {
					break
				}
			}
		} else {
			if !start.Inclusive {
				startBuf = append(startBuf, 0)
			}
			endFlag := 0
			if !end.Inclusive {
				endFlag = -1
			}
			k, sc := c.Seek(startBuf)
			for i := 0; len(pairs) < opt.Limit; i++ {
				if len(sc) > 0 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) <= endFlag {
					if err := do(k, sc); err != nil {
						return pairs, 0, err
					}
					k, sc = c.Next()
				} else {
					break
				}
			}
		}

		if len(opt.DeleteLog) > 0 {
			return pairs, count, deletePair(tx, key, pairs, opt.DeleteLog)
		}
		return pairs, count, nil
	}
}

func rangeScore(key string, start, end s2pkg.RangeLimit, opt s2pkg.RangeOptions) rangeFunc {
	return func(tx *bbolt.Tx) (pairs []s2pkg.Pair, count int, err error) {
		bk := tx.Bucket([]byte("zset.score." + key))
		if bk == nil {
			return
		}
		opt.TranslateOffset(key, func() int { return int(sizeOfBucket(bk)) })

		c := bk.Cursor()
		do := func(k, dataBuf []byte) error {
			key := string(k[8:])
			if opt.LexMatch != "" {
				if m, err := filepath.Match(opt.LexMatch, key); err != nil {
					return err
				} else if !m {
					return nil
				}
			}
			if opt.ScoreMatchData != "" {
				if m, err := filepath.Match(opt.ScoreMatchData, key); err != nil {
					return err
				} else if !m {
					if m, err := filepath.Match(opt.ScoreMatchData, *(*string)(unsafe.Pointer(&dataBuf))); err != nil {
						return err
					} else if !m {
						return nil
					}
				}
			}
			p := s2pkg.Pair{Member: key, Score: s2pkg.BytesToFloat(k[:8])}
			if opt.WithData {
				p.Data = append([]byte{}, dataBuf...)
			}
			count++
			if opt.Append != nil && !opt.Append(&pairs, p) {
				return errSafeExit
			}
			return nil
		}

		startInt, endInt := s2pkg.FloatToOrderedUint64(start.Float), s2pkg.FloatToOrderedUint64(end.Float)
		if opt.Rev {
			if start.Inclusive {
				startInt++
			}
			if !end.Inclusive {
				endInt++
			}
			var k, dataBuf = c.Seek(s2pkg.Uint64ToBytes(startInt))
			if len(k) == 0 {
				k, dataBuf = c.Last()
			} else {
				k, dataBuf = c.Prev()
			}
			for i := 0; len(k) >= 8 && len(pairs) < opt.Limit; i++ {
				x := binary.BigEndian.Uint64(k)
				if x <= startInt && x >= endInt {
					if i >= opt.OffsetStart && i <= opt.OffsetEnd {
						if err := do(k, dataBuf); err != nil {
							return pairs, 0, err
						}
					} else if i > opt.OffsetEnd {
						break
					}
					k, dataBuf = c.Prev()
				} else {
					break
				}
			}
		} else {
			if !start.Inclusive {
				startInt++
			}
			if end.Inclusive {
				endInt++
			}
			k, dataBuf := c.Seek(s2pkg.Uint64ToBytes(startInt))
			for i := 0; len(k) >= 8 && len(pairs) < opt.Limit; i++ {
				x := binary.BigEndian.Uint64(k)
				if x >= startInt && x < endInt {
					if i >= opt.OffsetStart && i <= opt.OffsetEnd {
						if err := do(k, dataBuf); err != nil {
							return pairs, 0, err
						}
					} else if i > opt.OffsetEnd {
						break
					}
					k, dataBuf = c.Next()
				} else {
					break
				}
			}
		}

		if len(opt.DeleteLog) > 0 {
			return pairs, count, deletePair(tx, key, pairs, opt.DeleteLog)
		}
		return pairs, count, nil
	}
}

func (s *Server) ZRank(rev bool, key, member string, flags redisproto.Flags) (rank int, err error) {
	rank = -1
	keybuf := []byte(member)
	err = s.pick(key).View(func(tx *bbolt.Tx) error {
		func() {
			bk := tx.Bucket([]byte("zset.score." + key))
			if bk == nil {
				return
			}
			c := bk.Cursor()
			if rev {
				for k, _ := c.Last(); len(k) > 8; k, _ = c.Prev() {
					rank++
					if bytes.Equal(k[8:], keybuf) || rank == flags.COUNT+1 {
						return
					}
				}
			} else {
				for k, _ := c.First(); len(k) > 8; k, _ = c.Next() {
					rank++
					if bytes.Equal(k[8:], keybuf) || rank == flags.COUNT+1 {
						return
					}
				}
			}
			rank = -1
		}()
		if rank == flags.COUNT+1 {
			rank = -1
		}
		s.addStaticCache(key, flags.HashCode(), rank)
		return nil
	})
	return
}

func (s *Server) Foreach(cursor string, shard int, f func(k string, bk *bbolt.Bucket) bool) error {
	startShard := 0
	if cursor != "" {
		startShard = shardIndex(cursor)
	}
	if shard >= 0 {
		startShard = shard
	}

	for ; startShard < ShardNum; startShard++ {
		err := s.db[startShard].View(func(tx *bbolt.Tx) error {
			c := tx.Cursor()
			k, _ := c.First()
			if cursor != "" {
				k, _ = c.Seek([]byte("q." + cursor)) // 'q.' comes first, then 'zset.'
			}

			for ; len(k) > 0; k, _ = c.Next() {
				var key []byte
				if bytes.HasPrefix(k, []byte("zset.score")) {
					continue
				} else if bytes.HasPrefix(k, []byte("zset.")) {
					key = k[5:]
				} else if bytes.HasPrefix(k, []byte("q.")) {
					key = k[2:]
				} else {
					continue
				}
				if !f(string(key), tx.Bucket(k)) {
					return errSafeExit
				}
			}
			return nil
		})
		if err == errSafeExit {
			return nil
		} else if err != nil {
			return err
		}
		cursor = ""
		if shard >= 0 {
			break
		}
	}
	return nil
}

func (s *Server) Scan(cursor string, match string, shard int, count int) (pairs []s2pkg.Pair, nextCursor string, err error) {
	count++
	if err := s.Foreach(cursor, shard, func(k string, bk *bbolt.Bucket) bool {
		if match != "" {
			if m, _ := filepath.Match(match, k); !m {
				return true
			}
		}
		pairs = append(pairs, s2pkg.Pair{
			Member: k,
			Score:  float64(sizeOfBucket(bk)),
		})
		return len(pairs) < count
	}); err != nil {
		return nil, "", err
	}
	if len(pairs) >= count {
		pairs, nextCursor = pairs[:count-1], pairs[count-1].Member
	}
	return
}

func (s *Server) prepareIntersectBuckets(flags redisproto.Flags) (bkm map[*bbolt.Bucket]redisproto.IntersectFlags, goahead bool, close func()) {
	bkm = map[*bbolt.Bucket]redisproto.IntersectFlags{}
	var txs []*bbolt.Tx
	for k, f := range flags.INTERSECT {
		tx, err := s.pick(k).Begin(false)
		if err != nil {
			log.Errorf("prepareIntersect(%s): %v", k, err)
			continue
		}
		bk := tx.Bucket([]byte("zset." + k))
		txs = append(txs, tx)
		if bk == nil {
			if f.Not {
				// Must not intersect, since key doesn't exist, there is no need to check it
				tx.Rollback()
				txs = txs[:len(txs)-1]
				goahead = true
				continue
			} else {
				// Must intersect, but key doesn't exist, so must fail
				closeAllReadTxs(txs)
				return nil, false, func() {}
			}
		}
		bkm[bk] = f
	}
	return bkm, goahead, func() { closeAllReadTxs(txs) }
}

func (s *Server) prepareMergeBuckets(flags redisproto.Flags) (bkm []*bbolt.Bucket, close func()) {
	var txs []*bbolt.Tx
	for _, k := range flags.MERGE.ENDPOINTS {
		tx, err := s.pick(k).Begin(false)
		if err != nil {
			log.Errorf("prepareMergeBuckets(%s): %v", k, err)
			continue
		}
		bk := tx.Bucket([]byte("zset." + k))
		if bk == nil {
			tx.Rollback()
			continue
		}
		txs = append(txs, tx)
		bkm = append(bkm, bk)
	}
	return bkm, func() { closeAllReadTxs(txs) }
}

func (s *Server) openAllTx() (txs [ShardNum]*bbolt.Tx, close func()) {
	for i := range s.db {
		tx, err := s.db[i].Begin(false)
		if err != nil {
			log.Errorf("openAllTx(%d): %v", i, err)
			continue
		}
		txs[i] = tx
	}
	return txs, func() { closeAllReadTxs(txs[:]) }
}

func genTwoHopsFunc(s *Server, txs [ShardNum]*bbolt.Tx, flags redisproto.Flags) func(pairs *[]s2pkg.Pair, p s2pkg.Pair) bool {
	ddl := time.Now().Add(flags.TIMEOUT)
	endpoint := []byte(flags.TWOHOPS.ENDPOINT)
	return func(pairs *[]s2pkg.Pair, p s2pkg.Pair) bool {
		key := p.Member
		if bas.IsCallable(flags.TWOHOPS.KEYMAP) {
			res, err := bas.Call2(flags.TWOHOPS.KEYMAP.Object(), bas.Str(key))
			if err != nil {
				log.Error("TwoHopsFunc: ", key, " error: ", err)
				return false
			}
			key = res.String()
		}
		if bk := txs[shardIndex(key)].Bucket([]byte("zset." + key)); bk != nil {
			if len(bk.Get(endpoint)) > 0 {
				*pairs = append(*pairs, p)
			}
		}
		return len(*pairs) < flags.LIMIT && time.Now().Before(ddl)
	}
}

func genIntersectFunc(bkm map[*bbolt.Bucket]redisproto.IntersectFlags, flags redisproto.Flags) func(pairs *[]s2pkg.Pair, p s2pkg.Pair) bool {
	ddl := time.Now().Add(flags.TIMEOUT)
	return func(pairs *[]s2pkg.Pair, p s2pkg.Pair) bool {
		key := p.Member
		count, hits := 0, 0
		for bk, f := range bkm {
			if bas.IsCallable(f.F) {
				res, err := bas.Call2(f.F.Object(), bas.Str(key))
				if err != nil {
					log.Error("IntersectFunc: ", key, " error: ", err)
					return false
				}
				key = res.String()
			}

			exist := len(bk.Get([]byte(key))) > 0
			if f.Not {
				if exist {
					goto OUT
				}
			} else {
				if exist {
					hits++
				}
				count++
			}
		}
		if hits == count {
			*pairs = append(*pairs, p)
		}
	OUT:
		return len(*pairs) < flags.LIMIT && time.Now().Before(ddl)
	}
}

func genMergeFunc(bkm []*bbolt.Bucket, flags redisproto.Flags) func(pairs *[]s2pkg.Pair, p s2pkg.Pair) bool {
	ddl := time.Now().Add(flags.TIMEOUT)
	f := flags.MERGE.FUNC
	args := []bas.Value{bas.Nil, bas.NewArray(make([]bas.Value, 1+len(bkm))...).ToValue()}
	return func(pairs *[]s2pkg.Pair, p s2pkg.Pair) bool {
		key := []byte(p.Member)
		if bas.IsCallable(f) {
			args[0], args[1].Array().Values()[0] = bas.UnsafeStr(key), bas.Float64(p.Score)
			for i, bk := range bkm {
				args[1].Array().Values()[i+1] = bas.Float64(s2pkg.BytesToFloatZero(bk.Get(key)))
			}
			res, err := bas.Call2(f.Object(), args...)
			if err != nil {
				log.Error("MergeFunc: ", key, " error: ", err)
				return false
			} else if res.Type() != typ.Number {
				log.Error("MergeFunc: ", key, " should return numbers")
				return false
			}
			p.Score = res.Float64()
			*pairs = append(*pairs, p)
		} else {
			for _, bk := range bkm {
				p.Score += s2pkg.BytesToFloatZero(bk.Get(key))
			}
			*pairs = append(*pairs, p)
		}
		return len(*pairs) < flags.LIMIT && time.Now().Before(ddl)
	}
}
