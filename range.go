package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	log "github.com/sirupsen/logrus"
)

var (
	MinScoreRange = s2pkg.RangeLimit{Float: math.Inf(-1), Inclusive: true}
	MaxScoreRange = s2pkg.RangeLimit{Float: math.Inf(1), Inclusive: true}

	errSafeExit = fmt.Errorf("exit")
)

func (s *Server) ZCount(lex bool, key string, start, end string, flags redisproto.Flags) (int, error) {
	ro := s2pkg.RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		Match:       flags.MATCH,
		Limit:       s2pkg.RangeHardLimit,
	}
	if lex {
		_, c, err := s.runPreparedRangeTx(key, rangeLex(key, s2pkg.NewLexRL(start), s2pkg.NewLexRL(end), ro))
		return c, err
	}
	_, c, err := s.runPreparedRangeTx(key, rangeScore(key, s2pkg.NewScoreRL(start), s2pkg.NewScoreRL(end), ro))
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
	}))
	return p, err
}

func (s *Server) ZRangeByLex(rev bool, key string, start, end string, flags redisproto.Flags) (p []s2pkg.Pair, err error) {
	ro := s2pkg.RangeOptions{
		Rev:   rev,
		Match: flags.MATCH,
	}
	p, err = s.zRangeScoreLex(key, &ro, flags, func() rangeFunc { return rangeLex(key, s2pkg.NewLexRL(start), s2pkg.NewLexRL(end), ro) })
	if flags.WITHDATA && err == nil {
		err = s.fillPairsData(key, p)
	}
	return p, err
}

func (s *Server) ZRangeByScore(rev bool, key string, start, end string, flags redisproto.Flags) (p []s2pkg.Pair, err error) {
	ro := s2pkg.RangeOptions{
		Rev:         rev,
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		Match:       flags.MATCH,
		WithData:    flags.WITHDATA,
	}
	return s.zRangeScoreLex(key, &ro, flags, func() rangeFunc { return rangeScore(key, s2pkg.NewScoreRL(start), s2pkg.NewScoreRL(end), ro) })
}

func (s *Server) zRangeScoreLex(key string, ro *s2pkg.RangeOptions, flags redisproto.Flags, f func() rangeFunc) (p []s2pkg.Pair, err error) {
	if flags.INTERSECT != nil {
		iter := s.DB.NewIter(zsetKeyScoreFullRange)
		defer iter.Close()
		ro.Limit = math.MaxInt64
		ro.Append = s.genIntersectFunc(iter, flags)
	} else if flags.TWOHOPS.ENDPOINT != "" {
		iter := s.DB.NewIter(zsetKeyScoreFullRange)
		defer iter.Close()
		ro.Limit = math.MaxInt64
		ro.Append = s.genTwoHopsFunc(iter, flags)
	} else {
		ro.Limit = flags.LIMIT
		ro.Append = s2pkg.DefaultRangeAppend
	}
	p, _, err = s.runPreparedRangeTx(key, f())
	if err == errSafeExit {
		err = nil
	}
	return p, err
}

func (s *Server) ZRangeByScore2D(rev bool, keys []string, start, end string, flags redisproto.Flags) (p []s2pkg.Pair, err error) {
	ph := &s2pkg.PairHeap{Desc: !rev}
	ro := s2pkg.RangeOptions{
		Rev:         rev,
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		Match:       flags.MATCH,
		WithData:    flags.WITHDATA,
		Limit:       flags.LIMIT,
		Append: func(pairs *[]s2pkg.Pair, p s2pkg.Pair) bool {
			*pairs = append(*pairs, p)
			heap.Push(ph, p)
			if ph.Len() > flags.LIMIT {
				heap.Pop(ph)
			}
			return true
		},
	}

	for _, key := range keys {
		_, _, err := s.runPreparedRangeTx(key, rangeScore(key, s2pkg.NewScoreRL(start), s2pkg.NewScoreRL(end), ro))
		if err != errSafeExit && err != nil {
			return nil, err
		}
	}
	return ph.ToPairs(ro.Limit, true), nil
}

func rangeLex(key string, start, end s2pkg.RangeLimit, opt s2pkg.RangeOptions) rangeFunc {
	return func(tx s2pkg.LogTx) (pairs []s2pkg.Pair, count int, err error) {
		bkName, _, _ := getZSetRangeKey(key)
		c := tx.NewIter(&pebble.IterOptions{
			LowerBound: bkName,
			UpperBound: s2pkg.IncBytes(bkName),
		})
		defer c.Close()

		do := func(k, sc []byte) error {
			if opt.Match != "" && !s2pkg.MatchBinary(opt.Match, k) {
				return nil
			}

			p := s2pkg.Pair{Member: string(k), Score: s2pkg.BytesToFloat(sc)}
			count++
			if opt.Append != nil && !opt.Append(&pairs, p) {
				return errSafeExit
			}
			return nil
		}

		startBuf, endBuf := append(s2pkg.Bytes(bkName), start.Value...), append(s2pkg.Bytes(bkName), end.Value...)
		if opt.Rev {
			if end.LexPlus {
				goto EXIT
			}
			if start.LexPlus {
				c.Last()
			} else {
				if start.Inclusive {
					startBuf = append(startBuf, 0)
				}
				c.SeekLT(startBuf)
			}
			for ; c.Valid() && len(pairs) < opt.Limit; c.Prev() {
				if end.Inclusive && bytes.Compare(c.Key(), endBuf) < 0 {
					break
				}
				if !end.Inclusive && bytes.Compare(c.Key(), endBuf) <= 0 {
					break
				}
				if err := do(c.Key()[len(bkName):], c.Value()); err != nil {
					return pairs, 0, err
				}
			}
		} else {
			if start.LexPlus {
				goto EXIT
			}
			if !start.Inclusive {
				startBuf = append(startBuf, 0)
			}
			c.SeekGE(startBuf)
			for ; c.Valid() && len(pairs) < opt.Limit; c.Next() {
				if !end.LexPlus {
					if end.Inclusive && bytes.Compare(c.Key(), endBuf) > 0 {
						break
					}
					if !end.Inclusive && bytes.Compare(c.Key(), endBuf) >= 0 {
						break
					}
				}
				if err := do(c.Key()[len(bkName):], c.Value()); err != nil {
					return pairs, 0, err
				}
			}
		}
	EXIT:
		if len(opt.DeleteLog) > 0 {
			return pairs, count, deletePair(tx, key, pairs, opt.DeleteLog)
		}
		return pairs, count, nil
	}
}

func rangeScore(key string, start, end s2pkg.RangeLimit, opt s2pkg.RangeOptions) rangeFunc {
	return func(tx s2pkg.LogTx) (pairs []s2pkg.Pair, count int, err error) {
		_, bkScore, bkCounter := getZSetRangeKey(key)
		c := tx.NewIter(&pebble.IterOptions{
			LowerBound: bkScore,
			UpperBound: s2pkg.IncBytes(bkScore),
		})
		defer c.Close()

		_, n, _, err := s2pkg.GetKeyNumber(tx, bkCounter)
		if err != nil {
			return nil, 0, err
		}
		if opt.OffsetStart < 0 {
			opt.OffsetStart += int(n)
		}
		if opt.OffsetEnd < 0 {
			opt.OffsetEnd += int(n)
		}

		do := func(k, dataBuf []byte) error {
			k = k[len(bkScore):]
			key := string(k[8:])
			if opt.Match != "" {
				if !s2pkg.Match(opt.Match, key) && !s2pkg.MatchBinary(opt.Match, dataBuf) {
					return nil
				}
			}
			p := s2pkg.Pair{Member: key, Score: s2pkg.BytesToFloat(k[:])}
			if opt.WithData {
				p.Data = s2pkg.Bytes(dataBuf)
			}
			count++
			if opt.Append != nil && !opt.Append(&pairs, p) {
				return errSafeExit
			}
			return nil
		}

		startBuf := append(s2pkg.Bytes(bkScore), s2pkg.FloatToBytes(start.Float)...)
		endBuf := append(s2pkg.Bytes(bkScore), s2pkg.FloatToBytes(end.Float)...)
		if opt.Rev {
			if start.Inclusive {
				startBuf = s2pkg.IncBytes(startBuf)
			}
			if !end.Inclusive {
				endBuf = s2pkg.IncBytes(endBuf)
			}
			c.SeekLT(startBuf)
			for i := 0; c.Valid() && len(pairs) < opt.Limit && i <= opt.OffsetEnd; i++ {
				if bytes.Compare(c.Key(), endBuf) < 0 {
					break
				}
				if i >= opt.OffsetStart {
					if err := do(c.Key(), c.Value()); err != nil {
						return pairs, 0, err
					}
				}
				c.Prev()
			}
		} else {
			if !start.Inclusive {
				startBuf = s2pkg.IncBytes(startBuf)
			}
			if end.Inclusive {
				endBuf = s2pkg.IncBytes(endBuf)
			}
			c.SeekGE(startBuf)
			for i := 0; c.Valid() && len(pairs) < opt.Limit && i <= opt.OffsetEnd; i++ {
				if bytes.Compare(c.Key(), endBuf) >= 0 {
					break
				}
				if i >= opt.OffsetStart {
					if err := do(c.Key(), c.Value()); err != nil {
						return pairs, 0, err
					}
				}
				c.Next()
			}
		}

		if len(opt.DeleteLog) > 0 {
			return pairs, count, deletePair(tx, key, pairs, opt.DeleteLog)
		}
		return pairs, count, nil
	}
}

func (s *Server) ZRank(rev bool, key, member string, maxMembers int) (rank int, err error) {
	keybuf := []byte(member)
	func() {
		_, bkScore, _ := getZSetRangeKey(key)
		c := s.DB.NewIter(&pebble.IterOptions{
			LowerBound: bkScore,
			UpperBound: s2pkg.IncBytes(bkScore),
		})
		defer c.Close()
		if rev {
			for c.Last(); c.Valid() && bytes.HasPrefix(c.Key(), bkScore); c.Prev() {
				if bytes.Equal(c.Key()[len(bkScore)+8:], keybuf) || rank == maxMembers+1 {
					return
				}
				rank++
			}
		} else {
			for c.First(); c.Valid() && bytes.HasPrefix(c.Key(), bkScore); c.Next() {
				if bytes.Equal(c.Key()[len(bkScore)+8:], keybuf) || rank == maxMembers+1 {
					return
				}
				rank++
			}
		}
		rank = -1
	}()
	if rank == maxMembers+1 {
		rank = -1
	}
	return
}

func (s *Server) Foreach(cursor string, f func(string) bool) {
	opts := &pebble.IterOptions{}
	opts.LowerBound = []byte("zsetks__" + cursor)
	opts.UpperBound = []byte("zsetks_\xff")
	c := s.DB.NewIter(opts)
	defer c.Close()
	if !c.First() {
		return
	}
	for c.Valid() {
		k := c.Key()[8:]
		key := string(k[:bytes.IndexByte(k, 0)])
		if !f(key) {
			return
		}
		c.SeekGE([]byte("zsetks__" + key + "\x01"))
	}
}

func (s *Server) Scan(cursor string, flags redisproto.Flags) (pairs []s2pkg.Pair, nextCursor string) {
	count, timedout, start := flags.COUNT+1, "", time.Now()
	s.Foreach(cursor, func(k string) bool {
		if flags.MATCH != "" && !s2pkg.Match(flags.MATCH, k) {
			return true
		}
		if time.Since(start) > flags.TIMEOUT {
			timedout = k
			return false
		}
		_, _, bkCounter := getZSetRangeKey(k)
		_, v, _, _ := s2pkg.GetKeyNumber(s.DB, bkCounter)
		pairs = append(pairs, s2pkg.Pair{Member: k, Score: float64(v)})
		return len(pairs) < count
	})
	if timedout != "" {
		return nil, timedout
	}
	if len(pairs) >= count {
		pairs, nextCursor = pairs[:count-1], pairs[count-1].Member
	}
	return
}

func (s *Server) genTwoHopsFunc(iter *pebble.Iterator, flags redisproto.Flags) func(pairs *[]s2pkg.Pair, p s2pkg.Pair) bool {
	ddl := time.Now().Add(flags.TIMEOUT)
	endpoint := flags.TWOHOPS.ENDPOINT
	return func(pairs *[]s2pkg.Pair, p s2pkg.Pair) bool {
		member := p.Member
		if bas.IsCallable(flags.TWOHOPS.KEYMAP) {
			res, err := bas.Call2(flags.TWOHOPS.KEYMAP.Object(), bas.Str(member))
			if err != nil {
				log.Error("TwoHopsFunc: ", member, " error: ", err)
				return false
			}
			member = res.String()
		}
		bkName, _, _ := getZSetRangeKey(member)
		x := append(bkName, endpoint...)
		iter.SeekGE(x)
		if iter.Valid() && bytes.Equal(iter.Key(), x) {
			*pairs = append(*pairs, p)
		}
		return len(*pairs) < flags.LIMIT && time.Now().Before(ddl)
	}
}

func (s *Server) genIntersectFunc(iter *pebble.Iterator, flags redisproto.Flags) func(pairs *[]s2pkg.Pair, p s2pkg.Pair) bool {
	ddl := time.Now().Add(flags.TIMEOUT)
	return func(pairs *[]s2pkg.Pair, p s2pkg.Pair) bool {
		count, hits := 0, 0
		for key, f := range flags.INTERSECT {
			if bas.IsCallable(f.F) {
				res, err := bas.Call2(f.F.Object(), bas.Str(key))
				if err != nil {
					log.Error("IntersectFunc: ", key, " error: ", err)
					return false
				}
				key = res.String()
			}
			bkName, _, _ := getZSetRangeKey(key)
			x := append(bkName, p.Member...)
			iter.SeekGE(x)
			exist := iter.Valid() && bytes.Equal(iter.Key(), x)
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
