package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/ranges"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
)

func (s *Server) ZCount(lex bool, key string, start, end string, flags wire.Flags) (int, error) {
	ro := ranges.Options{
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		Match:       flags.MATCH,
		Limit:       ranges.HardLimit,
		Append:      func(rr *ranges.Result, p s2pkg.Pair) error { rr.Count++; return nil },
	}
	if lex {
		c, err := s.runPreparedRangeTx(key, rangeLex(key, ranges.Lex(start), ranges.Lex(end), ro))
		return c.Count, err
	}
	c, err := s.runPreparedRangeTx(key, rangeScore(key, ranges.Score(start), ranges.Score(end), ro))
	return c.Count, err
}

func (s *Server) ZRange(rev bool, key string, start, end int, flags wire.Flags) ([]s2pkg.Pair, error) {
	rangeStart, rangeEnd := ranges.MinScoreRange, ranges.MaxScoreRange
	if rev {
		rangeStart, rangeEnd = ranges.MaxScoreRange, ranges.MinScoreRange
	}
	p, err := s.runPreparedRangeTx(key, rangeScore(key, rangeStart, rangeEnd, ranges.Options{
		Rev:         rev,
		OffsetStart: start,
		OffsetEnd:   end,
		Limit:       flags.LIMIT,
		WithData:    flags.WITHDATA,
		Append:      ranges.DefaultAppend,
	}))
	return p.Pairs, err
}

func (s *Server) ZRangeByLex(rev bool, key string, start, end string, flags wire.Flags) ([]s2pkg.Pair, error) {
	ro, closer := s.createRangeOptions(ranges.Options{
		Rev:      rev,
		Match:    flags.MATCH,
		WithData: flags.WITHDATA,
	}, flags)
	defer closer()
	c, err := s.runPreparedRangeTx(key, rangeLex(key, ranges.Lex(start), ranges.Lex(end), ro))
	return c.Pairs, err
}

func (s *Server) ZRangeByScore(rev bool, key string, start, end string, flags wire.Flags) ([]s2pkg.Pair, error) {
	ro, closer := s.createRangeOptions(ranges.Options{
		Rev:         rev,
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		Match:       flags.MATCH,
		WithData:    flags.WITHDATA,
	}, flags)
	defer closer()
	c, err := s.runPreparedRangeTx(key, rangeScore(key, ranges.Score(start), ranges.Score(end), ro))
	return c.Pairs, err
}

func (s *Server) createRangeOptions(ro ranges.Options, flags wire.Flags) (_ ranges.Options, closer func() error) {
	if flags.INTERSECT != nil {
		ro.Limit = math.MaxInt64
		ro.Append, closer = s.makeIntersect(flags)
	} else if flags.TWOHOPS.ENDPOINT != "" {
		ro.Limit = math.MaxInt64
		ro.Append, closer = s.makeTwoHops(flags)
	} else {
		ro.Limit = flags.LIMIT
		ro.Append, closer = ranges.DefaultAppend, func() error { return nil }
	}
	return ro, closer
}

func (s *Server) ZRangeByScore2D(rev bool, keys []string, start, end string, flags wire.Flags) ([]s2pkg.Pair, error) {
	ph := &s2pkg.PairHeap{Desc: !rev}
	ro := ranges.Options{
		Rev:         rev,
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		Match:       flags.MATCH,
		WithData:    flags.WITHDATA,
		Limit:       flags.LIMIT,
		Append: func(r *ranges.Result, p s2pkg.Pair) error {
			r.Count++
			heap.Push(ph, p)
			if ph.Len() > flags.LIMIT {
				heap.Pop(ph)
			}
			return nil
		},
	}

	for _, key := range keys {
		_, err := s.runPreparedRangeTx(key, rangeScore(key, ranges.Score(start), ranges.Score(end), ro))
		if err != nil {
			return nil, err
		}
	}
	return ph.ToPairs(ro.Limit, true), nil
}

func rangeLex(key string, start, end ranges.Limit, opt ranges.Options) rangeFunc {
	return func(tx extdb.LogTx) (rr ranges.Result, err error) {
		bkName, bkScore, _ := ranges.GetZSetRangeKey(key)
		c := tx.NewIter(&pebble.IterOptions{
			LowerBound: bkName,
			UpperBound: s2pkg.IncBytes(bkName),
		})
		defer c.Close()

		process := func(keyBuf, scoreBuf []byte) error {
			if opt.Match != "" && !s2pkg.MatchBinary(opt.Match, keyBuf) {
				return nil
			}
			p := s2pkg.Pair{Member: string(keyBuf), Score: s2pkg.BytesToFloat(scoreBuf)}
			if opt.WithData {
				var e error
				p.Data, e = extdb.GetKey(tx, append(append(bkScore, scoreBuf...), keyBuf...))
				if e != nil {
					return e
				}
			}
			return opt.Append(&rr, p)
		}

		startBuf := append(s2pkg.Bytes(bkName), start.LexValue...)
		endBuf := append(s2pkg.Bytes(bkName), end.LexValue...)
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
			for ; c.Valid() && rr.Count < opt.Limit; c.Prev() {
				if end.Inclusive && bytes.Compare(c.Key(), endBuf) < 0 {
					break
				}
				if !end.Inclusive && bytes.Compare(c.Key(), endBuf) <= 0 {
					break
				}
				if err := process(c.Key()[len(bkName):], c.Value()); err != nil {
					return rr, err
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
			for ; c.Valid() && rr.Count < opt.Limit; c.Next() {
				if !end.LexPlus {
					if end.Inclusive && bytes.Compare(c.Key(), endBuf) > 0 {
						break
					}
					if !end.Inclusive && bytes.Compare(c.Key(), endBuf) >= 0 {
						break
					}
				}
				if err := process(c.Key()[len(bkName):], c.Value()); err != nil {
					return rr, err
				}
			}
		}
	EXIT:
		if len(opt.DeleteLog) > 0 {
			return rr, deletePair(tx, key, rr.Pairs, opt.DeleteLog)
		}
		return rr, nil
	}
}

func rangeScore(key string, start, end ranges.Limit, opt ranges.Options) rangeFunc {
	return func(tx extdb.LogTx) (rr ranges.Result, err error) {
		_, bkScore, bkCounter := ranges.GetZSetRangeKey(key)
		c := tx.NewIter(&pebble.IterOptions{
			LowerBound: bkScore,
			UpperBound: s2pkg.IncBytes(bkScore),
		})
		defer c.Close()

		_, n, _, err := extdb.GetKeyNumber(tx, bkCounter)
		if err != nil {
			return rr, err
		}
		if opt.OffsetStart < 0 {
			opt.OffsetStart += int(n)
		}
		if opt.OffsetEnd < 0 {
			opt.OffsetEnd += int(n)
		}

		process := func(k, dataBuf []byte) error {
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
			return opt.Append(&rr, p)
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
			for i := 0; c.Valid() && rr.Count < opt.Limit && i <= opt.OffsetEnd; i++ {
				if bytes.Compare(c.Key(), endBuf) < 0 {
					break
				}
				if i >= opt.OffsetStart {
					if err := process(c.Key(), c.Value()); err != nil {
						return rr, err
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
			for i := 0; c.Valid() && rr.Count < opt.Limit && i <= opt.OffsetEnd; i++ {
				if bytes.Compare(c.Key(), endBuf) >= 0 {
					break
				}
				if i >= opt.OffsetStart {
					if err := process(c.Key(), c.Value()); err != nil {
						return rr, err
					}
				}
				c.Next()
			}
		}

		if len(opt.DeleteLog) > 0 {
			return rr, deletePair(tx, key, rr.Pairs, opt.DeleteLog)
		}
		return rr, nil
	}
}

func (s *Server) ZRank(rev bool, key, member string, maxMembers int) (rank int, err error) {
	keybuf := []byte(member)
	func() {
		_, bkScore, _ := ranges.GetZSetRangeKey(key)
		c := ranges.NewPrefixIter(s.DB, bkScore)
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

func (s *Server) Scan(cursor string, flags wire.Flags) (pairs []s2pkg.Pair, nextCursor string) {
	count, timedout, start := flags.COUNT+1, "", time.Now()
	s.Foreach(cursor, func(k string) bool {
		if flags.MATCH != "" && !s2pkg.Match(flags.MATCH, k) {
			return true
		}
		if time.Since(start) > flags.TIMEOUT {
			timedout = k
			return false
		}
		_, v, _, _ := extdb.GetKeyNumber(s.DB, ranges.GetZSetCounterKey(k))
		pairs = append(pairs, s2pkg.Pair{Member: k, Score: float64(v)})
		return len(pairs) < count
	})
	if timedout != "" {
		return pairs, timedout
	}
	if len(pairs) >= count {
		pairs, nextCursor = pairs[:count-1], pairs[count-1].Member
	}
	return
}

func (s *Server) makeTwoHops(flags wire.Flags) (func(*ranges.Result, s2pkg.Pair) error, func() error) {
	iter := s.DB.NewIter(ranges.ZSetKeyScoreFullRange)
	ddl := time.Now().Add(flags.TIMEOUT)
	endpoint := flags.TWOHOPS.ENDPOINT
	return func(r *ranges.Result, p s2pkg.Pair) error {
		member := p.Member
		if bas.IsCallable(flags.TWOHOPS.KEYMAP) {
			res, err := bas.Call2(flags.TWOHOPS.KEYMAP.Object(), bas.Str(member))
			if err != nil {
				return fmt.Errorf("TwoHopsFunc %q: %v", member, err)
			}
			member = res.String()
		}
		bkName, _, _ := ranges.GetZSetRangeKey(member)
		x := append(bkName, endpoint...)
		iter.SeekGE(x)
		if iter.Valid() && bytes.Equal(iter.Key(), x) {
			r.Count++
			r.Pairs = append(r.Pairs, p)
		}
		if r.Count < flags.LIMIT && time.Now().Before(ddl) {
			return nil
		}
		return ranges.ErrAppendSafeExit
	}, iter.Close
}

func (s *Server) makeIntersect(flags wire.Flags) (func(r *ranges.Result, p s2pkg.Pair) error, func() error) {
	iter := s.DB.NewIter(ranges.ZSetKeyScoreFullRange)
	ddl := time.Now().Add(flags.TIMEOUT)
	return func(r *ranges.Result, p s2pkg.Pair) error {
		count, hits := 0, 0
		for key, f := range flags.INTERSECT {
			if bas.IsCallable(f.F) {
				res, err := bas.Call2(f.F.Object(), bas.Str(key))
				if err != nil {
					return fmt.Errorf("IntersectFunc %q: %v", key, err)
				}
				key = res.String()
			}
			bkName, _, _ := ranges.GetZSetRangeKey(key)
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
			r.Count++
			r.Pairs = append(r.Pairs, p)
		}
	OUT:
		if r.Count < flags.LIMIT && time.Now().Before(ddl) {
			return nil
		}
		return ranges.ErrAppendSafeExit
	}, iter.Close
}
