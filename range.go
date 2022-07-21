package main

import (
	"bytes"
	"compress/gzip"
	"container/heap"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/bitmap"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/ranges"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
	log "github.com/sirupsen/logrus"
)

func (s *Server) ZCount(lex bool, key string, start, end string, flags wire.Flags) (int, error) {
	ro := ranges.Options{
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		Match:       flags.Match,
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
		Limit:       flags.Limit,
		WithData:    flags.WithData,
		Append:      ranges.DefaultAppend,
	}))
	return p.Pairs, err
}

func (s *Server) ZRangeByLex(rev bool, key string, start, end string, flags wire.Flags) ([]s2pkg.Pair, error) {
	ro, closer := s.createRangeOptions(ranges.Options{
		Rev:      rev,
		Match:    flags.Match,
		WithData: flags.WithData,
	}, flags)
	defer closer()
	c, err := s.runPreparedRangeTx(key, rangeLex(key, ranges.Lex(start), ranges.Lex(end), ro))
	c.BFToFakePair()
	return c.Pairs, err
}

func (s *Server) ZRangeByScore(rev bool, key string, start, end string, flags wire.Flags) ([]s2pkg.Pair, error) {
	ro, closer := s.createRangeOptions(ranges.Options{
		Rev:         rev,
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		Match:       flags.Match,
		WithData:    flags.WithData,
		ILimit:      flags.ILimit,
	}, flags)
	defer closer()
	c, err := s.runPreparedRangeTx(key, rangeScore(key, ranges.Score(start), ranges.Score(end), ro))
	c.BFToFakePair()
	return c.Pairs, err
}

func (s *Server) ZRangeRangeByScore(rev bool, key string, start, end, start2, end2 string,
	flags wire.Flags) ([]s2pkg.Pair, error) {
	s2, e2 := ranges.Score(start2), ranges.Score(end2)
	a, closer := s.makeFanoutScore(s2, e2, flags)
	ro := ranges.Options{
		Rev:         rev,
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		Match:       flags.Match,
		WithData:    flags.WithData,
		Limit:       flags.Limit,
		ILimit:      flags.ILimit,
		FanoutStart: s2,
		FanoutEnd:   e2,
		Append:      a,
	}
	defer closer()
	c, err := s.runPreparedRangeTx(key, rangeScore(key, ranges.Score(start), ranges.Score(end), ro))
	return c.Pairs, err
}

func (s *Server) createRangeOptions(ro ranges.Options, flags wire.Flags) (_ ranges.Options, closer func() error) {
	if !math.IsNaN(flags.MemberBF) {
		ro.Limit = flags.Limit
		ro.Append, closer = func(r *ranges.Result, p s2pkg.Pair) error {
			if r.Bloom == nil {
				r.Bloom = bitmap.NewBloomFilter(int(ro.Limit), flags.MemberBF)
			}
			r.Bloom.Add(p.Member)
			r.Count++
			if len(r.Pairs) == 0 {
				r.Pairs = []s2pkg.Pair{p, p}
			} else {
				r.Pairs[1] = p
			}
			return nil
		}, func() error { return nil }
	} else if flags.Intersect != nil {
		ro.Limit = math.MaxInt64
		ro.Append, closer = s.makeIntersect(flags)
	} else if flags.TwoHops != "" {
		ro.Limit = math.MaxInt64
		ro.Append, closer = s.makeTwoHops(flags)
	} else {
		ro.Limit = flags.Limit
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
		Match:       flags.Match,
		WithData:    flags.WithData,
		Limit:       flags.Limit,
		Append: func(r *ranges.Result, p s2pkg.Pair) error {
			r.Count++
			heap.Push(ph, p)
			if ph.Len() > flags.Limit {
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

		startclk := clock.Now()
		process := func(keyBuf, scoreBuf []byte) error {
			if opt.Match != "" {
				if clock.Now().Sub(startclk) > ranges.HardMatchTimeout {
					return ranges.ErrAppendSafeExit
				}
				if !s2pkg.MatchBinary(opt.Match, keyBuf) {
					return nil
				}
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

		startclk := clock.Now()
		process := func(k, dataBuf []byte) error {
			k = k[len(bkScore):]
			key := string(k[8:])
			if opt.Match != "" {
				if clock.Now().Sub(startclk) > ranges.HardMatchTimeout {
					return ranges.ErrAppendSafeExit
				}
				if !s2pkg.MatchMemberOrData(opt.Match, key, dataBuf) {
					return nil
				}
			}
			p := s2pkg.Pair{Member: key, Score: s2pkg.BytesToFloat(k[:])}
			if opt.WithData {
				p.Data = s2pkg.Bytes(dataBuf)
			}
			err := opt.Append(&rr, p)
			if opt.ILimit != nil {
				// Before reaching ILimit, we don't count.
				// Refer to TestZRangeNorm in redis_test.go for examples.
				if len(rr.Pairs) > ranges.HardLimit {
					return fmt.Errorf("range [start:ILimit] is too large to store")
				}

				if il := *opt.ILimit; opt.Rev {
					if p.Score > il {
						rr.Count = 0
					}
				} else {
					if p.Score < il {
						rr.Count = 0
					}
				}
			}
			return err
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

func (s *Server) makeFanoutScore(start, end ranges.Limit, flags wire.Flags) (func(*ranges.Result, s2pkg.Pair) error, func() error) {
	iter := s.DB.NewIter(ranges.ZSetScoreKeyValueFullRange)
	ddl := clock.Now().Add(flags.Timeout)
	return func(r *ranges.Result, p s2pkg.Pair) error {
		_, bkScore, _ := ranges.GetZSetRangeKey(flags.KeyFunc(p.Member))
		startKey := append(s2pkg.Bytes(bkScore), s2pkg.FloatToBytes(start.Float)...)
		endKey := append(s2pkg.Bytes(bkScore), s2pkg.FloatToBytes(end.Float)...)
		rev := start.Float > end.Float

		p.Children = new([]s2pkg.Pair)
		r.Pairs = append(r.Pairs, p)
		r.Count++

		if rev {
			iter.SeekLT(startKey)
		} else {
			iter.SeekGE(startKey)
		}
		for iter.Valid() {
			if !rev && bytes.Compare(iter.Key(), endKey) >= 0 {
				break
			}
			if rev && bytes.Compare(iter.Key(), endKey) < 0 {
				break
			}
			if !bytes.HasPrefix(iter.Key(), bkScore) {
				break
			}
			if clock.Now().After(ddl) || r.Count >= flags.Limit {
				return ranges.ErrAppendSafeExit
			}
			key := iter.Key()[len(startKey):]
			var data []byte
			if flags.WithData {
				data = s2pkg.Bytes(iter.Value())
			}
			*p.Children = append(*p.Children, s2pkg.Pair{
				Member: string(key),
				Score:  s2pkg.BytesToFloat(iter.Key()[len(bkScore):]),
				Data:   data,
			})
			r.Count++

			if rev {
				iter.Prev()
			} else {
				iter.Next()
			}
		}

		if clock.Now().After(ddl) || r.Count >= flags.Limit {
			return ranges.ErrAppendSafeExit
		}
		return nil
	}, iter.Close
}

func (s *Server) makeTwoHops(flags wire.Flags) (func(*ranges.Result, s2pkg.Pair) error, func() error) {
	iter := s.DB.NewIter(ranges.ZSetKeyScoreFullRange)
	ddl := clock.Now().Add(flags.Timeout)
	endpoint := flags.TwoHops
	return func(r *ranges.Result, p s2pkg.Pair) error {
		bkName, _, _ := ranges.GetZSetRangeKey(flags.KeyFunc(p.Member))
		x := append(bkName, endpoint...)
		iter.SeekGE(x)
		if iter.Valid() && bytes.Equal(iter.Key(), x) {
			r.Count++
			r.Pairs = append(r.Pairs, p)
		}
		if r.Count < flags.Limit && clock.Now().Before(ddl) {
			return nil
		}
		return ranges.ErrAppendSafeExit
	}, iter.Close
}

func (s *Server) makeIntersect(flags wire.Flags) (func(r *ranges.Result, p s2pkg.Pair) error, func() error) {
	iter := s.DB.NewIter(ranges.ZSetKeyScoreFullRange)
	ddl := clock.Now().Add(flags.Timeout)
	return func(r *ranges.Result, p s2pkg.Pair) error {
		count, hits := 0, 0
		for _, ia := range flags.Intersect {
			var exist bool
			if ia.Bloom != nil {
				exist = ia.Bloom.Contains(p.Member)
			} else {
				bkName, _, _ := ranges.GetZSetRangeKey(ia.Key)
				x := append(bkName, p.Member...)
				exist = iter.SeekGE(x) && bytes.Equal(iter.Key(), x)
			}
			if ia.Not {
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
		if r.Count < flags.Limit && clock.Now().Before(ddl) {
			return nil
		}
		return ranges.ErrAppendSafeExit
	}, iter.Close
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
	count, timedout, start := flags.Count+1, "", clock.Now()
	s.Foreach(cursor, func(k string) bool {
		if time.Since(start) > flags.Timeout {
			timedout = k
			return false
		}
		if flags.Match != "" && !s2pkg.Match(flags.Match, k) {
			return true
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

func (s *Server) ScanScoreDump(out io.Writer, startCursor, endCursor string, start, end float64, flags wire.Flags) {
	log := log.WithField("shard", fmt.Sprintf("ssd%04x", uint16(clock.UnixNano())))

	gw := gzip.NewWriter(out)
	defer gw.Close()

	w := csv.NewWriter(gw)
	defer w.Flush()

	snap := s.DB.NewSnapshot()
	defer snap.Close()

	log.Infof("ScanScoreDump starts range %q-%q, %v-%v, match: %q", startCursor, endCursor, start, end, flags.Match)

	c := snap.NewIter(&pebble.IterOptions{
		LowerBound: []byte("zsetskv_" + startCursor),
		UpperBound: s2pkg.IncBytesInplace([]byte("zsetskv_" + endCursor)),
	})
	defer c.Close()

	if !c.First() {
		log.Infof("finished dumping")
		w.Write([]string{"na"})
		return
	}

	skips := 0
	dummyWrite := func() bool {
		if skips++; skips%1000 == 0 {
			if err := w.Write([]string{"", "", "", ""}); err != nil {
				log.Errorf("write error: %v", err)
				return false
			}
			if err := gw.Flush(); err != nil {
				log.Errorf("flush error: %v", err)
				return false
			}
		}
		return true
	}

	var buf = []string{"", "", "", ""}
	for c.Valid() {
		cursor, pair := s2pkg.PairFromSKVCursor(c)
		if pair.Score < start {
			if !dummyWrite() {
				return
			}
			c.SeekGE(append([]byte("zsetskv_"+cursor+"\x00"), s2pkg.FloatToBytes(start)...))
			continue
		}
		if pair.Score > end {
			if !dummyWrite() {
				return
			}
			c.SeekGE([]byte("zsetskv_" + cursor + "\x01"))
			continue
		}

		if flags.Match != "" && !s2pkg.MatchMemberOrData(flags.Match, pair.Member, pair.Data) {
			if !dummyWrite() {
				return
			}
			c.Next()
			continue
		}

		buf[0] = cursor
		buf[1] = strconv.FormatFloat(pair.Score, 'f', -1, 64)
		buf[2] = pair.Member
		if flags.WithData {
			buf[3] = *(*string)(unsafe.Pointer(&pair.Data))
		} else {
			buf[3] = ""
		}

		if err := w.Write(buf); err != nil {
			log.Errorf("write error: %v", err)
			return
		}

		if clock.Rand() < 0.001 {
			if err := gw.Flush(); err != nil {
				log.Errorf("flush error: %v", err)
				return
			}
		}
		c.Next()
	}

	log.Infof("finished dumping")
}
