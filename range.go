package main

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"go.etcd.io/bbolt"
)

var (
	MinScoreRange = s2pkg.RangeLimit{Float: math.Inf(-1), Inclusive: true}
	MaxScoreRange = s2pkg.RangeLimit{Float: math.Inf(1), Inclusive: true}

	errSafeExit = fmt.Errorf("exit")
)

func (s *Server) ZCount(lex bool, key string, start, end string, flags redisproto.Flags) (int, error) {
	onSuccess := func(p []s2pkg.Pair, count int) { s.addCache(key, flags.Command.HashCode(), count) }
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
	}), func(p []s2pkg.Pair, count int) { s.addCache(key, flags.Command.HashCode(), p) })
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
	success := func(p []s2pkg.Pair, count int) { s.addCache(key, flags.Command.HashCode(), p) }

	ro.Limit = flags.LIMIT
	ro.Append = s2pkg.DefaultRangeAppend
	p, _, err = s.runPreparedRangeTx(key, f(), success)
	if err == errSafeExit {
		err = nil
	}
	return p, err
}

func rangeLex(key string, start, end s2pkg.RangeLimit, opt s2pkg.RangeOptions) rangeFunc {
	return func(tx s2pkg.LogTx) (pairs []s2pkg.Pair, count int, err error) {
		bkName, _, _ := getZSetRangeKey(key)
		c := tx.NewIter(&pebble.IterOptions{
			LowerBound: bkName,
			UpperBound: incrBytes(bkName),
		})
		defer c.Close()

		do := func(k, sc []byte) error {
			if opt.LexMatch != "" {
				if !s2pkg.MatchBinary(opt.LexMatch, k) {
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

		startBuf, endBuf := append(dupBytes(bkName), start.Value...), append(dupBytes(bkName), end.Value...)
		if opt.Rev {
			endFlag := 0
			if !end.Inclusive {
				endFlag = 1
			}
			if start.Inclusive && !start.LexEnd {
				startBuf = incrBytes(startBuf)
			}
			if !c.SeekGE(startBuf) {
				c.Last()
			} else {
				c.Prev()
			}
			for i := 0; c.Valid() && len(pairs) < opt.Limit; i++ {
				k, sc := c.Key(), c.Value()
				if len(sc) > 0 && bytes.Compare(k, startBuf) <= 0 && bytes.Compare(k, endBuf) >= endFlag {
					if err := do(k[len(bkName):], sc); err != nil {
						return pairs, 0, err
					}
					c.Prev()
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
			c.SeekGE(startBuf)
			for i := 0; c.Valid() && len(pairs) < opt.Limit; i++ {
				k, sc := c.Key(), c.Value()
				if len(sc) > 0 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) <= endFlag {
					if err := do(k[len(bkName):], sc); err != nil {
						return pairs, 0, err
					}
					c.Next()
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
	return func(tx s2pkg.LogTx) (pairs []s2pkg.Pair, count int, err error) {
		_, bkScore, bkCounter := getZSetRangeKey(key)
		c := tx.NewIter(&pebble.IterOptions{
			LowerBound: bkScore,
			UpperBound: incrBytes(bkScore),
		})
		defer c.Close()

		_, n, _, err := GetKeyNumber(tx, bkCounter)
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
			if opt.LexMatch != "" {
				if !s2pkg.Match(opt.LexMatch, key) {
					return nil
				}
			}
			if opt.ScoreMatchData != "" {
				if !s2pkg.Match(opt.ScoreMatchData, key) && !s2pkg.MatchBinary(opt.ScoreMatchData, dataBuf) {
					return nil
				}
			}
			p := s2pkg.Pair{Member: key, Score: s2pkg.BytesToFloat(k[:])}
			if opt.WithData {
				p.Data = dupBytes(dataBuf)
			}
			count++
			if opt.Append != nil && !opt.Append(&pairs, p) {
				return errSafeExit
			}
			return nil
		}

		startInt := append(dupBytes(bkScore), s2pkg.FloatToBytes(start.Float)...)
		endInt := append(dupBytes(bkScore), s2pkg.FloatToBytes(end.Float)...)
		if opt.Rev {
			if start.Inclusive {
				startInt = incrBytes(startInt)
			}
			if !end.Inclusive {
				endInt = incrBytes(endInt)
			}
			if !c.SeekGE(startInt) {
				c.Last()
			} else {
				c.Prev()
			}
			for i := 0; c.Valid() && len(pairs) < opt.Limit; i++ {
				x := c.Key()
				if bytes.Compare(x, startInt) <= 0 && bytes.Compare(x, endInt) >= 0 {
					if i >= opt.OffsetStart && i <= opt.OffsetEnd {
						if err := do(c.Key(), c.Value()); err != nil {
							return pairs, 0, err
						}
					} else if i > opt.OffsetEnd {
						break
					}
					c.Prev()
				} else {
					break
				}
			}
		} else {
			if !start.Inclusive {
				startInt = incrBytes(startInt)
			}
			if end.Inclusive {
				endInt = incrBytes(endInt)
			}
			c.SeekGE(startInt)
			for i := 0; c.Valid() && len(pairs) < opt.Limit; i++ {
				x := c.Key()
				if bytes.Compare(x, startInt) >= 0 && bytes.Compare(x, endInt) < 0 {
					if i >= opt.OffsetStart && i <= opt.OffsetEnd {
						if err := do(c.Key(), c.Value()); err != nil {
							return pairs, 0, err
						}
					} else if i > opt.OffsetEnd {
						break
					}
					c.Next()
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
	keybuf := []byte(member)
	func() {
		_, bkScore, _ := getZSetRangeKey(key)
		c := s.db.NewIter(&pebble.IterOptions{
			LowerBound: bkScore,
			UpperBound: incrBytes(bkScore),
		})
		defer c.Close()
		if rev {
			for c.Last(); c.Valid() && bytes.HasPrefix(c.Key(), bkScore); c.Prev() {
				if bytes.Equal(c.Key()[len(bkScore)+8:], keybuf) || rank == flags.COUNT+1 {
					return
				}
				rank++
			}
		} else {
			for c.First(); c.Valid() && bytes.HasPrefix(c.Key(), bkScore); c.Next() {
				if bytes.Equal(c.Key()[len(bkScore)+8:], keybuf) || rank == flags.COUNT+1 {
					return
				}
				rank++
			}
		}
		rank = -1
	}()
	if rank == flags.COUNT+1 {
		rank = -1
	}
	if flags.ArgCount() > 0 {
		s.addCache(key, flags.HashCode(), rank)
	}
	return
}

func (s *Server) Foreach(cursor string, f func(k, typ string, bk ...*bbolt.Bucket) bool) (err error) {
	return
	// txs, close, err := s.openAllTx()
	// if err != nil {
	// 	return err
	// }
	// defer close()

	// var cursorsZ, cursorsQ [ShardNum]*bbolt.Cursor
	// keys := &s2pkg.PairHeap{DataOrder: true}
	// for i := range s.runners {
	// 	cursorsZ[i], cursorsQ[i] = txs[i].Cursor(), txs[i].Cursor()
	// 	if k, _ := cursorsQ[i].Seek([]byte("q." + cursor)); bytes.HasPrefix(k, []byte("q.")) {
	// 		heap.Push(keys, s2pkg.Pair{Score: float64(i), Data: k[2:], Member: "queue"})
	// 	}
	// 	if k, _ := cursorsZ[i].Seek([]byte("zset.score." + cursor)); bytes.HasPrefix(k, []byte("zset.score.")) {
	// 		heap.Push(keys, s2pkg.Pair{Score: float64(i), Data: k[11:], Member: "zset"})
	// 	}
	// }

	// for cont := true; keys.Len() > 0 && cont; {
	// 	p := heap.Pop(keys).(s2pkg.Pair)
	// 	name := string(p.Data)
	// 	if p.Member == "queue" {
	// 		cont = f(name, "queue", txs[int(p.Score)].Bucket([]byte("q."+name)))
	// 		if k, _ := cursorsQ[int(p.Score)].Next(); bytes.HasPrefix(k, []byte("q.")) {
	// 			p.Data = k[2:]
	// 			heap.Push(keys, p)
	// 		}
	// 	}
	// 	if p.Member == "zset" {
	// 		tx := txs[int(p.Score)]
	// 		cont = f(name, "zset", tx.Bucket([]byte("zset."+name)), tx.Bucket([]byte("zset.score."+name)))
	// 		if k, _ := cursorsZ[int(p.Score)].Next(); bytes.HasPrefix(k, []byte("zset.score.")) {
	// 			p.Data = k[11:]
	// 			heap.Push(keys, p)
	// 		}
	// 	}
	// }
	// return nil
}

func (s *Server) Scan(cursor string, flags redisproto.Flags) (pairs []s2pkg.Pair, nextCursor string, err error) {
	count, timedout, start := flags.COUNT+1, "", time.Now()
	if err := s.Foreach(cursor, func(k, typ string, bk ...*bbolt.Bucket) bool {
		if flags.MATCH != "" && !s2pkg.Match(flags.MATCH, k) {
			return true
		}
		if time.Since(start) > flags.TIMEOUT {
			timedout = k
			return false
		}
		pairs = append(pairs, s2pkg.Pair{Member: k, Score: float64(sizeOfBucket(bk[0]))})
		return len(pairs) < count
	}); err != nil {
		return nil, "", err
	}
	if timedout != "" {
		return nil, timedout, nil
	}
	if len(pairs) >= count {
		pairs, nextCursor = pairs[:count-1], pairs[count-1].Member
	}
	return
}
