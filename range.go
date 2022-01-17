package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/internal"
	"github.com/coyove/s2db/redisproto"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

var (
	MinScoreRange = internal.RangeLimit{Float: math.Inf(-1), Inclusive: true}
	MaxScoreRange = internal.RangeLimit{Float: math.Inf(1), Inclusive: true}

	errSafeExit = fmt.Errorf("exit")
)

func (s *Server) ZCount(lex bool, name string, start, end string, match string) (int, error) {
	if lex {
		_, c, err := s.runPreparedRangeTx(name, rangeLex(name,
			internal.NewRLFromString(start),
			internal.NewRLFromString(end),
			internal.RangeOptions{
				OffsetStart: 0,
				OffsetEnd:   math.MaxInt64,
				LexMatch:    match,
				Limit:       internal.RangeHardLimit,
			}))
		return c, err
	}
	rangeStart, err := internal.NewRLFromFloatString(start)
	if err != nil {
		return 0, err
	}
	rangeEnd, err := internal.NewRLFromFloatString(end)
	if err != nil {
		return 0, err
	}
	_, c, err := s.runPreparedRangeTx(name, rangeScore(name, rangeStart, rangeEnd, internal.RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		Limit:       internal.RangeHardLimit,
		LexMatch:    match,
	}))
	return c, err

}

func (s *Server) ZRange(rev bool, name string, start, end int, flags redisproto.Flags) ([]internal.Pair, error) {
	rangeStart, rangeEnd := MinScoreRange, MaxScoreRange
	if rev {
		rangeStart, rangeEnd = MaxScoreRange, MinScoreRange
	}
	p, _, err := s.runPreparedRangeTx(name, rangeScore(name, rangeStart, rangeEnd, internal.RangeOptions{
		Rev:         rev,
		OffsetStart: start,
		OffsetEnd:   end,
		Limit:       flags.LIMIT,
		WithData:    flags.WITHDATA,
		Append:      internal.DefaultRangeAppend,
	}))
	return p, err
}

func (s *Server) ZRangeByLex(rev bool, name string, start, end string, flags redisproto.Flags) (p []internal.Pair, err error) {
	ro := internal.RangeOptions{
		Rev:      rev,
		LexMatch: flags.MATCH,
	}
	if flags.INTERSECT != nil {
		p, err = s.zIntersect(flags, func(bkm map[*bbolt.Bucket]bas.Value) ([]internal.Pair, int, error) {
			ro.Limit = math.MaxInt64
			ro.Append = genIntersectFunc(bkm, flags)
			return s.runPreparedRangeTx(name, rangeLex(name, internal.NewRLFromString(start), internal.NewRLFromString(end), ro))
		})
	} else if flags.TWOHOPS.ENDPOINT != nil {
		ro.Limit = math.MaxInt64
		ro.Append = genTwoHopsFunc(s, flags)
		p, _, err = s.runPreparedRangeTx(name, rangeLex(name, internal.NewRLFromString(start), internal.NewRLFromString(end), ro))
	} else {
		ro.Limit = flags.LIMIT
		ro.Append = internal.DefaultRangeAppend
		p, _, err = s.runPreparedRangeTx(name, rangeLex(name, internal.NewRLFromString(start), internal.NewRLFromString(end), ro))
	}
	if flags.WITHDATA && err == nil {
		err = s.fillPairsData(name, p)
	}
	return p, err
}

func (s *Server) ZRangeByScore(rev bool, name string, start, end string, flags redisproto.Flags) (p []internal.Pair, err error) {
	rangeStart, err := internal.NewRLFromFloatString(start)
	if err != nil {
		return nil, err
	}
	rangeEnd, err := internal.NewRLFromFloatString(end)
	if err != nil {
		return nil, err
	}
	ro := internal.RangeOptions{
		Rev:            rev,
		OffsetStart:    0,
		OffsetEnd:      math.MaxInt64,
		LexMatch:       flags.MATCH,
		ScoreMatchData: flags.MATCHDATA,
		WithData:       flags.WITHDATA,
	}
	if flags.INTERSECT != nil {
		return s.zIntersect(flags, func(bkm map[*bbolt.Bucket]bas.Value) ([]internal.Pair, int, error) {
			ro.Limit = math.MaxInt64
			ro.Append = genIntersectFunc(bkm, flags)
			return s.runPreparedRangeTx(name, rangeScore(name, rangeStart, rangeEnd, ro))
		})
	}
	if flags.TWOHOPS.ENDPOINT != nil {
		ro.Limit = math.MaxInt64
		ro.Append = genTwoHopsFunc(s, flags)
		p, _, err = s.runPreparedRangeTx(name, rangeScore(name, rangeStart, rangeEnd, ro))
		return p, err
	}
	ro.Limit = flags.LIMIT
	ro.Append = internal.DefaultRangeAppend
	p, _, err = s.runPreparedRangeTx(name, rangeScore(name, rangeStart, rangeEnd, ro))
	return p, err
}

func rangeLex(name string, start, end internal.RangeLimit, opt internal.RangeOptions) func(tx *bbolt.Tx) ([]internal.Pair, int, error) {
	return func(tx *bbolt.Tx) (pairs []internal.Pair, count int, err error) {
		bk := tx.Bucket([]byte("zset." + name))
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

			p := internal.Pair{Member: string(k), Score: internal.BytesToFloat(sc)}
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
			return pairs, count, deletePair(tx, name, pairs, opt.DeleteLog)
		}
		return pairs, count, nil
	}
}

func rangeScore(name string, start, end internal.RangeLimit, opt internal.RangeOptions) func(tx *bbolt.Tx) ([]internal.Pair, int, error) {
	return func(tx *bbolt.Tx) (pairs []internal.Pair, count int, err error) {
		bk := tx.Bucket([]byte("zset.score." + name))
		if bk == nil {
			return
		}
		opt.TranslateOffset(name, bk)

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
			p := internal.Pair{Member: key, Score: internal.BytesToFloat(k[:8])}
			if opt.WithData {
				p.Data = append([]byte{}, dataBuf...)
			}
			count++
			if opt.Append != nil && !opt.Append(&pairs, p) {
				return errSafeExit
			}
			return nil
		}

		startInt, endInt := internal.FloatToOrderedUint64(start.Float), internal.FloatToOrderedUint64(end.Float)
		if opt.Rev {
			if start.Inclusive {
				startInt++
			}
			if !end.Inclusive {
				endInt++
			}
			var k, dataBuf = c.Seek(internal.Uint64ToBytes(startInt))
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
			k, dataBuf := c.Seek(internal.Uint64ToBytes(startInt))
			for i := 0; len(k) >= 8 && len(pairs) < opt.Limit; i++ {
				x := binary.BigEndian.Uint64(k)
				if x >= startInt && x < endInt {
					if i >= opt.OffsetStart && i <= opt.OffsetEnd {
						if err := do(k, dataBuf); err != nil {
							return pairs, 0, err
						}
					}
					k, dataBuf = c.Next()
				} else {
					break
				}
			}
		}

		if len(opt.DeleteLog) > 0 {
			return pairs, count, deletePair(tx, name, pairs, opt.DeleteLog)
		}
		return pairs, count, nil
	}
}

func (s *Server) ZRank(rev bool, name, key string, limit int) (rank int, err error) {
	rank = -1
	keybuf := []byte(key)
	err = s.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset.score." + name))
		if bk == nil {
			return nil
		}
		if limit <= 0 {
			limit = HardLimit * 2
		}
		c := bk.Cursor()
		if rev {
			for k, _ := c.Last(); len(k) > 8; k, _ = c.Prev() {
				rank++
				if bytes.Equal(k[8:], keybuf) || rank == limit+1 {
					return nil
				}
			}
		} else {
			for k, _ := c.First(); len(k) > 8; k, _ = c.Next() {
				rank++
				if bytes.Equal(k[8:], keybuf) || rank == limit+1 {
					return nil
				}
			}
		}
		rank = -1
		return nil
	})
	if rank == limit+1 {
		rank = -1
	}
	return
}

func (s *Server) Scan(cursor string, match string, shard int, count int) (pairs []internal.Pair, nextCursor string, err error) {
	if count > HardLimit || count <= 0 {
		count = HardLimit
	}
	count++

	startShard := 0
	if cursor != "" {
		startShard = shardIndex(cursor)
	}
	if shard >= 0 {
		startShard = shard
	}

	for ; startShard < ShardNum; startShard++ {
		err = s.db[startShard].View(func(tx *bbolt.Tx) error {
			c := tx.Cursor()
			k, _ := c.First()
			if cursor != "" {
				k, _ = c.Seek([]byte("q." + cursor))
			}

			for ; len(k) > 0; k, _ = c.Next() {
				if bytes.HasPrefix(k, []byte("zset.score")) {
					continue
				}
				var key string
				if bytes.HasPrefix(k, []byte("zset.")) {
					key = string(k[5:])
				} else if bytes.HasPrefix(k, []byte("q.")) {
					key = string(k[2:])
				} else {
					continue
				}
				if match != "" {
					m, err := filepath.Match(match, key)
					if err != nil {
						return err
					}
					if !m {
						continue
					}
				}
				pairs = append(pairs, internal.Pair{
					Member: key,
					Score:  float64(tx.Bucket(k).Stats().KeyN),
				})
				if len(pairs) >= count {
					return errSafeExit
				}
			}
			return nil
		})
		if err != nil {
			if err == errSafeExit {
				err = nil
				break
			}
			return
		}
		cursor = ""
		if shard >= 0 {
			break
		}
	}

	if len(pairs) >= count {
		pairs, nextCursor = pairs[:count-1], pairs[count-1].Member
	}
	return
}

func (s *Server) zIntersect(flags redisproto.Flags,
	runner func(map[*bbolt.Bucket]bas.Value) ([]internal.Pair, int, error)) (p []internal.Pair, err error) {
	bkm := map[*bbolt.Bucket]bas.Value{}
	for k, f := range flags.INTERSECT {
		tx, err := s.pick(k).Begin(false)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		bk := tx.Bucket([]byte("zset." + k))
		if bk == nil {
			continue
		}
		bkm[bk] = f
	}
	if len(bkm) == 0 {
		return
	}
	p, _, err = runner(bkm)
	if err == errSafeExit {
		err = nil
	}
	return
}

func genTwoHopsFunc(s *Server, flags redisproto.Flags) func(pairs *[]internal.Pair, p internal.Pair) bool {
	ddl := time.Now().Add(flags.TIMEOUT)
	return func(pairs *[]internal.Pair, p internal.Pair) bool {
		key := p.Member
		if bas.IsCallable(flags.TWOHOPS.KEYMAP) {
			res, err := bas.Call2(flags.TWOHOPS.KEYMAP.Object(), bas.Str(key))
			if err != nil {
				log.Error("TwoHopsFunc: ", key, " error: ", err)
				return false
			}
			key = res.String()
		}
		s.pick(key).View(func(tx *bbolt.Tx) error {
			if bk := tx.Bucket([]byte("zset." + key)); bk != nil {
				if len(bk.Get(flags.TWOHOPS.ENDPOINT)) > 0 {
					*pairs = append(*pairs, p)
				}
			}
			return nil
		})
		return len(*pairs) < flags.LIMIT && time.Now().Before(ddl)
	}
}

func genIntersectFunc(bkm map[*bbolt.Bucket]bas.Value, flags redisproto.Flags) func(pairs *[]internal.Pair, p internal.Pair) bool {
	ddl := time.Now().Add(flags.TIMEOUT)
	return func(pairs *[]internal.Pair, p internal.Pair) bool {
		key := p.Member
		hits := 0
		for bk, f := range bkm {
			if bas.IsCallable(f) {
				res, err := bas.Call2(f.Object(), bas.Str(key))
				if err != nil {
					log.Error("IntersectFunc: ", key, " error: ", err)
					return false
				}
				key = res.String()
			}
			if len(bk.Get([]byte(key))) > 0 {
				hits++
			}
		}
		if hits == len(bkm) {
			*pairs = append(*pairs, p)
		}
		return len(*pairs) < flags.LIMIT && time.Now().Before(ddl)
	}
}
