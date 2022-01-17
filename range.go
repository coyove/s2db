package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"sync"
	"unsafe"

	"github.com/coyove/s2db/internal"
	"github.com/coyove/s2db/redisproto"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

var (
	MinScoreRange = internal.RangeLimit{Float: math.Inf(-1), Inclusive: true}
	MaxScoreRange = internal.RangeLimit{Float: math.Inf(1), Inclusive: true}
)

func (s *Server) ZCount(lex bool, name string, start, end string, match string) (int, error) {
	if lex {
		_, c, err := s.runPreparedRangeTx(name, rangeLex(name,
			internal.NewRLFromString(start),
			internal.NewRLFromString(end),
			internal.RangeOptions{
				OffsetStart: 0,
				OffsetEnd:   math.MaxInt64,
				CountOnly:   true,
				LexMatch:    match,
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
		CountOnly:   true,
		LexMatch:    match,
	}))
	return c, err

}

func (s *Server) ZRange(rev bool, name string, start, end int, flags redisproto.Flags) ([]Pair, error) {
	if rev {
		p, _, err := s.runPreparedRangeTx(name, rangeScore(name, MaxScoreRange, MinScoreRange, internal.RangeOptions{
			Rev:         true,
			OffsetStart: start,
			OffsetEnd:   end,
			WithData:    flags.WITHDATA,
		}))
		return p, err
	}
	p, _, err := s.runPreparedRangeTx(name, rangeScore(name, MinScoreRange, MaxScoreRange, internal.RangeOptions{
		OffsetStart: start,
		OffsetEnd:   end,
		WithData:    flags.WITHDATA,
	}))
	return p, err
}

func (s *Server) ZRangeByLex(rev bool, name string, start, end string, flags redisproto.Flags) ([]Pair, error) {
	if len(flags.UNION) > 0 {
		names := append(flags.UNION, name)
		flags.UNION = nil
		out := s.zUnion(names, func(key string) ([]Pair, error) {
			return s.ZRangeByLex(rev, key, start, end, flags)
		})
		sort.SliceStable(out, func(i, j int) bool {
			if rev {
				return out[i].Key > out[j].Key
			}
			return out[i].Key < out[j].Key
		})
		if flags.LIMIT > 0 && len(out) > flags.LIMIT {
			out = out[:flags.LIMIT]
		}
		return out, nil
	}
	p, _, err := s.runPreparedRangeTx(name, rangeLex(name,
		internal.NewRLFromString(start),
		internal.NewRLFromString(end),
		internal.RangeOptions{
			Rev:      rev,
			LexMatch: flags.MATCH,
			Limit:    flags.LIMIT,
		}))
	if flags.WITHDATA {
		err = s.fillPairsData(name, p)
	}
	return p, err
}

func (s *Server) ZRangeByScore(rev bool, name string, start, end string, flags redisproto.Flags) ([]Pair, error) {
	if len(flags.UNION) > 0 {
		names := append(flags.UNION, name)
		flags.UNION = nil
		out := s.zUnion(names, func(key string) ([]Pair, error) {
			return s.ZRangeByScore(rev, key, start, end, flags)
		})
		sort.SliceStable(out, func(i, j int) bool {
			if rev {
				return out[i].Score > out[j].Score
			}
			return out[i].Score < out[j].Score
		})
		if flags.LIMIT > 0 && len(out) > flags.LIMIT {
			out = out[:flags.LIMIT]
		}
		return out, nil
	}
	rangeStart, err := internal.NewRLFromFloatString(start)
	if err != nil {
		return nil, err
	}
	rangeEnd, err := internal.NewRLFromFloatString(end)
	if err != nil {
		return nil, err
	}
	p, _, err := s.runPreparedRangeTx(name, rangeScore(name, rangeStart, rangeEnd, internal.RangeOptions{
		Rev:            rev,
		OffsetStart:    0,
		OffsetEnd:      math.MaxInt64,
		Limit:          flags.LIMIT,
		WithData:       flags.WITHDATA,
		LexMatch:       flags.MATCH,
		ScoreMatchData: flags.MATCHDATA,
	}))
	return p, err
}

func (s *Server) zUnion(names []string, f func(string) ([]Pair, error)) []Pair {
	wg, out, outMu := sync.WaitGroup{}, make([]Pair, 0), sync.Mutex{}
	wg.Add(len(names))
	for _, key := range names {
		go func(key string) {
			defer wg.Done()
			p, err := f(key)
			if err != nil {
				log.Errorf("zUnion(%s): %v", key, err)
				return
			}
			outMu.Lock()
			out = append(out, p...)
			outMu.Unlock()
		}(key)
	}
	wg.Wait()
	return out
}

func rangeLex(name string, start, end internal.RangeLimit, opt internal.RangeOptions) func(tx *bbolt.Tx) ([]Pair, int, error) {
	return func(tx *bbolt.Tx) (pairs []Pair, count int, err error) {
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

			if !opt.CountOnly {
				pairs = append(pairs, Pair{Key: string(k), Score: internal.BytesToFloat(sc)})
			}
			count++
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
			for i := 0; len(pairs) < getLimit(opt); i++ {
				if len(sc) > 0 && bytes.Compare(k, startBuf) <= 0 && bytes.Compare(k, endBuf) >= endFlag {
					if err := do(k, sc); err != nil {
						return nil, 0, err
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
			for i := 0; len(pairs) < getLimit(opt); i++ {
				if len(sc) > 0 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) <= endFlag {
					if err := do(k, sc); err != nil {
						return nil, 0, err
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

func rangeScore(name string, start, end internal.RangeLimit, opt internal.RangeOptions) func(tx *bbolt.Tx) ([]Pair, int, error) {
	return func(tx *bbolt.Tx) (pairs []Pair, count int, err error) {
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
			if !opt.CountOnly {
				p := Pair{Key: key, Score: internal.BytesToFloat(k[:8])}
				if opt.WithData {
					p.Data = append([]byte{}, dataBuf...)
				}
				pairs = append(pairs, p)
			}
			count++
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
			for i := 0; len(k) >= 8 && len(pairs) < getLimit(opt); i++ {
				x := binary.BigEndian.Uint64(k)
				if x <= startInt && x >= endInt {
					if i >= opt.OffsetStart && i <= opt.OffsetEnd {
						if err := do(k, dataBuf); err != nil {
							return nil, 0, err
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
			for i := 0; len(k) >= 8 && len(pairs) < getLimit(opt); i++ {
				x := binary.BigEndian.Uint64(k)
				if x >= startInt && x < endInt {
					if i >= opt.OffsetStart && i <= opt.OffsetEnd {
						if err := do(k, dataBuf); err != nil {
							return nil, 0, err
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

func (s *Server) Scan(cursor string, match string, shard int, count int) (pairs []Pair, nextCursor string, err error) {
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

	exitErr := fmt.Errorf("exit")
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
				pairs = append(pairs, Pair{
					Key:   key,
					Score: float64(tx.Bucket(k).Stats().KeyN),
				})
				if len(pairs) >= count {
					return exitErr
				}
			}
			return nil
		})
		if err != nil {
			if err == exitErr {
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
		pairs, nextCursor = pairs[:count-1], pairs[count-1].Key
	}
	return
}
