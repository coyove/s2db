package main

import (
	"bytes"
	"fmt"
	"math"
	"path/filepath"

	"go.etcd.io/bbolt"
)

var (
	MinScoreRange = RangeLimit{Float: math.Inf(-1), Inclusive: true}
	MaxScoreRange = RangeLimit{Float: math.Inf(1), Inclusive: true}
)

func (s *Server) ZRank(name, key string, limit int) (int, error) {
	return s.zRank(name, key, limit, false)
}

func (s *Server) ZRevRank(name, key string, limit int) (int, error) {
	return s.zRank(name, key, limit, true)
}

func (s *Server) ZCount(name string, start, end string, match string) (int, error) {
	rangeStart, err := (RangeLimit{}).fromFloatString(start)
	if err != nil {
		return 0, err
	}
	rangeEnd, err := (RangeLimit{}).fromFloatString(end)
	if err != nil {
		return 0, err
	}
	_, c, err := s.runPreparedRangeTx(name, s.rangeScore(name, rangeStart, rangeEnd, RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		CountOnly:   true,
		LexMatch:    match,
	}))
	return c, err

}

func (s *Server) ZRange(name string, start, end int, withData bool) ([]Pair, error) {
	p, _, err := s.runPreparedRangeTx(name, s.rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{
		OffsetStart: start,
		OffsetEnd:   end,
		WithData:    withData,
	}))
	return p, err
}

func (s *Server) ZRevRange(name string, start, end int, withData bool) ([]Pair, error) {
	p, _, err := s.runPreparedRangeTx(name, s.rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{
		OffsetStart: -end - 1,
		OffsetEnd:   -start - 1,
		WithData:    withData,
	}))
	return reversePairs(p), err
}

func (s *Server) ZRangeByLex(name string, start, end string, match string, limit int) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromString(start)
	rangeEnd := (RangeLimit{}).fromString(end)
	p, _, err := s.runPreparedRangeTx(name, s.rangeLex(name, rangeStart, rangeEnd, RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		LexMatch:    match,
		Limit:       limit,
	}))
	return p, err
}

func (s *Server) ZRevRangeByLex(name string, start, end string, match string, limit int) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromString(end)
	rangeEnd := (RangeLimit{}).fromString(start)
	p, _, err := s.runPreparedRangeTx(name, s.rangeLex(name, rangeStart, rangeEnd, RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		LexMatch:    match,
		Limit:       limit,
	}))
	return reversePairs(p), err
}

func (s *Server) ZRangeByScore(name string, start, end string, match string, limit int, withData bool) ([]Pair, error) {
	rangeStart, err := (RangeLimit{}).fromFloatString(start)
	if err != nil {
		return nil, err
	}
	rangeEnd, err := (RangeLimit{}).fromFloatString(end)
	if err != nil {
		return nil, err
	}
	p, _, err := s.runPreparedRangeTx(name, s.rangeScore(name, rangeStart, rangeEnd, RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		WithData:    withData,
		LexMatch:    match,
		Limit:       limit,
	}))
	return p, err
}

func (s *Server) ZRevRangeByScore(name string, start, end string, match string, limit int, withData bool) ([]Pair, error) {
	rangeStart, err := (RangeLimit{}).fromFloatString(end)
	if err != nil {
		return nil, err
	}
	rangeEnd, err := (RangeLimit{}).fromFloatString(start)
	if err != nil {
		return nil, err
	}
	p, _, err := s.runPreparedRangeTx(name, s.rangeScore(name, rangeStart, rangeEnd, RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		Limit:       limit,
		WithData:    withData,
		LexMatch:    match,
	}))
	return reversePairs(p), err
}

func (s *Server) rangeLex(name string, start, end RangeLimit, opt RangeOptions) func(tx *bbolt.Tx) ([]Pair, int, error) {
	return func(tx *bbolt.Tx) (pairs []Pair, count int, err error) {
		bk := tx.Bucket([]byte("zset." + name))
		if bk == nil {
			return
		}

		startBuf, endBuf := []byte(start.Value), []byte(end.Value)
		opt.translateOffset(name, bk)

		if !start.Inclusive {
			startBuf = append(startBuf, 0)
		}

		endFlag := 0
		if !end.Inclusive {
			endFlag = -1
		}

		c := bk.Cursor()
		k, sc := c.Seek(startBuf)

		limit := opt.getLimit(s)
		for i := 0; len(pairs) < limit; i++ {
			if len(sc) > 0 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) <= endFlag {
				if i >= opt.OffsetStart {
					if i <= opt.OffsetEnd {
						if opt.LexMatch != "" {
							m, err := filepath.Match(opt.LexMatch, string(k))
							if err != nil {
								return nil, 0, err
							}
							if !m {
								k, sc = c.Next()
								continue
							}
						}

						if !opt.CountOnly {
							pairs = append(pairs, Pair{Key: string(k), Score: bytesToFloat(sc)})
						}
						count++
					} else {
						break
					}
				}
			} else {
				break
			}
			k, sc = c.Next()
		}

		if len(opt.DeleteLog) > 0 {
			return pairs, count, s.deletePair(tx, name, pairs, opt.DeleteLog)
		}
		return pairs, count, nil
	}
}

func (s *Server) rangeScore(name string, start, end RangeLimit, opt RangeOptions) func(tx *bbolt.Tx) ([]Pair, int, error) {
	return func(tx *bbolt.Tx) (pairs []Pair, count int, err error) {
		bk := tx.Bucket([]byte("zset.score." + name))
		if bk == nil {
			return
		}

		startBuf, endBuf := floatToBytes(start.Float), floatToBytes(end.Float)
		opt.translateOffset(name, bk)

		if !start.Inclusive {
			startBuf = floatBytesStep(startBuf, 1)
		}

		if end.Inclusive {
			endBuf = floatBytesStep(endBuf, 1)
		}

		c := bk.Cursor()
		k, dataBuf := c.Seek(startBuf)

		limit := opt.getLimit(s)
		for i := 0; len(pairs) < limit; i++ {
			if len(k) >= 8 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) < 0 {
				if i >= opt.OffsetStart {
					if i <= opt.OffsetEnd {
						key := string(k[8:])
						if opt.LexMatch != "" {
							m, err := filepath.Match(opt.LexMatch, key)
							if err != nil {
								return nil, 0, err
							}
							if !m {
								k, dataBuf = c.Next()
								continue
							}
						}
						if !opt.CountOnly {
							p := Pair{
								Key:   key,
								Score: bytesToFloat(k[:8]),
							}
							if opt.WithData {
								p.Data = append([]byte{}, dataBuf...)
							}
							pairs = append(pairs, p)
						}
						count++
					} else {
						break
					}
				}
			} else {
				break
			}
			k, dataBuf = c.Next()
		}
		if len(opt.DeleteLog) > 0 {
			return pairs, count, s.deletePair(tx, name, pairs, opt.DeleteLog)
		}
		return pairs, count, nil
	}
}

func (s *Server) zRank(name, key string, limit int, rev bool) (rank int, err error) {
	rank = -1
	keybuf := []byte(key)
	err = s.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset.score." + name))
		if bk == nil {
			return nil
		}
		if limit <= 0 {
			limit = s.HardLimit * 2
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

func (s *Server) scan(cursor string, match string, shard int, count int) (pairs []Pair, nextCursor string, err error) {
	if count > s.HardLimit {
		count = s.HardLimit
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
				k, _ = c.Seek([]byte("zset." + cursor))
			}

			for ; len(k) > 0; k, _ = c.Next() {
				if bytes.HasPrefix(k, []byte("zset.score")) {
					continue
				}
				if !bytes.HasPrefix(k, []byte("zset.")) {
					continue
				}
				key := string(k[5:])
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
