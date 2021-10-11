package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"

	"go.etcd.io/bbolt"
)

var (
	MinScoreRange = RangeLimit{Float: math.Inf(-1), Inclusive: true}
	MaxScoreRange = RangeLimit{Float: math.Inf(1), Inclusive: true}
)

func (s *Server) ZCount(name string, start, end string, match string) (int, error) {
	rangeStart, err := (RangeLimit{}).fromFloatString(start)
	if err != nil {
		return 0, err
	}
	rangeEnd, err := (RangeLimit{}).fromFloatString(end)
	if err != nil {
		return 0, err
	}
	_, c, err := s.runPreparedRangeTx(name, rangeScore(name, rangeStart, rangeEnd, RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		CountOnly:   true,
		LexMatch:    match,
	}))
	return c, err

}

func (s *Server) ZRange(rev bool, name string, start, end int, withData bool) ([]Pair, error) {
	if rev {
		p, _, err := s.runPreparedRangeTx(name, rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{
			OffsetStart: -end - 1,
			OffsetEnd:   -start - 1,
			WithData:    withData,
		}))
		return reversePairs(p), err
	}
	p, _, err := s.runPreparedRangeTx(name, rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{
		OffsetStart: start,
		OffsetEnd:   end,
		WithData:    withData,
	}))
	return p, err
}

func (s *Server) ZRangeByLex(rev bool, name string, start, end string, match string, limit int, withData bool) ([]Pair, error) {
	p, _, err := s.runPreparedRangeTx(name, rangeLex(name, (RangeLimit{}).fromString(start), (RangeLimit{}).fromString(end), RangeOptions{
		Rev:      rev,
		LexMatch: match,
		Limit:    limit,
	}))
	if withData {
		err = s.fillPairsData(name, p)
	}
	return p, err
}

func (s *Server) ZRangeByScore(rev bool, name string, start, end string, match string, limit int, withData bool) (p []Pair, err error) {
	var rangeStart, rangeEnd RangeLimit
	var err1, err2 error
	if rev {
		rangeStart, err1 = rangeStart.fromFloatString(end)
		rangeEnd, err2 = rangeEnd.fromFloatString(start)
	} else {
		rangeStart, err1 = rangeStart.fromFloatString(start)
		rangeEnd, err2 = rangeEnd.fromFloatString(end)
	}
	if err1 != nil {
		return nil, err1
	} else if err2 != nil {
		return nil, err2
	}
	p, _, err = s.runPreparedRangeTx(name, rangeScore(name, rangeStart, rangeEnd, RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   math.MaxInt64,
		Limit:       limit,
		WithData:    withData,
		LexMatch:    match,
	}))
	if rev {
		reversePairs(p)
	}
	return p, err
}

func rangeLex(name string, start, end RangeLimit, opt RangeOptions) func(tx *bbolt.Tx) ([]Pair, int, error) {
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
				pairs = append(pairs, Pair{Key: string(k), Score: bytesToFloat(sc)})
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
			k, sc := c.Seek(startBuf)
			if start.LexLast {
				k, sc = c.Last()
			} else if !start.Inclusive && bytes.Equal(k, startBuf) {
				k, sc = c.Prev()
			}
			for i := 0; len(pairs) < opt.getLimit(); i++ {
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
			for i := 0; len(pairs) < opt.getLimit(); i++ {
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

func rangeScore(name string, start, end RangeLimit, opt RangeOptions) func(tx *bbolt.Tx) ([]Pair, int, error) {
	return func(tx *bbolt.Tx) (pairs []Pair, count int, err error) {
		bk := tx.Bucket([]byte("zset.score." + name))
		if bk == nil {
			return
		}

		startBuf, endBuf := floatToInternalUint64(start.Float), floatToInternalUint64(end.Float)
		opt.translateOffset(name, bk)

		if !start.Inclusive {
			startBuf++
		}

		if end.Inclusive {
			endBuf++
		}

		c := bk.Cursor()
		k, dataBuf := c.Seek(intToBytes(startBuf))

		limit := opt.getLimit()
		for i := 0; len(pairs) < limit; i++ {
			if len(k) < 8 {
				break
			}
			x := binary.BigEndian.Uint64(k)
			if x >= startBuf && x < endBuf {
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

func (s *Server) scan(cursor string, match string, shard int, count int) (pairs []Pair, nextCursor string, err error) {
	if count > HardLimit {
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
