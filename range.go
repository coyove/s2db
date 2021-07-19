package main

import (
	"bytes"
	"math"

	"go.etcd.io/bbolt"
)

var (
	MinScoreRange = RangeLimit{Float: math.Inf(-1), Inclusive: true}
	MaxScoreRange = RangeLimit{Float: math.Inf(1), Inclusive: true}
)

func (s *Server) ZCount(name string, start, end string) (int, error) {
	_, c, err := s.doZRangeByScore(name, start, end, -1, nil, true)
	return c, err
}

func (s *Server) ZRank(name, key string, limit int) (int, error) {
	return s.zRank(name, key, limit, false)
}

func (s *Server) ZRevRank(name, key string, limit int) (int, error) {
	return s.zRank(name, key, limit, true)
}

func (s *Server) ZRange(name string, start, end int) ([]Pair, error) {
	return s.doZRange(name, start, end, nil)
}

func (s *Server) doZRange(name string, start, end int, delete []byte) ([]Pair, error) {
	p, _, err := s.rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{OffsetStart: start, OffsetEnd: end, DeleteLog: delete})
	return p, err
}

func (s *Server) ZRevRange(name string, start, end int) ([]Pair, error) {
	p, _, err := s.rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{OffsetStart: -end - 1, OffsetEnd: -start - 1})
	return reversePairs(p), err
}

func (s *Server) ZRangeByLex(name string, start, end string) ([]Pair, error) {
	return s.doZRangeByLex(name, start, end, nil)
}

func (s *Server) doZRangeByLex(name string, start, end string, delete []byte) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromString(start)
	rangeEnd := (RangeLimit{}).fromString(end)
	p, _, err := s.rangeLex(name, rangeStart, rangeEnd, RangeOptions{OffsetStart: 0, OffsetEnd: -1, DeleteLog: delete})
	return p, err
}

func (s *Server) ZRevRangeByLex(name string, start, end string) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromString(end)
	rangeEnd := (RangeLimit{}).fromString(start)
	p, _, err := s.rangeLex(name, rangeStart, rangeEnd, RangeOptions{OffsetStart: 0, OffsetEnd: -1})
	return reversePairs(p), err
}

func (s *Server) ZRangeByScore(name string, start, end string, limit int) ([]Pair, error) {
	p, _, err := s.doZRangeByScore(name, start, end, limit, nil, false)
	return p, err
}

func (s *Server) ZRevRangeByScore(name string, start, end string, limit int) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromFloatString(end)
	rangeEnd := (RangeLimit{}).fromFloatString(start)
	p, _, err := s.rangeScore(name, rangeStart, rangeEnd, RangeOptions{OffsetStart: 0, OffsetEnd: -1, Limit: limit})
	return reversePairs(p), err
}

func (s *Server) doZRangeByScore(name string, start, end string, limit int, delete []byte, countOnly bool) ([]Pair, int, error) {
	rangeStart := (RangeLimit{}).fromFloatString(start)
	rangeEnd := (RangeLimit{}).fromFloatString(end)
	p, c, err := s.rangeScore(name, rangeStart, rangeEnd, RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   -1,
		DeleteLog:   delete,
		CountOnly:   countOnly,
		Limit:       limit,
	})
	// fmt.Println(start, end, rangeStart, rangeEnd, p)
	return p, c, err
}

func (s *Server) ZRemRangeByRank(name string, start, end int, dd []byte) ([]Pair, error) {
	return s.doZRange(name, start, end, dd)
}

func (s *Server) ZRemRangeByLex(name string, start, end string, dd []byte) ([]Pair, error) {
	return s.doZRangeByLex(name, start, end, dd)
}

func (s *Server) ZRemRangeByScore(name string, start, end string, dd []byte) ([]Pair, error) {
	p, _, err := s.doZRangeByScore(name, start, end, -1, dd, false)
	return p, err
}

func (s *Server) rangeLex(name string, start, end RangeLimit, opt RangeOptions) (pairs []Pair, count int, err error) {
	f := func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset." + name))
		if bk == nil {
			return nil
		}

		startBuf, endBuf := []byte(start.Value), []byte(end.Value)
		opt.translateOffset(bk)

		if !start.Inclusive {
			startBuf = append(startBuf, 0)
		}

		endFlag := 0
		if !end.Inclusive {
			endFlag = -1
		}

		c := bk.Cursor()
		k, sc := c.Seek(startBuf)

		for i := 0; len(pairs) < s.HardLimit; i++ {
			if len(sc) > 0 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) <= endFlag {
				if i >= opt.OffsetStart {
					if i <= opt.OffsetEnd {
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
			return s.deletePair(tx, name, pairs, opt.DeleteLog)
		}
		return nil
	}
	if len(opt.DeleteLog) > 0 {
		err = s.pick(name).Update(f)
	} else {
		err = s.pick(name).View(f)
	}
	return
}

func (s *Server) rangeScore(name string, start, end RangeLimit, opt RangeOptions) (pairs []Pair, count int, err error) {
	f := func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset.score." + name))
		if bk == nil {
			return nil
		}

		startBuf, endBuf := floatToBytes(start.Float), floatToBytes(end.Float)
		opt.translateOffset(bk)

		if !start.Inclusive {
			startBuf = floatBytesStep(startBuf, 1)
		}
		if !end.Inclusive {
			endBuf = floatBytesStep(endBuf, -1)
		}
		endBuf = append(endBuf, 0xff)

		c := bk.Cursor()
		k, _ := c.Seek(startBuf)

		limit := s.HardLimit
		if opt.Limit > 0 && opt.Limit < s.HardLimit {
			limit = opt.Limit
		}

		for i := 0; len(pairs) < limit; i++ {
			if len(k) >= 8 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) <= 0 {
				if i >= opt.OffsetStart {
					if i <= opt.OffsetEnd {
						if !opt.CountOnly {
							p := Pair{Key: string(k[8:]), Score: bytesToFloat(k[:8])}
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
			k, _ = c.Next()
		}
		if len(opt.DeleteLog) > 0 {
			return s.deletePair(tx, name, pairs, opt.DeleteLog)
		}
		return nil
	}
	if len(opt.DeleteLog) > 0 {
		err = s.pick(name).Update(f)
	} else {
		err = s.pick(name).View(f)
	}
	return
}

func (o *RangeOptions) translateOffset(bk *bbolt.Bucket) {
	n := bk.Stats().KeyN
	if o.OffsetStart < 0 {
		o.OffsetStart += n
	}
	if o.OffsetEnd < 0 {
		o.OffsetEnd += n
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
