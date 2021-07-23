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
	_, c, err := s.doZRangeByScore(name, start, end, -1, nil, true, false)
	return c, err
}

func (s *Server) ZRank(name, key string, limit int) (int, error) {
	return s.zRank(name, key, limit, false)
}

func (s *Server) ZRevRank(name, key string, limit int) (int, error) {
	return s.zRank(name, key, limit, true)
}

func (s *Server) ZRange(name string, start, end int, withData bool) ([]Pair, error) {
	return s.doZRange(name, start, end, nil, withData)
}

func (s *Server) ZRevRange(name string, start, end int, withData bool) ([]Pair, error) {
	p, _, err := s.rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{
		OffsetStart: -end - 1,
		OffsetEnd:   -start - 1,
		WithData:    withData,
	})
	return reversePairs(p), err
}

func (s *Server) ZRangeByLex(name string, start, end string) ([]Pair, error) {
	return s.doZRangeByLex(name, start, end, nil)
}

func (s *Server) ZRevRangeByLex(name string, start, end string) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromString(end)
	rangeEnd := (RangeLimit{}).fromString(start)
	p, _, err := s.rangeLex(name, rangeStart, rangeEnd, RangeOptions{OffsetStart: 0, OffsetEnd: -1})
	return reversePairs(p), err
}

func (s *Server) ZRangeByScore(name string, start, end string, limit int, withData bool) ([]Pair, error) {
	p, _, err := s.doZRangeByScore(name, start, end, limit, nil, false, withData)
	return p, err
}

func (s *Server) ZRevRangeByScore(name string, start, end string, limit int, withData bool) ([]Pair, error) {
	rangeStart, err := (RangeLimit{}).fromFloatString(end)
	if err != nil {
		return nil, err
	}
	rangeEnd, err := (RangeLimit{}).fromFloatString(start)
	if err != nil {
		return nil, err
	}
	p, _, err := s.rangeScore(name, rangeStart, rangeEnd, RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   -1,
		Limit:       limit,
		WithData:    withData,
	})
	return reversePairs(p), err
}

func (s *Server) ZRemRangeByRank(name string, start, end int, dd []byte) ([]Pair, error) {
	return s.doZRange(name, start, end, dd, false)
}

func (s *Server) ZRemRangeByLex(name string, start, end string, dd []byte) ([]Pair, error) {
	return s.doZRangeByLex(name, start, end, dd)
}

func (s *Server) ZRemRangeByScore(name string, start, end string, dd []byte) ([]Pair, error) {
	p, _, err := s.doZRangeByScore(name, start, end, -1, dd, false, false)
	return p, err
}

func (s *Server) doZRange(name string, start, end int, delete []byte, withData bool) ([]Pair, error) {
	p, _, err := s.rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{
		OffsetStart: start,
		OffsetEnd:   end,
		DeleteLog:   delete,
		WithData:    withData,
	})
	return p, err
}

func (s *Server) doZRangeByLex(name string, start, end string, delete []byte) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromString(start)
	rangeEnd := (RangeLimit{}).fromString(end)
	p, _, err := s.rangeLex(name, rangeStart, rangeEnd, RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   -1,
		DeleteLog:   delete,
	})
	return p, err
}

func (s *Server) doZRangeByScore(name string, start, end string, limit int, delete []byte, countOnly, withData bool) ([]Pair, int, error) {
	rangeStart, err := (RangeLimit{}).fromFloatString(start)
	if err != nil {
		return nil, 0, err
	}
	rangeEnd, err := (RangeLimit{}).fromFloatString(end)
	if err != nil {
		return nil, 0, err
	}
	p, c, err := s.rangeScore(name, rangeStart, rangeEnd, RangeOptions{
		OffsetStart: 0,
		OffsetEnd:   -1,
		DeleteLog:   delete,
		CountOnly:   countOnly,
		WithData:    withData,
		Limit:       limit,
	})
	return p, c, err
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
			// fmt.Println(k, startBuf, endBuf)
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

		if end.Inclusive {
			endBuf = floatBytesStep(endBuf, 1)
		}

		c := bk.Cursor()
		k, dataBuf := c.Seek(startBuf)

		limit := s.HardLimit
		if opt.Limit > 0 && opt.Limit < s.HardLimit {
			limit = opt.Limit
		}

		for i := 0; len(pairs) < limit; i++ {
			if len(k) >= 8 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) < 0 {
				if i >= opt.OffsetStart {
					if i <= opt.OffsetEnd {
						if !opt.CountOnly {
							p := Pair{
								Key:   string(k[8:]),
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
