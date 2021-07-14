package main

import (
	"bytes"

	"go.etcd.io/bbolt"
)

func (s *Server) ZCount(name string, start, end string) (int, error) {
	_, c, err := s.doZRangeByScore(name, start, end, false, true)
	return c, err
}

func (s *Server) ZRange(name string, start, end int) ([]Pair, error) {
	return s.doZRange(name, start, end, false)
}

func (s *Server) ZRemRangeByRank(name string, start, end int) ([]Pair, error) {
	return s.doZRange(name, start, end, true)
}

func (s *Server) doZRange(name string, start, end int, delete bool) ([]Pair, error) {
	p, _, err := s.rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{OffsetStart: start, OffsetEnd: end, Delete: delete})
	return p, err
}

func (s *Server) ZRevRange(name string, start, end int) ([]Pair, error) {
	p, _, err := s.rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{OffsetStart: -end - 1, OffsetEnd: -start - 1})
	return reversePairs(p), err
}

func (s *Server) ZRangeByLex(name string, start, end string) ([]Pair, error) {
	return s.doZRangeByLex(name, start, end, false)
}

func (s *Server) ZRemRangeByLex(name string, start, end string) ([]Pair, error) {
	return s.doZRangeByLex(name, start, end, true)
}

func (s *Server) doZRangeByLex(name string, start, end string, delete bool) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromString(start)
	rangeEnd := (RangeLimit{}).fromString(end)
	p, _, err := s.rangeLex(name, rangeStart, rangeEnd, RangeOptions{OffsetStart: 0, OffsetEnd: -1, Delete: delete})
	return p, err
}

func (s *Server) ZRevRangeByLex(name string, start, end string) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromString(end)
	rangeEnd := (RangeLimit{}).fromString(start)
	p, _, err := s.rangeLex(name, rangeStart, rangeEnd, RangeOptions{OffsetStart: 0, OffsetEnd: -1})
	return reversePairs(p), err
}

func (s *Server) ZRangeByScore(name string, start, end string) ([]Pair, error) {
	p, _, err := s.doZRangeByScore(name, start, end, false, false)
	return p, err
}

func (s *Server) ZRemRangeByScore(name string, start, end string) ([]Pair, error) {
	p, _, err := s.doZRangeByScore(name, start, end, true, false)
	return p, err
}

func (s *Server) doZRangeByScore(name string, start, end string, delete, countOnly bool) ([]Pair, int, error) {
	rangeStart := (RangeLimit{}).fromFloatString(start)
	rangeEnd := (RangeLimit{}).fromFloatString(end)
	p, c, err := s.rangeScore(name, rangeStart, rangeEnd, RangeOptions{OffsetStart: 0, OffsetEnd: -1, Delete: delete, CountOnly: countOnly})
	// fmt.Println(start, end, rangeStart, rangeEnd, p)
	return p, c, err
}

func (s *Server) ZRevRangeByScore(name string, start, end string) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromFloatString(end)
	rangeEnd := (RangeLimit{}).fromFloatString(start)
	p, _, err := s.rangeScore(name, rangeStart, rangeEnd, RangeOptions{OffsetStart: 0, OffsetEnd: -1})
	return reversePairs(p), err
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
							pairs = append(pairs, Pair{string(k), bytesToFloat(sc)})
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

		if opt.Delete {
			return s.deletePair(tx, name, pairs...)
		}
		return nil
	}
	if opt.Delete {
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
		k, sc := c.Seek(startBuf)

		for i := 0; len(pairs) < s.HardLimit; i++ {
			if len(sc) > 0 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) <= 0 {
				if i >= opt.OffsetStart {
					if i <= opt.OffsetEnd {
						if !opt.CountOnly {
							pairs = append(pairs, Pair{string(sc), bytesToFloat(k[:8])})
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
		if opt.Delete {
			return s.deletePair(tx, name, pairs...)
		}
		return nil
	}
	if opt.Delete {
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
