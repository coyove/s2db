package main

import (
	"bytes"

	"go.etcd.io/bbolt"
)

func (z *DB) ZCount(name string, start, end string) (int, error) {
	_, c, err := z.doZRangeByScore(name, start, end, false, true)
	return c, err
}

func (z *DB) ZRange(name string, start, end int) ([]Pair, error) {
	return z.doZRange(name, start, end, false)
}

func (z *DB) ZRemRangeByRank(name string, start, end int) ([]Pair, error) {
	return z.doZRange(name, start, end, true)
}

func (z *DB) doZRange(name string, start, end int, delete bool) ([]Pair, error) {
	p, _, err := z.rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{OffsetStart: start, OffsetEnd: end, Delete: delete})
	return p, err
}

func (z *DB) ZRevRange(name string, start, end int) ([]Pair, error) {
	p, _, err := z.rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{OffsetStart: -end - 1, OffsetEnd: -start - 1})
	return reversePairs(p), err
}

func (z *DB) ZRangeByLex(name string, start, end string) ([]Pair, error) {
	return z.doZRangeByLex(name, start, end, false)
}

func (z *DB) ZRemRangeByLex(name string, start, end string) ([]Pair, error) {
	return z.doZRangeByLex(name, start, end, true)
}

func (z *DB) doZRangeByLex(name string, start, end string, delete bool) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromString(start)
	rangeEnd := (RangeLimit{}).fromString(end)
	p, _, err := z.rangeLex(name, rangeStart, rangeEnd, RangeOptions{OffsetStart: 0, OffsetEnd: -1, Delete: delete})
	return p, err
}

func (z *DB) ZRevRangeByLex(name string, start, end string) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromString(end)
	rangeEnd := (RangeLimit{}).fromString(start)
	p, _, err := z.rangeLex(name, rangeStart, rangeEnd, RangeOptions{OffsetStart: 0, OffsetEnd: -1})
	return reversePairs(p), err
}

func (z *DB) ZRangeByScore(name string, start, end string) ([]Pair, error) {
	p, _, err := z.doZRangeByScore(name, start, end, false, false)
	return p, err
}

func (z *DB) ZRemRangeByScore(name string, start, end string) ([]Pair, error) {
	p, _, err := z.doZRangeByScore(name, start, end, true, false)
	return p, err
}

func (z *DB) doZRangeByScore(name string, start, end string, delete, countOnly bool) ([]Pair, int, error) {
	rangeStart := (RangeLimit{}).fromFloatString(start)
	rangeEnd := (RangeLimit{}).fromFloatString(end)
	p, c, err := z.rangeScore(name, rangeStart, rangeEnd, RangeOptions{OffsetStart: 0, OffsetEnd: -1, Delete: delete, CountOnly: countOnly})
	return p, c, err
}

func (z *DB) ZRevRangeByScore(name string, start, end string) ([]Pair, error) {
	rangeStart := (RangeLimit{}).fromFloatString(end)
	rangeEnd := (RangeLimit{}).fromFloatString(start)
	p, _, err := z.rangeScore(name, rangeStart, rangeEnd, RangeOptions{OffsetStart: 0, OffsetEnd: -1})
	return reversePairs(p), err
}

func (z *DB) rangeLex(name string, start, end RangeLimit, opt RangeOptions) (pairs []Pair, count int, err error) {
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
		k, s := c.Seek(startBuf)

		for i := 0; ; i++ {
			if len(s) > 0 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) <= endFlag {
				if i >= opt.OffsetStart {
					if i <= opt.OffsetEnd {
						if !opt.CountOnly {
							pairs = append(pairs, Pair{string(k), bytesToFloat(s)})
						}
						count++
					} else {
						break
					}
				}
			} else {
				break
			}
			k, s = c.Next()

		}

		if opt.Delete {
			return z.deletePair(tx, name, pairs...)
		}
		return nil
	}
	if opt.Delete {
		err = z.pick(name).Update(f)
	} else {
		err = z.pick(name).View(f)
	}
	return
}

func (z *DB) rangeScore(name string, start, end RangeLimit, opt RangeOptions) (pairs []Pair, count int, err error) {
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
		k, s := c.Seek(startBuf)

		for i := 0; ; i++ {
			if len(s) > 0 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) <= 0 {
				if i >= opt.OffsetStart {
					if i <= opt.OffsetEnd {
						if !opt.CountOnly {
							pairs = append(pairs, Pair{string(s), bytesToFloat(k[:16])})
						}
						count++
					} else {
						break
					}
				}
			} else {
				break
			}
			k, s = c.Next()

		}
		if opt.Delete {
			return z.deletePair(tx, name, pairs...)
		}
		return nil
	}
	if opt.Delete {
		err = z.pick(name).Update(f)
	} else {
		err = z.pick(name).View(f)
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
