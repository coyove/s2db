package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/coyove/s2db/internal"
	"go.etcd.io/bbolt"
)

func prepareDel(name string, dd []byte) func(tx *bbolt.Tx) (count interface{}, err error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		bkName := tx.Bucket([]byte("zset." + name))
		bkScore := tx.Bucket([]byte("zset.score." + name))
		if bkName == nil || bkScore == nil {
			bkQ := tx.Bucket([]byte("q." + name))
			if bkQ == nil {
				return 0, writeLog(tx, dd)
			}
			if bkQ.KeyN() > 65536 {
				return 0, fmt.Errorf("too many members to delete, use 'unlink' instead")
			}
			if err := tx.DeleteBucket([]byte("q." + name)); err != nil {
				return 0, err
			}
			return 1, writeLog(tx, dd)
		}
		if bkName.KeyN() > 65536 {
			return 0, fmt.Errorf("too many members to delete, use 'unlink' instead")
		}
		if err := tx.DeleteBucket([]byte("zset." + name)); err != nil {
			return 0, err
		}
		if err := tx.DeleteBucket([]byte("zset.score." + name)); err != nil {
			return 0, err
		}
		return 1, writeLog(tx, dd)
	}
}

func prepareZAdd(name string, pairs []internal.Pair, nx, xx, ch bool, fillPercent float64, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		bkName, err := tx.CreateBucketIfNotExists([]byte("zset." + name))
		if err != nil {
			return nil, err
		}
		bkScore, err := tx.CreateBucketIfNotExists([]byte("zset.score." + name))
		if err != nil {
			return nil, err
		}

		if fillPercent > 0 && fillPercent < 1 {
			bkName.FillPercent = fillPercent
			bkScore.FillPercent = fillPercent
		}

		added, updated := 0, 0
		for _, p := range pairs {
			if err := checkScore(p.Score); err != nil {
				return nil, err
			}
			scoreBuf := bkName.Get([]byte(p.Member))
			if len(scoreBuf) != 0 {
				// old key exists
				if nx {
					continue
				}
				if err := bkScore.Delete([]byte(string(scoreBuf) + p.Member)); err != nil {
					return nil, err
				}
				if p.Score != internal.BytesToFloat(scoreBuf) {
					updated++
				}
			} else {
				// we are adding a new key
				if xx {
					continue
				}
				added++
			}
			scoreBuf = internal.FloatToBytes(p.Score)
			if err := bkName.Put([]byte(p.Member), scoreBuf); err != nil {
				return nil, err
			}
			if err := bkScore.Put([]byte(string(scoreBuf)+p.Member), p.Data); err != nil {
				return nil, err
			}
		}
		if ch {
			return added + updated, writeLog(tx, dd)
		}
		return added, writeLog(tx, dd)
	}
}

func prepareZRem(name string, keys []string, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (count interface{}, err error) {
		bkName := tx.Bucket([]byte("zset." + name))
		if bkName == nil {
			return 0, writeLog(tx, dd)
		}
		var pairs []internal.Pair
		for _, key := range keys {
			scoreBuf := bkName.Get([]byte(key))
			if len(scoreBuf) == 0 {
				continue
			}
			pairs = append(pairs, internal.Pair{Member: key, Score: internal.BytesToFloat(scoreBuf)})
		}
		return len(pairs), deletePair(tx, name, pairs, dd)
	}
}

func prepareZIncrBy(name string, key string, by float64, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (newValue interface{}, err error) {
		bkName, err := tx.CreateBucketIfNotExists([]byte("zset." + name))
		if err != nil {
			return 0, err
		}
		bkScore, err := tx.CreateBucketIfNotExists([]byte("zset.score." + name))
		if err != nil {
			return 0, err
		}
		scoreBuf := bkName.Get([]byte(key))
		score := 0.0

		var dataBuf []byte
		if len(scoreBuf) != 0 {
			oldKey := []byte(string(scoreBuf) + key)
			dataBuf = append([]byte{}, bkScore.Get(oldKey)...)
			if err := bkScore.Delete(oldKey); err != nil {
				return 0, err
			}
			score = internal.BytesToFloat(scoreBuf)
		} else {
			dataBuf = []byte("")
		}

		if by == 0 {
			_ = "special case: zincrby name 0 non_existed_key"
		}
		if err := checkScore(score + by); err != nil {
			return 0, err
		}
		scoreBuf = internal.FloatToBytes(score + by)
		if err := bkName.Put([]byte(key), scoreBuf); err != nil {
			return 0, err
		}
		if err := bkScore.Put([]byte(string(scoreBuf)+key), dataBuf); err != nil {
			return 0, err
		}
		return score + by, writeLog(tx, dd)
	}
}

func prepareZRemRangeByRank(name string, start, end int, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		_, c, err := rangeScore(name, MinScoreRange, MaxScoreRange, internal.RangeOptions{
			OffsetStart: start,
			OffsetEnd:   end,
			DeleteLog:   dd,
			Limit:       internal.RangeHardLimit,
			Append:      internal.DefaultRangeAppend,
		})(tx)
		return c, err
	}
}

func prepareZRemRangeByLex(name string, start, end string, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		rangeStart := internal.NewRLFromString(start)
		rangeEnd := internal.NewRLFromString(end)
		_, c, err := rangeLex(name, rangeStart, rangeEnd, internal.RangeOptions{
			OffsetStart: 0,
			OffsetEnd:   math.MaxInt64,
			DeleteLog:   dd,
			Limit:       internal.RangeHardLimit,
			Append:      internal.DefaultRangeAppend,
		})(tx)
		return c, err
	}
}

func prepareZRemRangeByScore(name string, start, end string, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	rangeStart, err := internal.NewRLFromFloatString(start)
	internal.PanicErr(err)
	rangeEnd, err := internal.NewRLFromFloatString(end)
	internal.PanicErr(err)
	return func(tx *bbolt.Tx) (interface{}, error) {
		_, c, err := rangeScore(name, rangeStart, rangeEnd, internal.RangeOptions{
			OffsetStart: 0,
			OffsetEnd:   math.MaxInt64,
			DeleteLog:   dd,
			Limit:       internal.RangeHardLimit,
			Append:      internal.DefaultRangeAppend,
		})(tx)
		return c, err
	}
}

func prepareQAppend(name string, value []byte, max int64, appender func(string) bool, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		bk, err := tx.CreateBucketIfNotExists([]byte("q." + name))
		if err != nil {
			return nil, err
		}

		var xid uint64
		if bytes.EqualFold(value, []byte("--TRIM--")) {
			// QAPPEND <Name> --TRIM-- <Max> is a trick to trim the head of a queue
			xid = bk.Sequence()
		} else {
			if appender != nil {
				if _, v := bk.Cursor().Last(); len(v) > 0 && !appender(*(*string)(unsafe.Pointer(&v))) {
					return int64(0), nil
				}
			}

			id, err := bk.NextSequence()
			if err != nil {
				return nil, err
			}

			xid = id
			bk.FillPercent = 0.9

			key := make([]byte, 16)
			binary.BigEndian.PutUint64(key, id)
			binary.BigEndian.PutUint64(key[8:], uint64(time.Now().UnixNano()))

			if err := bk.Put(key, value); err != nil {
				return nil, err
			}
		}

		if max > 0 {
			c := bk.Cursor()
			for _, _, n := qLenImpl(bk); n > max; n-- {
				k, _ := c.First()
				if len(k) != 16 {
					break
				}
				if err := bk.Delete(k); err != nil {
					return nil, err
				}
			}
		}
		return int64(xid), writeLog(tx, dd)
	}
}
