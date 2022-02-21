package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unsafe"

	s2pkg "github.com/coyove/s2db/s2pkg"
	"go.etcd.io/bbolt"
)

var ErrBigDelete = fmt.Errorf("can't delete big keys directly, use 'UNLINK key' command")

func prepareDel(key string, dd []byte) func(tx *bbolt.Tx) (count interface{}, err error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		bkName := tx.Bucket([]byte("zset." + key))
		bkScore := tx.Bucket([]byte("zset.score." + key))
		if bkName == nil || bkScore == nil {
			bkQ := tx.Bucket([]byte("q." + key))
			if bkQ == nil {
				return 0, writeLog(tx, dd)
			}
			if _, _, l := queueLenImpl(bkQ); l > 65536 {
				return 0, ErrBigDelete
			}
			if err := tx.DeleteBucket([]byte("q." + key)); err != nil {
				return 0, err
			}
			return 1, writeLog(tx, dd)
		}
		if bkScore.Sequence() > 65536 {
			return 0, ErrBigDelete
		}
		if err := tx.DeleteBucket([]byte("zset." + key)); err != nil {
			return 0, err
		}
		if err := tx.DeleteBucket([]byte("zset.score." + key)); err != nil {
			return 0, err
		}
		return 1, writeLog(tx, dd)
	}
}

func prepareZAdd(key string, pairs []s2pkg.Pair, nx, xx, ch bool, fillPercent float64, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		bkName, err := tx.CreateBucketIfNotExists([]byte("zset." + key))
		if err != nil {
			return nil, err
		}
		bkScore, err := tx.CreateBucketIfNotExists([]byte("zset.score." + key))
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
				if p.Score != s2pkg.BytesToFloat(scoreBuf) {
					updated++
				}
			} else {
				// we are adding a new key
				if xx {
					continue
				}
				added++
			}
			scoreBuf = s2pkg.FloatToBytes(p.Score)
			if err := bkName.Put([]byte(p.Member), scoreBuf); err != nil {
				return nil, err
			}
			if err := bkScore.Put([]byte(string(scoreBuf)+p.Member), p.Data); err != nil {
				return nil, err
			}
		}

		bkScore.SetSequence(bkScore.Sequence() + uint64(added))
		if ch {
			return added + updated, writeLog(tx, dd)
		}
		return added, writeLog(tx, dd)
	}
}

func prepareZRem(key string, keys []string, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (count interface{}, err error) {
		bkName := tx.Bucket([]byte("zset." + key))
		if bkName == nil {
			return 0, writeLog(tx, dd)
		}
		var pairs []s2pkg.Pair
		for _, key := range keys {
			scoreBuf := bkName.Get([]byte(key))
			if len(scoreBuf) == 0 {
				continue
			}
			pairs = append(pairs, s2pkg.Pair{Member: key, Score: s2pkg.BytesToFloat(scoreBuf)})
		}
		return len(pairs), deletePair(tx, key, pairs, dd)
	}
}

func prepareZIncrBy(key string, member string, by float64, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (newValue interface{}, err error) {
		bkName, err := tx.CreateBucketIfNotExists([]byte("zset." + key))
		if err != nil {
			return 0, err
		}
		bkScore, err := tx.CreateBucketIfNotExists([]byte("zset.score." + key))
		if err != nil {
			return 0, err
		}
		scoreBuf := bkName.Get([]byte(member))
		score := 0.0
		added := false

		var dataBuf []byte
		if len(scoreBuf) != 0 {
			oldKey := []byte(string(scoreBuf) + member)
			dataBuf = append([]byte{}, bkScore.Get(oldKey)...)
			if err := bkScore.Delete(oldKey); err != nil {
				return 0, err
			}
			score = s2pkg.BytesToFloat(scoreBuf)
		} else {
			dataBuf = []byte("")
			added = true
		}

		if by == 0 {
			_ = "special case: zincrby name 0 non_existed_key"
		}
		if err := checkScore(score + by); err != nil {
			return 0, err
		}
		scoreBuf = s2pkg.FloatToBytes(score + by)
		if err := bkName.Put([]byte(member), scoreBuf); err != nil {
			return 0, err
		}
		if err := bkScore.Put([]byte(string(scoreBuf)+member), dataBuf); err != nil {
			return 0, err
		}
		if added {
			bkScore.SetSequence(bkScore.Sequence() + 1)
		}
		return score + by, writeLog(tx, dd)
	}
}

func prepareZRemRangeByRank(key string, start, end int, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		_, c, err := rangeScore(key, MinScoreRange, MaxScoreRange, s2pkg.RangeOptions{
			OffsetStart: start,
			OffsetEnd:   end,
			DeleteLog:   dd,
			Limit:       s2pkg.RangeHardLimit,
			Append:      s2pkg.DefaultRangeAppend,
		})(tx)
		return c, err
	}
}

func prepareZRemRangeByLex(key string, start, end string, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		rangeStart := s2pkg.NewLexRL(start)
		rangeEnd := s2pkg.NewLexRL(end)
		_, c, err := rangeLex(key, rangeStart, rangeEnd, s2pkg.RangeOptions{
			OffsetStart: 0,
			OffsetEnd:   math.MaxInt64,
			DeleteLog:   dd,
			Limit:       s2pkg.RangeHardLimit,
			Append:      s2pkg.DefaultRangeAppend,
		})(tx)
		return c, err
	}
}

func prepareZRemRangeByScore(key string, start, end string, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		_, c, err := rangeScore(key, s2pkg.NewScoreRL(start), s2pkg.NewScoreRL(end), s2pkg.RangeOptions{
			OffsetStart: 0,
			OffsetEnd:   math.MaxInt64,
			DeleteLog:   dd,
			Limit:       s2pkg.RangeHardLimit,
			Append:      s2pkg.DefaultRangeAppend,
		})(tx)
		return c, err
	}
}

func prepareQAppend(key string, value []byte, max, ts int64, appender func(string) bool, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		bk, err := tx.CreateBucketIfNotExists([]byte("q." + key))
		if err != nil {
			return nil, err
		}

		var xid uint64
		if bytes.EqualFold(value, []byte("--TRIM--")) {
			// QAPPEND <Name> --TRIM-- COUNT <Max> is a trick to trim the head of a queue
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

			if ts == 0 {
				ts = time.Now().UnixNano()
			}
			binary.BigEndian.PutUint64(key[8:], uint64(ts))

			if err := bk.Put(key, value); err != nil {
				return nil, err
			}
		}

		if max > 0 {
			c := bk.Cursor()
			for _, _, n := queueLenImpl(bk); n > max; n-- {
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
