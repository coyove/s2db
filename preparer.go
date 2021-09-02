package main

import (
	"encoding/binary"
	"fmt"
	"math"

	"go.etcd.io/bbolt"
)

func prepareDel(name string, dd []byte) func(tx *bbolt.Tx) (count interface{}, err error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		bkName := tx.Bucket([]byte("zset." + name))
		bkScore := tx.Bucket([]byte("zset.score." + name))
		if bkName == nil || bkScore == nil {
			return 0, nil
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

func prepareZAdd(name string, pairs []Pair, nx, xx, ch bool, fillPercent int, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		bkName, err := tx.CreateBucketIfNotExists([]byte("zset." + name))
		if err != nil {
			return nil, err
		}
		bkScore, err := tx.CreateBucketIfNotExists([]byte("zset.score." + name))
		if err != nil {
			return nil, err
		}

		if fillPercent > 0 && fillPercent <= 10 {
			x := float64(fillPercent) / 10
			bkName.FillPercent = x
			bkScore.FillPercent = x
		}

		added, updated := 0, 0
		for _, p := range pairs {
			if err := checkScore(p.Score); err != nil {
				return nil, err
			}
			scoreBuf := bkName.Get([]byte(p.Key))
			if len(scoreBuf) != 0 {
				// old key exists
				if nx {
					continue
				}
				if err := bkScore.Delete([]byte(string(scoreBuf) + p.Key)); err != nil {
					return nil, err
				}
				if p.Score != bytesToFloat(scoreBuf) {
					updated++
				}
			} else {
				// we are adding a new key
				if xx {
					continue
				}
				added++
			}
			scoreBuf = floatToBytes(p.Score)
			if err := bkName.Put([]byte(p.Key), scoreBuf); err != nil {
				return nil, err
			}
			if err := bkScore.Put([]byte(string(scoreBuf)+p.Key), p.Data); err != nil {
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
			return 0, nil
		}
		pairs := []Pair{}
		for _, key := range keys {
			scoreBuf := bkName.Get([]byte(key))
			if len(scoreBuf) == 0 {
				continue
			}
			pairs = append(pairs, Pair{Key: key, Score: bytesToFloat(scoreBuf)})
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
			score = bytesToFloat(scoreBuf)
		} else {
			dataBuf = []byte("")
		}

		if by == 0 {
			if len(scoreBuf) == 0 {
				// special case: zincrby name 0 non_existed_key
			} else {
				return score, nil
			}
		}
		if err := checkScore(score + by); err != nil {
			return 0, err
		}
		scoreBuf = floatToBytes(score + by)
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
		_, c, err := rangeScore(name, MinScoreRange, MaxScoreRange, RangeOptions{
			OffsetStart: start,
			OffsetEnd:   end,
			DeleteLog:   dd,
		})(tx)
		return c, err
	}
}

func prepareZRemRangeByLex(name string, start, end string, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		rangeStart := (RangeLimit{}).fromString(start)
		rangeEnd := (RangeLimit{}).fromString(end)
		_, c, err := rangeLex(name, rangeStart, rangeEnd, RangeOptions{
			OffsetStart: 0,
			OffsetEnd:   math.MaxInt64,
			DeleteLog:   dd,
		})(tx)
		return c, err
	}
}

func prepareZRemRangeByScore(name string, start, end string, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	rangeStart, err := (RangeLimit{}).fromFloatString(start)
	if err != nil {
		panic(err)
	}
	rangeEnd, err := (RangeLimit{}).fromFloatString(end)
	if err != nil {
		panic(err)
	}
	return func(tx *bbolt.Tx) (interface{}, error) {
		_, c, err := rangeScore(name, rangeStart, rangeEnd, RangeOptions{
			OffsetStart: 0,
			OffsetEnd:   math.MaxInt64,
			DeleteLog:   dd,
		})(tx)
		return c, err
	}
}

func prepareQAppend(name string, msec float64, value []byte, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		bk, err := tx.CreateBucketIfNotExists([]byte("q." + name))
		if err != nil {
			return nil, err
		}
		id, err := bk.NextSequence()
		if err != nil {
			return nil, err
		}

		key := make([]byte, 16)
		binary.BigEndian.PutUint64(key, id)
		binary.BigEndian.PutUint64(key[8:], floatToInternalUint64(msec))

		bk.FillPercent = 0.9
		if err := bk.Put(key, value); err != nil {
			return nil, err
		}
		return int64(id), writeLog(tx, dd)
	}
}
