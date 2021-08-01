package main

import (
	"fmt"
	"math"

	"go.etcd.io/bbolt"
)

func (s *Server) pick(name string) *bbolt.DB {
	return s.db[hashStr(name)%uint64(len(s.db))].DB
}

func (s *Server) writeLog(tx *bbolt.Tx, dd []byte) error {
	bkWal, err := tx.CreateBucketIfNotExists([]byte("wal"))
	if err != nil {
		return err
	}
	id, _ := bkWal.NextSequence()
	return bkWal.Put(intToBytes(id), dd)
}

func (s *Server) ZCard(name string) (int64, error) {
	count := 0
	err := s.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset." + name))
		if bk == nil {
			return nil
		}
		count = bk.KeyN()
		return nil
	})
	return int64(count), err
}

func (s *Server) prepareDel(name string, dd []byte) func(tx *bbolt.Tx) (count interface{}, err error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		bkName := tx.Bucket([]byte("zset." + name))
		bkScore := tx.Bucket([]byte("zset.score." + name))
		if bkName == nil || bkScore == nil {
			return 0, nil
		}
		if err := tx.DeleteBucket([]byte("zset." + name)); err != nil {
			return 0, err
		}
		if err := tx.DeleteBucket([]byte("zset.score." + name)); err != nil {
			return 0, err
		}
		return 1, s.writeLog(tx, dd)
	}
}

func (s *Server) prepareZAdd(name string, pairs []Pair, nx, xx, ch bool, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
	return func(tx *bbolt.Tx) (interface{}, error) {
		bkName, err := tx.CreateBucketIfNotExists([]byte("zset." + name))
		if err != nil {
			return nil, err
		}
		bkScore, err := tx.CreateBucketIfNotExists([]byte("zset.score." + name))
		if err != nil {
			return nil, err
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
			return added + updated, s.writeLog(tx, dd)
		}
		return added, s.writeLog(tx, dd)
	}
}

func (s *Server) prepareZRem(name string, keys []string, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
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
		if len(pairs) == 0 {
			return 0, nil
		}
		return len(pairs), s.deletePair(tx, name, pairs, dd)
	}
}

func (s *Server) prepareZIncrBy(name string, key string, by float64, dd []byte) func(tx *bbolt.Tx) (interface{}, error) {
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
		return score + by, s.writeLog(tx, dd)
	}
}

func (s *Server) ZMScore(name string, keys ...string) (scores []float64, err error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing keys")
	}
	for range keys {
		scores = append(scores, math.NaN())
	}
	err = s.pick(name).View(func(tx *bbolt.Tx) error {
		bkName := tx.Bucket([]byte("zset." + name))
		if bkName == nil {
			return nil
		}
		for i, key := range keys {
			scoreBuf := bkName.Get([]byte(key))
			if len(scoreBuf) != 0 {
				scores[i] = bytesToFloat(scoreBuf)
			}
		}
		return nil
	})
	return
}

func (s *Server) ZMData(name string, keys ...string) (data [][]byte, err error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing keys")
	}
	data = make([][]byte, len(keys))
	err = s.pick(name).View(func(tx *bbolt.Tx) error {
		bkName := tx.Bucket([]byte("zset." + name))
		if bkName == nil {
			return nil
		}
		bkScore := tx.Bucket([]byte("zset.score." + name))
		if bkScore == nil {
			return nil
		}
		for i, key := range keys {
			scoreBuf := bkName.Get([]byte(key))
			if len(scoreBuf) != 0 {
				d := bkScore.Get([]byte(string(scoreBuf) + keys[i]))
				data[i] = append([]byte{}, d...)
			}
		}
		return nil
	})
	return
}

func (s *Server) deletePair(tx *bbolt.Tx, name string, pairs []Pair, dd []byte) error {
	bkName := tx.Bucket([]byte("zset." + name))
	if bkName == nil {
		return nil
	}
	bkScore := tx.Bucket([]byte("zset.score." + name))
	if bkScore == nil {
		return nil
	}
	for _, p := range pairs {
		if err := bkName.Delete([]byte(p.Key)); err != nil {
			return err
		}
		if err := bkScore.Delete([]byte(string(floatToBytes(p.Score)) + p.Key)); err != nil {
			return err
		}
	}
	return s.writeLog(tx, dd)
}
