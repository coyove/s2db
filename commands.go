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
		count = bk.Stats().KeyN
		return nil
	})
	return int64(count), err
}

func (s *Server) Del(name string, dd []byte) (count int, err error) {
	err = s.pick(name).Update(func(tx *bbolt.Tx) error {
		bkName := tx.Bucket([]byte("zset." + name))
		bkScore := tx.Bucket([]byte("zset.score." + name))
		if bkName == nil || bkScore == nil {
			return nil
		}
		if err := tx.DeleteBucket([]byte("zset." + name)); err != nil {
			return err
		}
		if err := tx.DeleteBucket([]byte("zset.score." + name)); err != nil {
			return err
		}
		count++
		return s.writeLog(tx, dd)
	})
	return
}

// ZAdd
// :scoreGt new score must be larger than old score + scoreGt
func (s *Server) ZAdd(name string, pairs []Pair, nx, xx bool, scoreGt float64, dd []byte) (added, updated int, err error) {
	err = s.pick(name).Update(func(tx *bbolt.Tx) error {
		bkName, err := tx.CreateBucketIfNotExists([]byte("zset." + name))
		if err != nil {
			return err
		}
		bkScore, err := tx.CreateBucketIfNotExists([]byte("zset.score." + name))
		if err != nil {
			return err
		}
		for _, p := range pairs {
			if err := checkScore(p.Score); err != nil {
				return err
			}
			scoreBuf := bkName.Get([]byte(p.Key))
			if len(scoreBuf) != 0 {
				// old key exists
				if nx {
					continue
				}
				if !math.IsNaN(scoreGt) && p.Score <= bytesToFloat(scoreBuf)+scoreGt {
					continue
				}
				if err := bkScore.Delete([]byte(string(scoreBuf) + p.Key)); err != nil {
					return err
				}
				if p.Score != bytesToFloat(scoreBuf) {
					updated++
				}
			} else {
				// we are adding a new key
				if xx {
					continue
				}
				if !math.IsNaN(scoreGt) && p.Score <= scoreGt {
					continue
				}
				added++
			}
			scoreBuf = floatToBytes(p.Score)
			if err := bkName.Put([]byte(p.Key), scoreBuf); err != nil {
				return err
			}
			if err := bkScore.Put([]byte(string(scoreBuf)+p.Key), p.Data); err != nil {
				return err
			}
		}
		return s.writeLog(tx, dd)
	})
	return
}

func (s *Server) ZRem(name string, keys []string, dd []byte) (count int, err error) {
	err = s.pick(name).Update(func(tx *bbolt.Tx) error {
		bkName := tx.Bucket([]byte("zset." + name))
		if bkName == nil {
			return nil
		}
		pairs := []Pair{}
		for _, key := range keys {
			scoreBuf := bkName.Get([]byte(key))
			if len(scoreBuf) == 0 {
				continue
			}
			pairs = append(pairs, Pair{Key: key, Score: bytesToFloat(scoreBuf)})
			count++
		}
		if len(pairs) == 0 {
			return nil
		}
		return s.deletePair(tx, name, pairs, dd)
	})
	return
}

func (s *Server) ZMScore(name string, keys ...string) (scores []float64, err error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing keys")
	}
	for range keys {
		scores = append(scores, math.NaN())
	}
	err = s.pick(name).Update(func(tx *bbolt.Tx) error {
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
	err = s.pick(name).Update(func(tx *bbolt.Tx) error {
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

func (s *Server) ZIncrBy(name string, key string, by float64, dd []byte) (newValue float64, err error) {
	err = s.pick(name).Update(func(tx *bbolt.Tx) error {
		bkName, err := tx.CreateBucketIfNotExists([]byte("zset." + name))
		if err != nil {
			return err
		}
		bkScore, err := tx.CreateBucketIfNotExists([]byte("zset.score." + name))
		if err != nil {
			return err
		}
		scoreBuf := bkName.Get([]byte(key))
		score := 0.0

		var dataBuf []byte
		if len(scoreBuf) != 0 {
			oldKey := []byte(string(scoreBuf) + key)
			dataBuf = append([]byte{}, bkScore.Get(oldKey)...)
			if err := bkScore.Delete(oldKey); err != nil {
				return err
			}
			score = bytesToFloat(scoreBuf)
		} else {
			dataBuf = []byte("")
		}

		if by == 0 {
			if len(scoreBuf) == 0 {
				// special case: zincrby name 0 non_existed_key
			} else {
				newValue = score
				return nil
			}
		}
		if err := checkScore(score + by); err != nil {
			return err
		}
		scoreBuf = floatToBytes(score + by)
		if err := bkName.Put([]byte(key), scoreBuf); err != nil {
			return err
		}
		if err := bkScore.Put([]byte(string(scoreBuf)+key), dataBuf); err != nil {
			return err
		}
		newValue = score + by
		return s.writeLog(tx, dd)
	})
	return
}
