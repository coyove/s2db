package main

import (
	"fmt"
	"math"

	"go.etcd.io/bbolt"
)

func (s *Server) pick(name string) *bbolt.DB {
	return s.db[hashStr(name)%uint64(len(s.db))].DB
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

func (s *Server) GroupKeys(names ...string) [][]string {
	nameShards := make([][]string, len(s.db))
	for _, name := range names {
		x := &nameShards[hashStr(name)%uint64(len(s.db))]
		*x = append(*x, name)
	}
	return nameShards
}

func (s *Server) DelGroupedKeys(names ...string) (count int, err error) {
	if len(names) == 0 {
		return
	}
	db := s.pick(names[0])
	for i := 1; i < len(names); i++ {
		if s.pick(names[i]) != db {
			return 0, fmt.Errorf("keys not grouped")
		}
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, name := range names {
			bkName := tx.Bucket([]byte("zset." + name))
			bkScore := tx.Bucket([]byte("zset.score." + name))
			if bkName == nil || bkScore == nil {
				continue
			}
			if err := tx.DeleteBucket([]byte("zset." + name)); err != nil {
				return err
			}
			if err := tx.DeleteBucket([]byte("zset.score." + name)); err != nil {
				return err
			}
			count++
		}
		return nil
	})
	return
}

func (s *Server) ZAdd(name string, pairs []Pair, nx, xx bool) (added, updated int, err error) {
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
				if bytesToFloat(scoreBuf) == p.Score {
					continue
				}
				if err := bkScore.Delete([]byte(string(scoreBuf) + p.Key)); err != nil {
					return err
				}
				updated++
			} else {
				// we are adding a new key
				if xx {
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
		return nil
	})
	return
}

func (s *Server) ZRem(name string, keys ...string) (count int, err error) {
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
		return s.deletePair(tx, name, pairs...)
	})
	return
}

func (s *Server) ZMScore(name string, keys ...string) (scores []float64, err error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing keys")
	}
	err = s.pick(name).Update(func(tx *bbolt.Tx) error {
		bkName := tx.Bucket([]byte("zset." + name))
		if bkName == nil {
			return nil
		}
		for _, key := range keys {
			scoreBuf := bkName.Get([]byte(key))
			if len(scoreBuf) != 0 {
				scores = append(scores, bytesToFloat(scoreBuf))
			} else {
				scores = append(scores, math.NaN())
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
	err = s.pick(name).Update(func(tx *bbolt.Tx) error {
		bkName := tx.Bucket([]byte("zset." + name))
		if bkName == nil {
			return nil
		}
		bkScore := tx.Bucket([]byte("zset.score." + name))
		if bkScore == nil {
			return nil
		}
		scores := make([]float64, len(keys))
		for i, key := range keys {
			scoreBuf := bkName.Get([]byte(key))
			if len(scoreBuf) != 0 {
				scores[i] = bytesToFloat(scoreBuf)
			} else {
				scores[i] = math.NaN()
			}
		}
		for i, s := range scores {
			if !math.IsNaN(s) {
				d := bkScore.Get(append(floatToBytes(s), keys[i]...))
				data = append(data, append([]byte{}, d...))
			} else {
				data = append(data, nil)
			}
		}
		return nil
	})
	return
}

func (s *Server) deletePair(tx *bbolt.Tx, name string, pairs ...Pair) error {
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
	return nil
}

func (s *Server) ZIncrBy(name string, key string, by float64) (newValue float64, err error) {
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
		return nil
	})
	return
}
