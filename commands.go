package main

import (
	"fmt"
	"math"

	"go.etcd.io/bbolt"
)

func (z *DB) pick(name string) *bbolt.DB {
	return z.db[hashStr(name)%uint64(len(z.db))]
}

func (z *DB) ZCard(name string) (int64, error) {
	count := 0
	err := z.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset." + name))
		if bk == nil {
			return nil
		}
		count = bk.Stats().KeyN
		return nil
	})
	return int64(count), err
}

func (z *DB) Del(names ...string) (count int, err error) {
	nameShards := make([][]string, len(z.db))
	for _, name := range names {
		x := &nameShards[hashStr(name)%uint64(len(nameShards))]
		*x = append(*x, name)
	}
	for i := range nameShards {
		err = z.db[i].Update(func(tx *bbolt.Tx) error {
			for _, name := range nameShards[i] {
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
	}
	return
}

func (z *DB) ZAdd(name string, pairs []Pair, nx, xx bool) (added, updated int, err error) {
	err = z.pick(name).Update(func(tx *bbolt.Tx) error {
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
			if err := bkScore.Put([]byte(string(scoreBuf)+p.Key), []byte(p.Key)); err != nil {
				return err
			}
		}
		return nil
	})
	return
}

func (z *DB) ZRem(name string, keys ...string) (count int, err error) {
	err = z.pick(name).Update(func(tx *bbolt.Tx) error {
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
			pairs = append(pairs, Pair{key, bytesToFloat(scoreBuf)})
			count++
		}
		if len(pairs) == 0 {
			return nil
		}
		return z.deletePair(tx, name, pairs...)
	})
	return
}

func (z *DB) ZMScore(name string, keys ...string) (scores []float64, err error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing keys")
	}
	err = z.pick(name).Update(func(tx *bbolt.Tx) error {
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

func (z *DB) deletePair(tx *bbolt.Tx, name string, pairs ...Pair) error {
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

func (z *DB) ZIncrBy(name string, key string, by float64) (newValue float64, err error) {
	err = z.pick(name).Update(func(tx *bbolt.Tx) error {
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
		if len(scoreBuf) != 0 {
			if err := bkScore.Delete([]byte(string(scoreBuf) + key)); err != nil {
				return err
			}
			score = bytesToFloat(scoreBuf)
		}
		if by == 0 {
			newValue = score
			return nil
		}
		if err := checkScore(score + by); err != nil {
			return err
		}
		scoreBuf = floatToBytes(score + by)
		if err := bkName.Put([]byte(key), scoreBuf); err != nil {
			return err
		}
		if err := bkScore.Put([]byte(string(scoreBuf)+key), []byte(key)); err != nil {
			return err
		}
		newValue = score + by
		return nil
	})
	return
}
