package main

import (
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

func (z *DB) ZAdd(name string, key string, score float64) error {
	if err := checkScore(score); err != nil {
		return err
	}

	return z.pick(name).Update(func(tx *bbolt.Tx) error {
		bkName, err := tx.CreateBucketIfNotExists([]byte("zset." + name))
		if err != nil {
			return err
		}
		bkScore, err := tx.CreateBucketIfNotExists([]byte("zset.score." + name))
		if err != nil {
			return err
		}
		scoreBuf := bkName.Get([]byte(key))
		if len(scoreBuf) != 0 {
			if err := bkScore.Delete([]byte(string(scoreBuf) + key)); err != nil {
				return err
			}
		}
		scoreBuf = floatToBytes(score)
		if err := bkName.Put([]byte(key), scoreBuf); err != nil {
			return err
		}
		if err := bkScore.Put([]byte(string(scoreBuf)+key), []byte(key)); err != nil {
			return err
		}
		return nil
	})
}

func (z *DB) ZRem(name string, key string) error {
	return z.pick(name).Update(func(tx *bbolt.Tx) error {
		bkName := tx.Bucket([]byte("zset." + name))
		if bkName == nil {
			return nil
		}
		scoreBuf := bkName.Get([]byte(key))
		if len(scoreBuf) == 0 {
			return nil
		}
		return z.deletePair(tx, name, Pair{key, bytesToFloat(scoreBuf)})
	})
}

func (z *DB) ZMScore(name string, keys ...string) (scores []float64, err error) {
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

func (z *DB) ZIncrBy(name string, key string, by float64) error {
	if by == 0 {
		return nil
	}
	return z.pick(name).Update(func(tx *bbolt.Tx) error {
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
		return nil
	})
}
