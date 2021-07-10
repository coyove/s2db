package main

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble"
)

func (z *DB) incrZSetCard(b *pebble.Batch, name string, v int64) error {
	k := []byte("zset.card." + name)
	old, c, err := z.db.Get(k)
	if err != nil {
		if err != pebble.ErrNotFound {
			return err
		}
		b.Set(k, intToBytes(v), pebble.NoSync)
	} else {
		defer c.Close()
		b.Set(k, intToBytes(bytesToInt(old)+v), pebble.NoSync)
	}
	return nil
}

func (z *DB) ZCard(name string) (int64, error) {
	k := []byte("zset.card." + name)
	old, c, err := z.db.Get(k)
	if err != nil {
		if err != pebble.ErrNotFound {
			return 0, err
		}
		return 0, nil
	}
	defer c.Close()
	return bytesToInt(old), nil
}

func (z *DB) ZAdd(name string, key string, score float64) error {
	z.Lock(name)
	defer z.Unlock(name)

	nameKey := makeZSetNameKey(name, key)
	scoreKey := makeZSetScoreKey(name, key, score)

	var b *pebble.Batch
	oldScoreKey, c, err := z.db.Get(nameKey)
	if err != nil {
		if err != pebble.ErrNotFound {
			return fmt.Errorf("get old score key: %v", err)
		}
		b = z.db.NewBatch()
		if err := z.incrZSetCard(b, name, 1); err != nil {
			return err
		}
	} else {
		defer c.Close()
		if bytes.Equal(oldScoreKey, scoreKey) {
			return nil
		}
		b = z.db.NewBatch()
		b.Delete(oldScoreKey, pebble.NoSync)
	}
	b.Set(nameKey, floatToBytes(score), pebble.NoSync)
	b.Set(scoreKey, nameKey, pebble.NoSync)
	return b.Commit(pebble.NoSync)
}

func (z *DB) ZRem(name string, key string) error {
	z.Lock(name)
	defer z.Unlock(name)

	nameKey := makeZSetNameKey(name, key)
	oldScore, c, err := z.db.Get(nameKey)
	if err != nil {
		if err != pebble.ErrNotFound {
			return err
		}
		return nil
	}
	defer c.Close()
	scoreKey := makeZSetScoreKey(name, key, bytesToFloat(oldScore))

	b := z.db.NewBatch()
	b.Delete(nameKey, pebble.NoSync)
	b.Delete(scoreKey, pebble.NoSync)
	z.incrZSetCard(b, name, -1)
	return b.Commit(pebble.NoSync)
}

func (z *DB) ZIncrBy(name string, key string, by float64) error {
	z.Lock(name)
	defer z.Unlock(name)

	nameKey := makeZSetNameKey(name, key)

	oldScoreBuf, c, err := z.db.Get(nameKey)
	oldScore := bytesToFloat(oldScoreBuf)
	if err != nil {
		if err != pebble.ErrNotFound {
			return fmt.Errorf("get old score key: %v", err)
		}
		oldScore = 0
	}
	defer c.Close()

	b := z.db.NewBatch()
	b.Delete(makeZSetScoreKey(name, key, oldScore), pebble.NoSync)
	b.Set(nameKey, floatToBytes(oldScore+by), pebble.NoSync)
	b.Set(makeZSetScoreKey(name, key, oldScore+by), nameKey, pebble.NoSync)
	return b.Commit(pebble.NoSync)
}
