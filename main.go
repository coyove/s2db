package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

type DB struct {
	wlocks [256]sync.Mutex
	db     *pebble.DB
}

type Pair struct {
	Key   string
	Score float64
}

func Open(path string) (*DB, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	return &DB{
		db: db,
	}, nil
}

func (z *DB) Close() error { return z.db.Close() }

func (z *DB) Lock(key string) { z.wlocks[byte(hashStr(key))].Lock() }

func (z *DB) Unlock(key string) { z.wlocks[byte(hashStr(key))].Unlock() }

func (z *DB) makeZSetNameKey(name, key string) []byte {
	return []byte("zset.ns." + name + "." + key)
}

func (z *DB) makeZSetScoreKey(name, key string, score float64) []byte {
	return []byte("zset.rev." + name + "." + floatToBytesString(score) + "." + key)
}

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

func (z *DB) ZAdd(name string, key string, score float64) error {
	z.Lock(name)
	defer z.Unlock(name)

	nameKey := z.makeZSetNameKey(name, key)
	scoreKey := z.makeZSetScoreKey(name, key, score)

	var b *pebble.Batch
	oldScoreKey, c, err := z.db.Get(nameKey)
	if err != nil {
		if err != pebble.ErrNotFound {
			return fmt.Errorf("get old score key: %v", err)
		}
		b = z.db.NewBatch()
	} else {
		defer c.Close()
		if bytes.Equal(oldScoreKey, scoreKey) {
			return nil
		}
		b = z.db.NewBatch()
		b.Delete(oldScoreKey, pebble.NoSync)
	}
	if err := z.incrZSetCard(b, name, 1); err != nil {
		return err
	}
	b.Set(nameKey, floatToBytes(score), pebble.NoSync)
	b.Set(scoreKey, nameKey, pebble.NoSync)
	return b.Commit(pebble.NoSync)
}

func (z *DB) ZIncrBy(name string, key string, by float64) error {
	z.Lock(name)
	defer z.Unlock(name)

	nameKey := z.makeZSetNameKey(name, key)

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
	b.Delete(z.makeZSetScoreKey(name, key, oldScore), pebble.NoSync)
	b.Set(nameKey, floatToBytes(oldScore+by), pebble.NoSync)
	b.Set(z.makeZSetScoreKey(name, key, oldScore+by), nameKey, pebble.NoSync)
	return b.Commit(pebble.NoSync)
}

func main() {
	rand.Seed(time.Now().Unix())
	start := time.Now()
	db, _ := Open("test")
	for i := 0; i < 1000; i++ {
		fmt.Println(i)
		db.ZAdd("test", strconv.Itoa(i), rand.Float64())
	}
	db.db.Flush()
	fmt.Println(time.Since(start).Seconds())
}
