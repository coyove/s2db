package main

import (
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

func (z *DB) Flush() error { return z.db.Flush() }

func (z *DB) Close() error { return z.db.Close() }

func (z *DB) Lock(key string) { z.wlocks[byte(hashStr(key))].Lock() }

func (z *DB) Unlock(key string) { z.wlocks[byte(hashStr(key))].Unlock() }

func main() {
	rand.Seed(time.Now().Unix())
	start := time.Now()
	db, _ := Open("test")
	for i := 0; i < 1000; i++ {
		db.ZAdd("test", strconv.Itoa(i), rand.Float64())
	}
	fmt.Println(db.ZCard("test"))
	db.db.Flush()
	// fmt.Println(db.rangeLex("test", RangeLimit{Value: "10"}, RangeLimit{Value: "20"}))
	// fmt.Println(db.rangeScore("test", RangeLimit{Value: "0.1"}, RangeLimit{Value: "0.3"}))
	fmt.Println(db.rangeScoreIndex("test", 0, 20))
	fmt.Println(time.Since(start).Seconds())
}
