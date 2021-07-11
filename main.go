package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

type DB struct {
	wlocks [256]sync.Mutex
	db     *bbolt.DB
}

type Pair struct {
	Key   string
	Score float64
}

func Open(path string) (*DB, error) {
	db, err := bbolt.Open(path, 0666, &bbolt.Options{
		FreelistType: bbolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}

	return &DB{
		db: db,
	}, nil
}

func (z *DB) Close() error { return z.db.Close() }

func main() {
	rand.Seed(time.Now().Unix())
	start := time.Now()
	db, _ := Open("test")
	for i := 0; i < 1000; i += 1 {
		fmt.Println(i)
		db.ZAdd("test", strconv.Itoa(i), rand.Float64())
	}
	fmt.Println(db.ZCard("test"))
	// fmt.Println(db.rangeLex("test", RangeLimit{Value: "11", Inclusive: false}, RangeLimit{Value: "20"}, false))
	fmt.Println(db.rangeScore("test", RangeLimit{Value: "0.1"}, RangeLimit{Value: "0.3"}, true))
	// fmt.Println(db.rangeScoreIndex("test", 0, 20))
	fmt.Println(time.Since(start).Seconds())
}
