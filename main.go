package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"go.etcd.io/bbolt"
)

type DB struct {
	db [32]*bbolt.DB
}

type Pair struct {
	Key   string
	Score float64
}

func Open(path string) (*DB, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}
	x := &DB{}
	for i := range x.db {
		db, err := bbolt.Open(filepath.Join(path, "shard"+strconv.Itoa(i)), 0666, &bbolt.Options{
			FreelistType: bbolt.FreelistMapType,
		})
		if err != nil {
			return nil, err
		}
		x.db[i] = db
	}
	return x, nil
}

func (z *DB) Close() error {
	var errs []error
	for _, db := range z.db {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close: %v", errs)
	}
	return nil
}

func main() {
	rand.Seed(time.Now().Unix())
	start := time.Now()
	db, _ := Open("test")
	for i := 0; i < 1000; i += 1 {
		// fmt.Println(i)
		db.ZAdd("test", strconv.Itoa(i), rand.Float64()*2)
	}
	fmt.Println(db.ZCard("test"))
	fmt.Println(db.rangeLex("test", RangeLimit{Value: "11", Inclusive: false}, RangeLimit{Value: "20"}, RangeOptions{OffsetStart: 0, OffsetEnd: 10}))
	fmt.Println(db.rangeScore("test", RangeLimit{Value: MinScoreStr}, RangeLimit{Value: MaxScoreStr}, RangeOptions{OffsetEnd: -1, CountOnly: true}))
	// fmt.Println(db.rangeScore("test", RangeLimit{Value: "0.1"}, RangeLimit{Value: "0.3"}, 0, 9, true))
	// fmt.Println(db.rangeScoreIndex("test", 0, 20))
	fmt.Println(time.Since(start).Seconds())
}
