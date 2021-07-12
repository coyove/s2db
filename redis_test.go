package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func assert(err error) {
	if err != nil {
		panic(err)
	}
}

func assertEqual(a, b interface{}) {
	buf1, _ := json.Marshal(a)
	buf2, _ := json.Marshal(b)
	if !bytes.Equal(buf1, buf2) {
		panic(fmt.Errorf("not equal: %q and %q", buf1, buf2))
	}
}

func z(s float64, m string) *redis.Z {
	return &redis.Z{Score: s, Member: m}
}

func TestZSet(t *testing.T) {
	db, _ := Open("test")
	s := Server{db}
	go s.Serve(":6666")

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6666"})

	assert(rdb.Ping(ctx).Err())

	rdb.Del(ctx, "ztmp")
	rdb.ZAdd(ctx, "ztmp", z(10, "x"))
	rdb.ZAdd(ctx, "ztmp", z(20, "y"))
	rdb.ZAdd(ctx, "ztmp", z(30, "z"))
	assertEqual(rdb.ZRange(ctx, "ztmp", 0, -1).Val(), []string{"x", "y", "z"})

	rdb.ZAdd(ctx, "ztmp", z(1, "y"))
	assertEqual(rdb.ZRange(ctx, "ztmp", 0, -1).Val(), []string{"y", "x", "z"})

	rdb.Del(ctx, "ztmp")
	assertEqual(rdb.ZAddXX(ctx, "ztmp", z(10, "x")).Val(), 0)

	rdb.Del(ctx, "ztmp")
	rdb.ZAdd(ctx, "ztmp", z(10, "x"))
	assertEqual(rdb.ZAddXX(ctx, "ztmp", z(20, "y")).Val(), 0)
	assertEqual(rdb.ZCard(ctx, "ztmp").Val(), 1)

	rdb.Del(ctx, "ztmp")
	rdb.ZAdd(ctx, "ztmp", z(10, "x"))
	assertEqual(rdb.ZAdd(ctx, "ztmp", z(10, "x"), z(20, "y"), z(30, "z")).Val(), 2)

	rdb.Del(ctx, "ztmp")
	rdb.ZAdd(ctx, "ztmp", z(10, "x"), z(20, "y"), z(30, "z"))
	rdb.ZAddXX(ctx, "ztmp", z(5, "doo"), z(11, "x"), z(21, "y"), z(40, "zap"))
	assertEqual(rdb.ZCard(ctx, "ztmp").Val(), 3)
	assertEqual(rdb.ZScore(ctx, "ztmp", "x").Val(), 11)
	assertEqual(rdb.ZScore(ctx, "ztmp", "y").Val(), 21)

	rdb.Del(ctx, "ztmp")
	rdb.ZAddNX(ctx, "ztmp", z(10, "x"), z(20, "y"), z(30, "z"))
	assertEqual(rdb.ZCard(ctx, "ztmp").Val(), 3)

	rdb.Del(ctx, "ztmp")
	rdb.ZAddNX(ctx, "ztmp", z(10, "x"), z(20, "y"), z(30, "z"))
	assertEqual(rdb.ZAddNX(ctx, "ztmp", z(11, "x"), z(21, "y"), z(100, "a"), z(200, "b")).Val(), 2)
	assertEqual(rdb.ZScore(ctx, "ztmp", "x").Val(), 10)
	assertEqual(rdb.ZScore(ctx, "ztmp", "y").Val(), 20)
	assertEqual(rdb.ZScore(ctx, "ztmp", "a").Val(), 100)
	assertEqual(rdb.ZScore(ctx, "ztmp", "b").Val(), 200)

	rdb.Del(ctx, "ztmp")
	rdb.ZAddNX(ctx, "ztmp", z(10, "x"), z(20, "y"), z(30, "z"))
	assertEqual(rdb.ZAdd(ctx, "ztmp", z(11, "x"), z(21, "y"), z(30, "z")).Val(), 0)
	assertEqual(rdb.ZAddCh(ctx, "ztmp", z(12, "x"), z(22, "y"), z(30, "z")).Val(), 2)

	rdb.Del(ctx, "ztmp")
	rdb.ZAddNX(ctx, "ztmp", z(10, "x"), z(20, "y"), z(30, "z"))
	assertEqual(3, rdb.ZCard(ctx, "ztmp").Val())
	assertEqual(0, rdb.ZCard(ctx, "zdoesntexist").Val())

	rdb.Del(ctx, "ztmp")
	rdb.ZAddNX(ctx, "ztmp", z(10, "a"), z(20, "b"), z(30, "c"))
	assertEqual(2, rdb.ZRem(ctx, "ztmp", "x", "y", "a", "b", "k").Val())
	assertEqual(0, rdb.ZRem(ctx, "ztmp", "foo", "bar").Val())
	assertEqual(1, rdb.ZRem(ctx, "ztmp", "c").Val())

	rdb.Del(ctx, "ztmp")
	rdb.ZAdd(ctx, "ztmp", z(1, "a"))
	rdb.ZAdd(ctx, "ztmp", z(2, "b"))
	rdb.ZAdd(ctx, "ztmp", z(3, "c"))
	rdb.ZAdd(ctx, "ztmp", z(4, "d"))

	assertEqual([]string{"a", "b", "c", "d"}, rdb.ZRange(ctx, "ztmp", 0, -1).Val())
	assertEqual([]string{"a", "b", "c"}, rdb.ZRange(ctx, "ztmp", 0, -2).Val())
	assertEqual([]string{"b", "c", "d"}, rdb.ZRange(ctx, "ztmp", 1, -1).Val())
	assertEqual([]string{"b", "c"}, rdb.ZRange(ctx, "ztmp", 1, -2).Val())
	assertEqual([]string{"c", "d"}, rdb.ZRange(ctx, "ztmp", -2, -1).Val())
	assertEqual([]string{"c"}, rdb.ZRange(ctx, "ztmp", -2, -2).Val())

	// # out of range start index
	assertEqual([]string{"a", "b", "c"}, rdb.ZRange(ctx, "ztmp", -5, 2).Val())
	assertEqual([]string{"a", "b"}, rdb.ZRange(ctx, "ztmp", -5, 1).Val())
	assertEqual([]string{}, rdb.ZRange(ctx, "ztmp", 5, -1).Val())
	assertEqual([]string{}, rdb.ZRange(ctx, "ztmp", 5, -2).Val())

	// # out of range end index
	assertEqual([]string{"a", "b", "c", "d"}, rdb.ZRange(ctx, "ztmp", 0, 5).Val())
	assertEqual([]string{"b", "c", "d"}, rdb.ZRange(ctx, "ztmp", 1, 5).Val())
	assertEqual([]string{}, rdb.ZRange(ctx, "ztmp", 0, -5).Val())
	assertEqual([]string{}, rdb.ZRange(ctx, "ztmp", 1, -5).Val())

	// # withscores
	assertEqual([]redis.Z{*z(1, "a"), *z(2, "b"), *z(3, "c"), *z(4, "d")}, rdb.ZRangeWithScores(ctx, "ztmp", 0, -1).Val())

	assertEqual([]string{"d", "c", "b", "a"}, rdb.ZRevRange(ctx, "ztmp", 0, -1).Val())
	assertEqual([]string{"d", "c", "b"}, rdb.ZRevRange(ctx, "ztmp", 0, -2).Val())
	assertEqual([]string{"c", "b", "a"}, rdb.ZRevRange(ctx, "ztmp", 1, -1).Val())
	assertEqual([]string{"c", "b"}, rdb.ZRevRange(ctx, "ztmp", 1, -2).Val())
	assertEqual([]string{"b", "a"}, rdb.ZRevRange(ctx, "ztmp", -2, -1).Val())
	assertEqual([]string{"b"}, rdb.ZRevRange(ctx, "ztmp", -2, -2).Val())

	// # out of range start index
	assertEqual([]string{"d", "c", "b"}, rdb.ZRevRange(ctx, "ztmp", -5, 2).Val())
	assertEqual([]string{"d", "c"}, rdb.ZRevRange(ctx, "ztmp", -5, 1).Val())
	assertEqual([]string{}, rdb.ZRevRange(ctx, "ztmp", 5, -1).Val())
	assertEqual([]string{}, rdb.ZRevRange(ctx, "ztmp", 5, -2).Val())

	// # out of range end index
	assertEqual([]string{"d", "c", "b", "a"}, rdb.ZRevRange(ctx, "ztmp", 0, 5).Val())
	assertEqual([]string{"c", "b", "a"}, rdb.ZRevRange(ctx, "ztmp", 1, 5).Val())
	assertEqual([]string{}, rdb.ZRevRange(ctx, "ztmp", 0, -5).Val())
	assertEqual([]string{}, rdb.ZRevRange(ctx, "ztmp", 1, -5).Val())

	// # withscores
	assertEqual([]redis.Z{*z(4, "d"), *z(3, "c"), *z(2, "b"), *z(1, "a")}, rdb.ZRevRangeWithScores(ctx, "ztmp", 0, -1).Val())

	time.Sleep(time.Second)
}
