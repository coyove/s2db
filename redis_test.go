package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
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
	s, _ := Open("test")
	go s.Serve(":6666")

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6666"})

	assert(rdb.Ping(ctx).Err())

	rdb.Del(ctx, "ztmp")
	rdb.ZAdd(ctx, "ztmp", z(10, "x"))
	rdb.ZAdd(ctx, "ztmp", z(20, "y"))
	// rdb.ZAdd(ctx, "ztmp", z(30, "z"))
	{
		rdb.Process(ctx, redis.NewIntCmd(ctx, "ZADD", "ztmp", "DATA", 30, "z", "zdata"))
		assertEqual(rdb.ZRange(ctx, "ztmp", 0, -1).Val(), []string{"x", "y", "z"})

		rdb.Process(ctx, redis.NewIntCmd(ctx, "ZADD", "ztmp", "DATA", 20, "x", "xxx"))

		cmd := redis.NewStringSliceCmd(ctx, "ZMDATA", "ztmp", "z", "z", "x", "y")
		rdb.Process(ctx, cmd)
		assertEqual(cmd.Val(), []string{"zdata", "zdata", "xxx", ""})
	}

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

	rdb.Del(ctx, "zranktmp")
	rdb.ZAddNX(ctx, "zranktmp", z(10, "x"), z(20, "y"), z(30, "z"))
	assertEqual(0, rdb.ZRank(ctx, "zranktmp", "x").Val())
	assertEqual(1, rdb.ZRank(ctx, "zranktmp", "y").Val())
	assertEqual(2, rdb.ZRank(ctx, "zranktmp", "z").Val())
	assertEqual(redis.Nil, rdb.ZRank(ctx, "zranktmp", "foo").Err())
	assertEqual(2, rdb.ZRevRank(ctx, "zranktmp", "x").Val())
	assertEqual(1, rdb.ZRevRank(ctx, "zranktmp", "y").Val())
	assertEqual(0, rdb.ZRevRank(ctx, "zranktmp", "z").Val())
	assertEqual(redis.Nil, rdb.ZRevRank(ctx, "zranktmp", "foo").Err())
	rdb.ZRem(ctx, "zranktmp", "y")
	assertEqual(0, rdb.ZRank(ctx, "zranktmp", "x").Val())
	assertEqual(1, rdb.ZRank(ctx, "zranktmp", "z").Val())

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

	rdb.Del(ctx, "zset")
	rdb.ZIncrBy(ctx, "zset", 1, "foo")
	assertEqual([]string{"foo"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
	assertEqual(1, rdb.ZScore(ctx, "zset", "foo").Val())

	rdb.ZIncrBy(ctx, "zset", 2, "foo")
	rdb.ZIncrBy(ctx, "zset", 1, "bar")
	assertEqual([]string{"bar", "foo"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

	rdb.ZIncrBy(ctx, "zset", 10, "bar")
	rdb.ZIncrBy(ctx, "zset", -5, "foo")
	rdb.ZIncrBy(ctx, "zset", -5, "bar")
	assertEqual([]string{"foo", "bar"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

	assertEqual(-2, rdb.ZScore(ctx, "zset", "foo").Val())
	assertEqual(6, rdb.ZScore(ctx, "zset", "bar").Val())

	rdb.Del(ctx, "zset")
	fmt.Println(rdb.ZAdd(ctx, "zset", z(math.Inf(-1), "a"), z(1, "b"), z(2, "c"), z(3, "d"), z(4, "e"), z(5, "f"), z(math.Inf(1), "g")).Err())

	// # inclusive range
	assertEqual([]string{"a", "b", "c"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "-inf", Max: "2"}).Val())
	assertEqual([]string{"b", "c", "d"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "3"}).Val())
	assertEqual([]string{"d", "e", "f"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "3", Max: "6"}).Val())
	assertEqual([]string{"e", "f", "g"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "4", Max: "+inf"}).Val())
	assertEqual([]string{"c", "b", "a"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "2", Min: "-inf"}).Val())
	assertEqual([]string{"d", "c", "b"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "3", Min: "0"}).Val())
	assertEqual([]string{"f", "e", "d"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "6", Min: "3"}).Val())
	assertEqual([]string{"g", "f", "e"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "+inf", Min: "4"}).Val())
	assertEqual(3, rdb.ZCount(ctx, "zset", "0", "3").Val())

	// # exclusive range
	assertEqual([]string{"b"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(-inf", Max: "(2"}).Val())
	assertEqual([]string{"b", "c"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(0", Max: "(3"}).Val())
	assertEqual([]string{"e", "f"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(3", Max: "(6"}).Val())
	assertEqual([]string{"f"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(4", Max: "(+inf"}).Val())
	assertEqual([]string{"b"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "(2", Min: "(-inf"}).Val())
	assertEqual([]string{"c", "b"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "(3", Min: "(0"}).Val())
	assertEqual([]string{"f", "e"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "(6", Min: "(3"}).Val())
	assertEqual([]string{"f"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "(+inf", Min: "(4"}).Val())
	assertEqual(2, rdb.ZCount(ctx, "zset", "(0", "(3").Val())

	// # test empty ranges
	rdb.ZRem(ctx, "zset", "a", "g")

	// # inclusive
	assertEqual([]string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "4", Max: "2"}).Val())
	assertEqual([]string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "6", Max: "+inf"}).Val())
	assertEqual([]string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "-inf", Max: "-6"}).Val())
	assertEqual([]string{}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "+inf", Min: "6"}).Val())
	assertEqual([]string{}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "-6", Min: "-inf"}).Val())

	// # exclusive([]string
	assertEqual([]string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(4", Max: "(2"}).Val())
	assertEqual([]string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "2", Max: "(2"}).Val())
	assertEqual([]string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(2", Max: "2"}).Val())
	assertEqual([]string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(6", Max: "(+inf"}).Val())
	assertEqual([]string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(-inf", Max: "(-6"}).Val())
	assertEqual([]string{}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "(+inf", Min: "(6"}).Val())
	assertEqual([]string{}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "(-6", Min: "(-inf"}).Val())

	// # empty inn([]stringr , nge
	assertEqual([]string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "2.4", Max: "2.6"}).Val())
	assertEqual([]string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(2.4", Max: "2.6"}).Val())
	assertEqual([]string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "2.4", Max: "(2.6"}).Val())
	assertEqual([]string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(2.4", Max: "(2.6"}).Val())

	assertEqual([]redis.Z{*z(1, "b"), *z(2, "c"), *z(3, "d")}, rdb.ZRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "3"}).Val())
	assertEqual([]redis.Z{*z(3, "d"), *z(2, "c"), *z(1, "b")}, rdb.ZRevRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{Max: "3", Min: "0"}).Val())

	remrangebyscore := func(min, max string) int64 {
		rdb.Del(ctx, "zset")
		rdb.ZAdd(ctx, "zset", z(1, "a"), z(2, "b"), z(3, "c"), z(4, "d"), z(5, "e"))
		return rdb.ZRemRangeByScore(ctx, "zset", min, max).Val()
	}

	//             # inner range
	assertEqual(3, remrangebyscore("2", "4"))
	assertEqual([]string{"a", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
	//
	//             # start underflow
	assertEqual(1, remrangebyscore("-10", "1"))
	assertEqual([]string{"b", "c", "d", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
	//
	//             # end overflow
	assertEqual(1, remrangebyscore("5", "10"))
	assertEqual([]string{"a", "b", "c", "d"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
	//
	//             # switch min and max
	assertEqual(0, remrangebyscore("4", "2"))
	assertEqual([]string{"a", "b", "c", "d", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
	//
	//             # -inf to mid
	assertEqual(3, remrangebyscore("-inf", "3"))
	assertEqual([]string{"d", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
	//
	//             # mid to +inf
	assertEqual(3, remrangebyscore("3", "+inf"))
	assertEqual([]string{"a", "b"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
	//
	//             # -inf to +inf
	assertEqual(5, remrangebyscore("-inf", "+inf"))
	assertEqual([]string{}, rdb.ZRange(ctx, "zset", 0, -1).Val())
	//
	//             # exclusive min
	assertEqual(4, remrangebyscore("(1", "5"))
	assertEqual([]string{"a"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
	assertEqual(3, remrangebyscore("(2", "5"))
	assertEqual([]string{"a", "b"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
	//
	//             # exclusive max
	assertEqual(4, remrangebyscore("1", "(5"))
	assertEqual([]string{"e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
	assertEqual(3, remrangebyscore("1", "(4"))
	assertEqual([]string{"d", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
	//
	//             # exclusive min and max
	assertEqual(3, remrangebyscore("(1", "(5"))
	assertEqual([]string{"a", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

	time.Sleep(time.Second)
	s.Close()
}

func TestBatch(t *testing.T) {
	// s, _ := Open("test")
	// go s.Serve(":6666")

	// ctx := context.TODO()
	// rdb := redis.NewClient(&redis.Options{Addr: "localhost:6666"})
	// rdb.Del(ctx, "bulk")
	// time.Sleep(time.Second)
	// shard := hashStr("bulk") % uint64(len(s.db))
	// index, _ := s.walProgress(int(shard))
	// cmd := redis.NewIntCmd(ctx, "BULK",
	// 	shard,
	// 	index+1,
	// 	joinCommandString("zadd", "bulk", "1", "a", "2", "b"),
	// 	joinCommandString("zadd", "bulk", "3", "c", "4", "d"),
	// 	joinCommandString("zremrangebyrank", "bulk", "1", "1"),
	// 	joinCommandString("bad command"),
	// 	joinCommandString("zadd", "bulk", "999", "bad"),
	// )
	// rdb.Process(ctx, cmd)
	// fmt.Println(cmd.Result())
	// assertEqual([]string{"a", "c", "d"}, rdb.ZRange(ctx, "bulk", 0, -1).Val())
	// s.Close()
}

func TestZSetCache(t *testing.T) {
	rand.Seed(time.Now().Unix())
	ctx := context.TODO()

	s, _ := Open("test")
	go s.Serve(":6666")

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6666"})

	assert(rdb.Ping(ctx).Err())

	data := []*redis.Z{}
	for i := 0; i < 50; i++ {
		data = append(data, &redis.Z{Score: rand.Float64(), Member: strconv.Itoa(i)})
	}

	const NAME = "test2"

	rdb.ZAdd(ctx, NAME, data...)
	fmt.Println("finish adding")

	start := time.Now()
	wg := sync.WaitGroup{}
	for c := 0; c < 10; c++ {
		wg.Add(1)
		go func(c int) {
			fmt.Println(c)
			for i := 0; i < 1e4; i++ {
				if rand.Intn(1000) == 0 {
					rdb.ZAdd(ctx, NAME, &redis.Z{Score: 1, Member: 0})
					fmt.Println("clear cache")
				} else {
					rdb.ZRangeByScore(ctx, NAME, &redis.ZRangeBy{Min: "(0.2", Max: "(0.4"})
				}
			}
			wg.Done()
		}(c)
	}
	wg.Wait()
	fmt.Println(time.Since(start).Seconds() / 1e6)

	s.Close()
	time.Sleep(time.Second)
}
