package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/go-redis/redis/v8"
	"github.com/mmcloughlin/geohash"
	log "github.com/sirupsen/logrus"
)

func init() {
	slowLogger = log.New()
	testFlag = true
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
	time.Sleep(time.Second)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6666"})

	s2pkg.PanicErr(rdb.Ping(ctx).Err())

	fmt.Println(rdb.ConfigSet(ctx, "SERVERNAME", "TEST").Err())

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

	pipe := rdb.Pipeline()
	pipe.Del(ctx, "ztmp")
	pipe.ZAdd(ctx, "ztmp", z(1, "a"), z(2, "b"), z(4, "d"))
	pipe.ZAdd(ctx, "ztmp", z(2, "b"), z(3, "c"))
	pipe.ZRemRangeByScore(ctx, "ztmp", "[4", "[4")
	v := pipe.ZRange(ctx, "ztmp", 0, -1)
	fmt.Println(pipe.Exec(ctx))
	assertEqual([]string{"a", "b", "c"}, v.Val())

	rdb.Del(ctx, "zset")
	rdb.ZAdd(ctx, "zset", z(0, "alpha"), z(0, "bar"), z(0, "cool"), z(0, "down"), z(0, "elephant"), z(0, "foo"), z(0, "great"), z(0, "hill"), z(0, "omega"))

	// inclusive range
	assertEqual([]string{"alpha", "bar", "cool"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "-", Max: "[cool"}).Val())
	assertEqual([]string{"bar", "cool", "down"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[bar", Max: "[down"}).Val())
	assertEqual([]string{"great", "hill", "omega"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[g", Max: "+"}).Val())
	assertEqual([]string{"cool", "bar", "alpha"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Max: "[cool", Min: "-"}).Val())
	assertEqual([]string{"down", "cool", "bar"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Max: "[down", Min: "[bar"}).Val())
	assertEqual([]string{"omega", "hill", "great", "foo", "elephant", "down"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Max: "+", Min: "[d"}).Val())

	// exclusive range
	assertEqual([]string{"alpha", "bar"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "-", Max: "(cool"}).Val())      // - (cool]
	assertEqual([]string{"cool"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "(bar", Max: "(down"}).Val())           // (bar (down]
	assertEqual([]string{"hill", "omega"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "(great", Max: "+"}).Val())    // (great +]
	assertEqual([]string{"bar", "alpha"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Max: "(cool", Min: "-"}).Val())   // (cool -]
	assertEqual([]string{"bar", "alpha"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Max: "(bas", Min: "-"}).Val())    // (cool -]
	assertEqual([]string{"cool"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Max: "(down", Min: "(bar"}).Val())        // (down (bar]
	assertEqual([]string{"omega", "hill"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Max: "+", Min: "(great"}).Val()) // + (great]

	// inclusive and exclusive
	assertEqual([]string{}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "(az", Max: "(b"}).Val())          // (az (b]
	assertEqual([]string{}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "(z", Max: "+"}).Val())            // (z +]
	assertEqual([]string{}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "-", Max: "[aaaa"}).Val())         // - \[aaaa]
	assertEqual([]string{}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Max: "[elez", Min: "[elex"}).Val())  // \[elez \[elex]
	assertEqual([]string{}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Max: "(hill", Min: "(omega"}).Val()) //(hill (omega]

	rdb.Del(ctx, "zset")
	fmt.Println(rdb.ZAdd(ctx, "zset", z(math.Inf(-1), "a"), z(1, "b"), z(2, "c"), z(3, "d"), z(4, "e"), z(5, "f"), z(math.Inf(1), "g")).Err())
	assertEqual([]string{"b", "c"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "10", Count: 2}).Val())
	assertEqual([]string{"f", "e"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "10", Min: "0", Count: 2}).Val())

	rdb.Del(ctx, "zset")
	rdb.ZAdd(ctx, "zset", z(0, "alpha"), z(0, "bar"), z(0, "cool"), z(0, "down"), z(0, "elephant"), z(0, "foo"), z(0, "great"), z(0, "hill"), z(0, "omega"))
	assertEqual([]string{"alpha", "bar"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "-", Max: "[cool", Count: 2}).Val())
	assertEqual([]string{"bar"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[bar", Max: "[down", Count: 1}).Val())
	assertEqual([]string{"bar", "cool", "down"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[bar", Max: "[down", Count: 100}).Val())
	assertEqual([]string{"omega", "hill", "great", "foo", "elephant"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Max: "+", Min: "[d", Count: 5}).Val())
	assertEqual([]string{"omega", "hill", "great", "foo"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Max: "+", Min: "[d", Count: 4}).Val())

	s.Close()
	time.Sleep(time.Second)
}

func TestIntersect(t *testing.T) {
	s, _ := Open("test")
	go s.Serve(":6666")

	ctx := context.TODO()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6666"})
	rdb.Del(ctx, "iz")
	rdb.Del(ctx, "iz2")
	rdb.Del(ctx, "iz3")

	for i := 1; i <= 40; i++ {
		rdb.Do(ctx, "ZADD", "iz", i, "m"+strconv.Itoa(i))
		if i%2 == 0 {
			rdb.Do(ctx, "ZADD", "iz2", i, "m"+strconv.Itoa(i))
		}
		if i%3 == 0 {
			rdb.Do(ctx, "ZADD", "iz3", i, "m"+strconv.Itoa(i))
		}
	}

	v, _ := rdb.Do(ctx, "ZRANGEBYSCORE", "iz", "-inf", "+inf", "LIMIT", 0, 6, "INTERSECT", "iz2").Result()
	assertEqual([]string{"m2", "m4", "m6", "m8", "m10", "m12"}, v)
	v, _ = rdb.Do(ctx, "ZRANGEBYSCORE", "iz", "-inf", "+inf", "LIMIT", 0, 6, "INTERSECT", "iz2", "INTERSECT", "iz3").Result()
	assertEqual([]string{"m6", "m12", "m18", "m24", "m30", "m36"}, v)
	v, _ = rdb.Do(ctx, "ZRANGEBYSCORE", "iz", "-inf", "+inf", "LIMIT", 0, 6, "INTERSECT", "whatever", "INTERSECT", "iz3").Result()
	assertEqual([]string{}, v)
	v, _ = rdb.Do(ctx, "ZRANGEBYSCORE", "iz", "-inf", "+inf", "LIMIT", 0, 6, "NOTINTERSECT", "whatever").Result()
	assertEqual([]string{"m1", "m2", "m3", "m4", "m5", "m6"}, v)
	v, _ = rdb.Do(ctx, "ZRANGEBYSCORE", "iz", "-inf", "+inf", "LIMIT", 0, 6, "NOTINTERSECT", "iz2", "INTERSECT", "iz3").Result()
	assertEqual([]string{"m3", "m9", "m15", "m21", "m27", "m33"}, v)

	mf := "lambda(k,s) (s[0]+1)*(s[1]+1) end"
	v, _ = rdb.Do(ctx, "ZREVRANGEBYSCORE", "iz", "+inf", "-inf", "LIMIT", 0, 3, "MERGE", "iz2", "MERGEFUNC", mf, "WITHSCORES").Result()
	assertEqual([]string{"m40", "1681", "m39", "40", "m38", "1521"}, v)
	v, _ = rdb.Do(ctx, "ZREVRANGEBYSCORE", "iz", "+inf", "-inf", "LIMIT", 0, 100, "MERGE", "iz2", "MERGEFUNC", mf, "MERGETOP", 4, "DESC", "WITHSCORES").Result()
	assertEqual([]string{"m40", "1681", "m38", "1521", "m36", "1369", "m34", "1225"}, v)

	s.Close()
}

func TestQueue(t *testing.T) {
	s, _ := Open("test")
	go s.Serve(":6666")

	ctx := context.TODO()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6666"})
	rdb.Del(ctx, "q")

	for i := 1; i <= 100; i++ {
		rdb.Do(ctx, "QAPPEND", "q", i, "COUNT", 10)
	}

	v, _ := rdb.Do(ctx, "QSCAN", "q", 2, 4).Result()
	assertEqual(v, []string{"92", "93", "94", "95"})
	v, _ = rdb.Do(ctx, "QSCAN", "q", -2, -4).Result()
	assertEqual(v, []string{"98", "97", "96", "95"})

	rdb.Do(ctx, "QAPPEND", "q", "--TRIM--", "COUNT", 8)
	v, _ = rdb.Do(ctx, "QSCAN", "q", 2, 4).Result()
	assertEqual(v, []string{"94", "95", "96", "97"})

	s.Close()
}

func TestIndex(t *testing.T) {
	var text = `河北固安发现2名阳性人员，系共同居住的母女
    31省区市新增确诊病例27例 其中本土病例9例
    北京2月4日新增1例本土确诊病例 在丰台区
    深圳2地升级中风险 全国共有高中风险区7+56个
    杭州疫情出现拐点，已从社区清零转向隔离点清零
    路透社记者给北京防疫找“茬”，推特网友群嘲
    深圳暂停全市线下校外培训服务和托管服务
    广西德保县公布1例阳性病例活动轨迹
	

    习主席的一天（2022年2月5日）
    民生有保障 看习近平的“天下一家”情怀
    “载入史册的盛会” 恢宏画卷 为突破喝彩 冬奥年
    这行泪，为国家荣耀而流淌 6日观赛指南：苏翊鸣亮相
    “鸟巢”和世界都安静了 中国为什么能赢得金牌
    新春走基层｜三江之源，他们守望家园
    北京冬奥会开幕式 令人眼见活力心生希望
    交通部门以科技护航冬奥春运出行安全

    交通部门以科技护航冬奥春运出行安全
    “感动中国”教师刘芳：母爱，是心中的一道光
    俄奥委会主席：北京冬奥会开幕式简洁而又精彩
    31省区市新增确诊病例43例 其中本土病例13例
    就在今晚！中国女足冲击亚洲杯冠军
    1月“调控”超66次，中国各地稳楼市政策频出
    队魂就是“拼命滑”！你永远可以相信中国短道队
    美国就这么看中俄联合声明？
    人少的代表团为什么都参赛高山滑雪？
    北京累计报告118例本土病例，分布丰台朝阳等多区
    “拉面馆”开到疫情防控卡口 暖胃又暖心

    财经 | 央企董事长、全国优秀企业家向全国人民拜年
    冬奥开幕式霸屏！微火火炬等酷科技，背后是这些公司
    实探春节楼市:中介放假 贷款额度充裕 客户主动求带看
    股票| 百亿基金经理如何看这十大投资问题？ 开户福利

    起底零跑汽车：IT人造车 2021房企品牌价值50强揭晓

    科技 | 小学生“最惨假期”？游戏防沉迷落地效果如何
    张艺谋全方位揭秘北京冬奥会开幕式：呈现一种现代感
    相亲对象太奇葩，可能得怪你爸妈 GIF:姿势最奇怪的狗

    汽车 | 特斯拉拟扩建奥斯汀工厂 生产电池阴极

    本地 | 上海铁路春运进入节前最高峰 双向过节为团圆
    上海众多新地标载体迎首个春节 花式秀年味掀消费热潮
    关注 | 隈研吾设计酒店7选 新手表很快就戴腻了

    必看 | 真香开箱：牛年12生肖幸运色 2021整体运势
`

	s, _ := Open("test")
	go s.Serve(":6666")

	ctx := context.TODO()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6666"})

	for i, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		fmt.Println(rdb.Do(ctx, "idxadd", i, line, "fts").Result())
	}
	time.Sleep(time.Second * 10)
	s.Close()
}

func TestGeo(t *testing.T) {
	s, _ := Open("test")
	go s.Serve(":6666")

	ctx := context.TODO()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6666"})
	rdb.Del(ctx, "geo")

	add := func(key string, lat, long float64) {
		rdb.ZAdd(ctx, "geo", &redis.Z{Score: float64(geohash.EncodeIntWithPrecision(lat, long, 52)), Member: key})
	}

	add("Zhangjiajie", 29.117001, 110.478996)
	add("Xingyi", 25.091999, 104.894997)
	add("Foshan", 23.016666, 113.116669)
	add("Anshan", 41.116669, 122.98333)
	add("Datong", 40.083332, 113.300003)
	add("Luoyang", 34.669724, 112.442223)
	add("Baotou", 40.650002, 109.833336)
	add("Nanchang", 28.683332, 115.883331)
	add("Guiyang", 26.646999, 106.629997)
	add("Zibo", 36.783333, 118.050003)
	add("Tangshan", 39.631001, 118.18)
	add("Changzhou", 31.811001, 119.973999)
	add("Kunming", 25.043333, 102.706108)
	add("Nanning", 22.816668, 108.316666)
	add("Fuzhou", 26.0753, 119.308197)
	add("Changsha", 28.228001, 112.939003)
	add("Jinan", 36.666668, 116.98333)
	add("Harbin", 45.75, 126.633331)
	add("Suzhou", 31.299999, 120.599998)
	add("Dalian", 38.920834, 121.639168)
	add("Qingdao", 36.066898, 120.382698)
	add("Shenyang", 41.799999, 123.400002)
	add("Hangzhou", 30.25, 120.166664)
	add("Nanjing", 32.049999, 118.76667)
	add("Chengdu", 30.657, 104.066002)
	add("Wuhan", 30.583332, 114.283333)
	add("Tianjin", 39.133331, 117.183334)
	// add("Beijing", 916668, 116.383331)
	// add("Shanghai", 224361, 121.46917)
	add("Xuzhou", 34.205769, 117.284126)
	add("Zhengzhou", 34.746613, 113.625328)
	add("Dabu", 24.347782, 116.695198)
	add("Taiyuan", 37.87146, 112.551208)
	add("Tieling", 42.223827, 123.726036)
	add("Shijiazhuang", 38.042805, 114.514893)
	add("Fuqing", 25.721292, 119.384155)
	add("Yichun", 27.819311, 114.41069)
	add("Shihezi", 44.306095, 86.080605)
	add("Jiamusi", 46.801086, 130.32312)
	add("Nagqu", 31.476202, 92.051239)
	add("Wuhu", 31.350012, 118.429398)
	add("Lanzhou", 36.054871, 103.828812)
	add("Qiqihar", 47.354347, 123.918182)
	add("Suqian", 33.965126, 118.270988)
	add("Changchun", 43.891262, 125.331345)
	add("Hohhot", 40.846333, 111.733017)
	add("Linfen", 36.088005, 111.518974)
	add("Wuhai", 39.863373, 106.814575)
	add("Dalangzhen", 22.940195, 113.943916)
	add("Yancheng", 33.347317, 120.163658)
	add("Shenzhen", 22.542883, 114.062996)
	add("Qidong", 31.808025, 121.65744)
	add("Wujiang", 139071, 120.645134)
	add("Xiamen", 24.479834, 118.089424)
	add("Zhongxiang", 31.16782, 112.58812)
	add("Bayingol", 41.749287, 86.168861)
	add("Weifang", 36.706776, 119.161758)
	add("Hefei", 31.848398, 117.272362)
	add("Xigaze", 29.26687, 88.880585)
	add("Haixi", 37.363132, 97.377106)
	add("Jiuquan", 39.732819, 98.494354)
	add("Tumxuk", 39.865776, 79.070511)
	add("Zhaohua", 32.323257, 105.962822)
	add("Zalantun", 48.013733, 122.737465)
	add("Linyi", 35.102074, 118.345329)
	add("Golmud", 36.406086, 94.910286)
	add("Cangnan", 27.51828, 120.425766)
	add("Korla", 41.723091, 86.175682)
	add("Huainan", 32.625477, 116.999931)
	add("Wuxi", 31.565372, 120.327583)
	add("Suqian", 33.96323, 118.2752)
	add("Yinchuan", 38.4888, 106.24929)
	add("Chifeng", 42.259621, 118.888634)
	add("Weihai", 37.509998, 122.120766)
	add("Guangzhou", 23.128994, 113.25325)
	add("Fuxin", 42.035522, 121.659676)
	add("Lianyungang", 34.661453, 119.224319)
	add("Neixiang", 33.05817, 111.851074)
	add("Shannan", 29.240574, 91.774055)
	add("Yongnian", 36.77774, 114.491051)
	add("Changji", 44.011185, 87.308228)
	add("Hulun", 49.220287, 119.75441)
	add("Damagouxiang", 36.98724, 81.07592)
	add("Aksu", 41.176327, 80.275902)
	add("Wuwei", 37.934246, 102.638931)
	add("Ankang", 32.684715, 109.029022)
	add("Zhangjiachuan", 34.991192, 106.206551)
	add("Baoshan", 31.405457, 121.489609)
	add("Anning", 24.919493, 102.478493)
	add("Maoxian", 31.681154, 103.853523)
	add("Bengbu", 32.916286, 117.389717)
	add("Qingpianxiang", 32.030197, 104.013405)
	add("Sihong", 33.485291, 118.224564)
	add("Handan", 36.625656, 114.538963)
	add("Kizilsu", 39.714527, 76.167816)
	add("Dongguan", 23.020536, 113.751762)
	add("Jiangmen", 22.578737, 113.081902)
	add("Hotan", 37.120632, 79.923134)
	add("Nanyang", 32.990833, 112.52832)

	{
		cities := []string{}
		x := rdb.GeoRadius(ctx, "geo", 118.77, 32.05, &redis.GeoRadiusQuery{Radius: 450, WithDist: true}).Val()
		for i := 0; i < len(x); i++ {
			cities = append(cities, x[i].Name)
		}
		sort.Strings(cities)
		assertEqual(cities, []string{"Baoshan", "Bengbu", "Changzhou", "Hangzhou", "Hefei", "Huainan", "Lianyungang", "Linyi", "Nanjing", "Qidong", "Sihong", "Suqian", "Suzhou", "Wuhu", "Wuxi", "Xuzhou", "Yancheng"})
	}
	{
		cities := []string{}
		x := rdb.GeoRadius(ctx, "geo", 118.77, 32.05, &redis.GeoRadiusQuery{Radius: 450, WithDist: true, Count: 5, Sort: "ASC"}).Val()
		for i := 0; i < len(x); i++ {
			cities = append(cities, x[i].Name)
		}
		assertEqual(cities, []string{"Nanjing", "Wuhu", "Changzhou", "Hefei", "Wuxi"})
	}
	{
		cities := []string{}
		x := rdb.GeoRadius(ctx, "geo", 118.77, 32.05, &redis.GeoRadiusQuery{Radius: 200, WithDist: true}).Val()
		for i := 0; i < len(x); i++ {
			cities = append(cities, x[i].Name)
		}
		sort.Strings(cities)
		assertEqual(cities, []string{"Bengbu", "Changzhou", "Hefei", "Huainan", "Nanjing", "Sihong", "Suzhou", "Wuhu", "Wuxi", "Yancheng"})
	}
	s.Close()
}

func TestZSetCache(t *testing.T) {
	rand.Seed(time.Now().Unix())
	ctx := context.TODO()

	s, _ := Open("test")
	go s.Serve(":6666")
	time.Sleep(time.Second)

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6666"})

	s2pkg.PanicErr(rdb.Ping(ctx).Err())

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
