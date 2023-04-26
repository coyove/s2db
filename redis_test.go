package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/s2pkg"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

func init() {
	slowLogger = log.New()
	dbLogger = log.New()
	testFlag = true
	rand.Seed(clock.UnixNano())
}

func doRange(r *redis.Client, key string, start string, n int) []s2pkg.Pair {
	cmd := redis.NewStringSliceCmd(context.TODO(), "RANGE", key, start, n)
	r.Process(context.TODO(), cmd)
	s2pkg.PanicErr(cmd.Err())
	return s2pkg.ConvertBulksToPairs(cmd.Val())
}

func TestAppend(t *testing.T) {
	os.RemoveAll("test/6666")
	os.RemoveAll("test/7777")

	s1, err := Open("test/6666")
	s2pkg.PanicErr(err)
	s1.Channel = 1
	go s1.Serve(":6666")

	s2, err := Open("test/7777")
	s2pkg.PanicErr(err)
	s2.Channel = 2
	go s2.Serve(":7777")

	time.Sleep(time.Second)

	rdb1 := redis.NewClient(&redis.Options{Addr: ":6666"})
	rdb2 := redis.NewClient(&redis.Options{Addr: ":7777"})

	ctx := context.TODO()
	s2pkg.PanicErr(rdb1.ConfigSet(ctx, "Peer2", "127.0.0.1:7777/?Name=1").Err())
	s2pkg.PanicErr(rdb2.ConfigSet(ctx, "Peer1", "127.0.0.1:6666/?Name=2").Err())

	count := 0
	for start := time.Now(); count < 50 || time.Since(start).Seconds() < 5; count++ {
		r := rdb1
		if rand.Intn(2) == 1 {
			r = rdb2
		}
		s2pkg.PanicErr(r.Do(ctx, "APPENDWAIT", "a", count).Err())
	}

	fmt.Println("count:", count)
	N := count / 2

	data := doRange(rdb1, "a", "+inf", -N)
	if len(data) != N {
		t.Fatal(data)
	}
	for i := range data {
		if string(data[i].Data) != strconv.Itoa(count-i-1) {
			t.Fatal(data, i, count)
		}
	}

	data = s2pkg.TrimPairs(data)
	{
		ts := string(data[0].IDHex())
		data = doRange(rdb2, "a", ts, N)
		fmt.Println(data, len(data))
		time.Sleep(time.Second / 2)
		data = doRange(rdb2, "a", ts, N)
		fmt.Println(data)
	}
}
