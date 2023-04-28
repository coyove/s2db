package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

func init() {
	slowLogger = log.New()
	dbLogger = log.New()
	testFlag = true
	rand.Seed(clock.UnixNano())
}

func pairsMap(p []s2pkg.Pair) map[string]s2pkg.Pair {
	m := map[string]s2pkg.Pair{}
	for _, p := range p {
		m[string(p.ID)] = p
	}
	return m
}

func doRange(r *redis.Client, key string, start string, n int) []s2pkg.Pair {
	cmd := redis.NewStringSliceCmd(context.TODO(), "RANGE", key, start, n)
	r.Process(context.TODO(), cmd)
	s2pkg.PanicErr(cmd.Err())
	return s2pkg.ConvertBulksToPairs(cmd.Val())
}

func catchPanic(f func()) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("catchPanic: %v", r)
		}
	}()
	f()
	return
}

func prepareServers() (*redis.Client, *redis.Client, *Server, *Server) {
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

	return rdb1, rdb2, s1, s2
}

func TestAppend(t *testing.T) {
	rdb1, rdb2, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	ctx := context.TODO()
	count := 0
	for start := time.Now(); count < 50 || time.Since(start).Seconds() < 5; count++ {
		r := rdb1
		if rand.Intn(2) == 1 {
			r = rdb2
		}
		s2pkg.PanicErr(r.Do(ctx, "APPEND", "a", count).Err())
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

	data = s2pkg.TrimPairsForConsolidation(data)
	ts := string(data[0].IDHex())
	data = doRange(rdb2, "a", ts, N)
	fmt.Println(data, len(data))
	fmt.Println(strings.Repeat("-", 10))

	time.Sleep(time.Second / 2)
	data = doRange(rdb2, "a", ts, N)
	fmt.Println(data)
}

func TestConsolidation(t *testing.T) {
	rdb1, rdb2, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	ctx := context.TODO()
	s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", 1, 2).Err())
	time.Sleep(200 * time.Millisecond)
	s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", 3).Err())
	time.Sleep(200 * time.Millisecond)
	s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", 4).Err())

	for i := 10; i <= 15; i += 2 {
		s2pkg.PanicErr(rdb2.Do(ctx, "APPEND", "a", i, i+1).Err())
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
	s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", 20, 21, 22).Err())

	s2.test.Fail = true

	data := doRange(rdb1, "a", "+inf", -10)
	fmt.Println(data)
	time.Sleep(time.Second)

	data = doRange(rdb1, "a", "+inf", -10)
	for _, d := range data {
		if d.C {
			t.Fatal(data)
		}
	}
	s2.test.Fail = false

	data = doRange(rdb1, "a", "+inf", -20)
	time.Sleep(time.Second)

	data = s2pkg.TrimPairsForConsolidation(data)
	trimmed := pairsMap(data)
	if len(trimmed) == 0 {
		t.Fatal(data)
	}
	fmt.Println("trimmed", data)

	data = doRange(rdb1, "a", "+inf", -20)
	for _, p := range data {
		if p.C {
			if _, ok := trimmed[string(p.ID)]; !ok {
				t.Fatal(data)
			}
		}
	}

	data = doRange(rdb2, "a", "0", 4)
	time.Sleep(time.Second)
	data = doRange(rdb2, "a", "0", 4) // returns 1, 2, [[3]], 4
	if data[0].C || data[1].C || !data[2].C || data[3].C {
		t.Fatal(data)
	}

	id3 := string(data[2].IDHex())

	s1.test.Fail = true
	s2.test.MustAllPeers = true
	if x := doRange(rdb2, "a", id3, 1); !x[0].Equal(data[2]) {
		t.Fatal(x)
	}
	if x := catchPanic(func() { doRange(rdb2, "a", id3, 2) }); x == nil {
		t.Fatal("should fail")
	}
	s2.test.MustAllPeers = false
	s1.test.Fail = false

	doRange(rdb2, "a", id3, 5) // returns [[3]], 4, 10, 11, 12
	time.Sleep(time.Second)

	s1.test.Fail = true
	data = doRange(rdb2, "a", id3, 3) // returns [[3]], [[4]], [[10]]
	s1.test.Fail = false

	if !s2pkg.AllPairsConsolidated(data) {
		t.Fatal(data)
	}
}

func TestConsolidation2(t *testing.T) {
	rdb1, rdb2, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	ctx := context.TODO()
	for i := 0; i < 5; i++ {
		s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", i).Err())
		time.Sleep(200 * time.Millisecond)
	}
	for i := 5; i < 10; i++ {
		s2pkg.PanicErr(rdb2.Do(ctx, "APPEND", "a", i).Err())
		time.Sleep(200 * time.Millisecond)
	}
	for i := 10; i < 15; i++ {
		s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", i).Err())
		time.Sleep(200 * time.Millisecond)
	}

	doRange(rdb1, "a", "0", 6)     // returns 0, 1, 2, 3, 4, 5
	doRange(rdb1, "a", "+inf", -6) // returns 14, 13, 12, 11, 10, 9
	time.Sleep(time.Second)
}

func TestTTL(t *testing.T) {
	rdb1, rdb2, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	ctx := context.TODO()
	var ids []string
	for i := 0; i <= 20; i++ {
		id := rdb2.Do(ctx, "APPEND", "TTL", 1, "a", i).Val().([]any)[0].(string)
		ids = append(ids, id)
		time.Sleep(time.Duration(rand.Intn(100)+200) * time.Millisecond)
	}

	var expired []string
	for i := range ids {
		buf, _ := hex.DecodeString(ids[i])
		sec := int64(binary.BigEndian.Uint64(buf) / 1e9)
		if sec >= future.UnixNano()/1e9-1 {
			expired = ids[:i]
			ids = ids[i:]
			break
		}
	}

	data := doRange(rdb1, "a", "0", 100)

	for i := 0; i < len(data) && i < len(ids); i++ {
		if string(data[len(data)-1-i].IDHex()) != ids[len(ids)-1-i] {
			t.Fatal(data, ids)
		}
	}

	for _, ex := range expired {
		v := (rdb1.Get(ctx, ex).Val())
		if v != "" {
			t.Fatal(v, ex)
		}
	}
}
