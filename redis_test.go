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
	"sync"
	"testing"
	"time"

	"github.com/coyove/s2db/client"
	s2pkg "github.com/coyove/s2db/s2"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

func init() {
	slowLogger = log.New()
	dbLogger = log.New()
	testFlag = true
	rand.Seed(future.UnixNano())
}

func pairsMap(p []s2pkg.Pair) map[string]s2pkg.Pair {
	m := map[string]s2pkg.Pair{}
	for _, p := range p {
		m[string(p.ID)] = p
	}
	return m
}

func doRange(r *redis.Client, key string, start string, n int, dedup ...any) []s2pkg.Pair {
	flag := client.ONCE
	if n < 0 {
		n = -n
		flag = flag | client.DESC
	}
	if len(dedup) > 0 {
		flag = flag | client.DISTINCT
	}

	a := client.Begin(r)
	p, err := a.Select(context.TODO(), key, start, n, flag)
	s2pkg.PanicErr(err)
	return p
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
	s2pkg.PanicErr(rdb1.ConfigSet(ctx, "Peer2", "127.0.0.1:7777").Err())
	s2pkg.PanicErr(rdb2.ConfigSet(ctx, "Peer1", "127.0.0.1:6666").Err())

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
		s2pkg.PanicErr(r.Do(ctx, "APPEND", "a", count, "WAIT").Err())
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

	data = s2pkg.TrimPairsForConsolidation(data, true, true)
	ts := string(data[0].IDHex())
	data = doRange(rdb2, "a", ts, N)
	fmt.Println(data, len(data))
	fmt.Println(strings.Repeat("-", 10))

	data = doRange(rdb2, "a", ts, N)
	fmt.Println(data)
}

func TestConsolidation(t *testing.T) {
	rdb1, rdb2, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	ctx := context.TODO()
	s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", 1, "AND", 2).Err())
	time.Sleep(200 * time.Millisecond)
	s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", 3).Err())
	time.Sleep(200 * time.Millisecond)
	s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", 4).Err())

	for i := 10; i <= 15; i += 2 {
		s2pkg.PanicErr(rdb2.Do(ctx, "APPEND", "a", i, "AND", i+1).Err())
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
	s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", 20, "AND", 21, "AND", 22).Err())

	s2.test.Fail = true

	data := doRange(rdb1, "a", "+inf", -10)
	fmt.Println(data)

	data = doRange(rdb1, "a", "+inf", -10)
	for _, d := range data {
		if d.C {
			t.Fatal(data)
		}
	}
	s2.test.Fail = false

	data = doRange(rdb1, "a", "+inf", -20)

	data = s2pkg.TrimPairsForConsolidation(data, true, false)
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
	data = doRange(rdb2, "a", "0", 4) // returns [[1]], [[2]], [[3]], 4
	if !data[0].C || !data[1].C || !data[2].C || data[3].C {
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
	for i := 10; i <= 15; i += 2 {
		s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", i, "AND", i+1).Err())
		time.Sleep(200 * time.Millisecond)
	}

	doRange(rdb1, "a", "0", 6) // returns 0, 1, 2, 3, 4, 5

	s1.test.NoSetMissing = true
	doRange(rdb1, "a", "0", 6) // returns 0, 1, 2, 3, 4, 5
	s1.test.NoSetMissing = false

	data := doRange(rdb1, "a", "+inf", -7) // returns 15, 14, 13, 12, 11, 10, 9
	if string(data[3].Data) != "12" {
		t.Fatal(data)
	}

	id12 := hex.EncodeToString(data[3].ID)

	s2.test.Fail = true
	s1.test.MustAllPeers = true
	data = doRange(rdb1, "a", id12, -3) // returns 12, 11, 10

	if len(data) != 3 || string(data[1].Data) != "11" {
		t.Fatal(data)
	}

	if err := catchPanic(func() { doRange(rdb1, "a", id12, -4) }); err == nil {
		t.FailNow()
	} else {
		fmt.Println(err)
	}
}

func TestWatermark(t *testing.T) {
	rdb1, rdb2, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	ctx := context.TODO()
	for i := 0; i < 10; i++ {
		r := rdb1
		if i%2 == 1 {
			r = rdb2
		}
		s2pkg.PanicErr(r.Do(ctx, "APPEND", "a", i).Err())
		time.Sleep(150 * time.Millisecond)
	}

	for i := 0; i < 5; i++ {
		s2pkg.PanicErr(rdb2.Do(ctx, "APPEND", "b", i).Err())
	}

	doRange(rdb2, "b", "+", -4)

	s1.test.IRangeCache = true
	s2.test.MustAllPeers = true

	data := doRange(rdb2, "b", "+", -4)
	if string(data[3].Data) != "1" {
		t.Fatal(data)
	}

	data = doRange(rdb2, "a", "+", -1)
	if string(data[0].Data) != "9" {
		t.Fatal(data)
	}

	id1 := rdb2.Do(ctx, "APPEND", "c", 0, "AND", 1, "WAIT").Val().([]any)[1].(string)

	for i := 2; i <= 5; i++ {
		s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "c", i, "WAIT").Err())
	}
	data = doRange(rdb1, "c", id1, 3) // returns 1, 2, 3
	if len(data) != 3 || string(data[2].Data) != "3" {
		t.Fatal(data)
	}
}

func TestDistinct(t *testing.T) {
	rdb1, rdb2, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	ctx := context.TODO()
	for i := 0; i < 10; i++ {
		s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", i/2*2, "AND", 100).Err())
		s2pkg.PanicErr(rdb2.Do(ctx, "APPEND", "a", i/2*2+1, "AND", "\x00\x03100abc").Err())
		time.Sleep(150 * time.Millisecond)
	}

	s2.test.Fail = true
	data := doRange(rdb1, "a", "+Inf", -100)
	if len(data) != 20 {
		t.Fatal(data)
	}
	s2.test.Fail = false

	data = doRange(rdb1, "a", "+Inf", -100, "distinct")
	if len(data) != 11 || string(data[10].Data) != "0" {
		t.Fatal(data)
	}

	s1.test.Fail = true
	data = doRange(rdb2, "a", "0", 100)
	if len(data) != 6 || string(data[0].Data) != "1" {
		t.Fatal(data)
	}
	s1.test.Fail = false

	for i := 0; i < 10; i++ {
		s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "b", i).Err())
		time.Sleep(150 * time.Millisecond)
	}
	s2pkg.PanicErr(rdb2.Do(ctx, "APPEND", "b", 10).Err())
	data = doRange(rdb2, "b", "+inf", -2)
	if len(data) != 2 {
		t.Fatal(data)
	}
}

func TestTTL(t *testing.T) {
	rdb1, rdb2, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	ctx := context.TODO()
	var ids []string
	for i := 0; i <= 20; i++ {
		id := rdb2.Do(ctx, "APPEND", "a", i, "TTL", 1).Val().([]any)[0].(string)
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

	res, _ := client.Begin(rdb1).Lookup(context.TODO(), ids[len(ids)-1])
	if string(res) != "20" {
		t.Fatal(string(res))
	}

	fmt.Println(expired, rdb1.Do(ctx, "SELECTCOUNT", "a").Val())

	data = doRange(rdb1, "a", "0", 100, "raw")
	fmt.Println(data)
	// for _, ex := range expired {
	// 	v := (rdb1.Get(ctx, ex).Val())
	// 	if v != "" {
	// 		t.Fatal(v, ex)
	// 	}
	// }
}

func TestQuorum(t *testing.T) {
	rdb1, rdb2, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	ctx := context.TODO()
	var ids []string
	for i := 0; i < 20; i++ {
		id := rdb1.Do(ctx, "APPEND", "a", i, "QUORUM").Val().([]any)[2].(string)
		ids = append(ids, id)
		time.Sleep(150 * time.Millisecond)
	}

	data := doRange(rdb2, "a", "0", 100)
	for i := range ids {
		if string(data[i].IDHex()) != ids[i] {
			t.Fatal(data, ids)
		}
	}

	rdb1.Do(ctx, "APPEND", "a", 100, "QUORUM", "TTL", 1)
	data2 := doRange(rdb2, "a", "0", 100)
	if len(data2) >= len(data) {
		t.Fatal(data2)
	}
}

func TestFuzzy1(t *testing.T) {
	rdb1, rdb2, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	ctx := context.TODO()
	ss := []*Server{s1, s2}

	const N = 500
	rand.Seed(future.UnixNano())
	for i := 0; i < 5; i++ {
		s := ss[rand.Intn(len(ss))]
		s.test.Fail = true
		go func() {
			time.Sleep(time.Duration(rand.Intn(400)+500) * time.Millisecond)
			s.test.Fail = false
		}()

		wg := sync.WaitGroup{}
		for j := 0; j < N/5; j++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				time.Sleep(time.Duration(rand.Intn(1000)+500) * time.Millisecond)
				a := client.Begin(rdb1, rdb2)
				a.Append(ctx, "a", i)
				a.Close()
			}(i*100 + j)
		}
		wg.Wait()
	}

	s1.test.Fail = false
	s2.test.Fail = true
	data1 := doRange(rdb1, "a", "recent", -N)

	s1.test.Fail = true
	s2.test.Fail = false
	data2 := doRange(rdb2, "a", "recent", -N)

	if len(data1)+len(data2) != N {
		t.Fatal(len(data1), len(data2))
	}
	fmt.Println(len(data1), len(data2))

	s1.test.Fail = false
	s2.test.Fail = false
	data := doRange(rdb1, "a", "recent", -N)

	m := map[string]bool{}
	for _, d := range data {
		m[string(d.Data)] = true
	}

	if len(m) != N {
		t.Fatal(len(m), len(data))
	}
}
