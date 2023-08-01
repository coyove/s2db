package main

import (
	"context"
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
	"github.com/coyove/s2db/s2"
	s2pkg "github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/server"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
)

func init() {
	server.InitLogger(false, "0,0,0,", "0,0,0,", "0,0,0,")
	server.EnableTest()
	rand.Seed(future.UnixNano())
	future.StartWatcher(func(error) {})
}

func pairsMap(p []s2pkg.Pair) map[string]s2pkg.Pair {
	m := map[string]s2pkg.Pair{}
	for _, p := range p {
		m[string(p.ID)] = p
	}
	return m
}

func doRange(r *redis.Client, key string, start string, n int, args ...any) []s2pkg.Pair {
	flag := s2.SelectOptions{}
	if n < 0 {
		n = -n
		flag.Desc = true
	}
	all := false
	for _, el := range args {
		switch el {
		case "all":
			all = true
		}
	}

	a := client.Begin(r)
	p, err := a.Select(context.TODO(), &flag, key, start, n)
	s2pkg.PanicErr(err)

	if all {
		for _, p := range p {
			if !p.All {
				panic("not all peers respond")
			}
		}
	}
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

func prepareServers() (*redis.Client, *redis.Client, *server.Server, *server.Server) {
	os.RemoveAll("test/6666")
	os.RemoveAll("test/7777")

	s1, err := server.Open("test/6666", 1)
	s2pkg.PanicErr(err)
	go s1.Serve(":6666")

	s2, err := server.Open("test/7777", 2)
	s2pkg.PanicErr(err)
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
		if rand.Intn(2) == 1 {
			s2.Interop.Append(&s2pkg.AppendOptions{Effect: true}, "a", fmt.Sprintf("%d", count))
		} else {
			s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", count, "EFFECT").Err())
		}
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

// func TestAppendEmpty(t *testing.T) {
// 	rdb1, rdb2, s1, s2 := prepareServers()
// 	defer s1.Close()
// 	defer s2.Close()
//
// 	ctx := context.TODO()
// 	for i := 0; i < 10; i++ {
// 		s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", i).Err())
// 		time.Sleep(time.Millisecond * 200)
// 	}
//
// 	data1 := doRange(rdb2, "a", "now", -100)
// 	for i := range data1 {
// 		if future.UnixNano()-data1[i].UnixNano() > 1e9 {
// 			data1 = data1[:i]
// 			break
// 		}
// 	}
// 	// s2pkg.PanicErr(rdb2.Do(ctx, "APPEND", "a", "", "TTL", 1, "SYNC").Err())
// 	s2.Interop.Append("Ttl=1Sync", "a", nil)
//
// 	data2 := doRange(rdb2, "a", "now", -100)
// 	if len(data2) != len(data1) {
// 		t.Fatal(data1, data2)
// 	}
// }

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

	s2.TestFlags.Fail = true

	data := doRange(rdb1, "a", "+inf", -10)
	fmt.Println(data)

	data, _ = s1.Interop.Select(&s2pkg.SelectOptions{Desc: true}, "a", []byte("+inf"), 10)
	if len(data) == 0 {
		t.Fatal(data)
	}
	for _, d := range data {
		if d.Con {
			t.Fatal(data)
		}
	}
	s2.TestFlags.Fail = false

	data = doRange(rdb1, "a", "+inf", -20)

	data = s2pkg.TrimPairsForConsolidation(data, true, false)
	trimmed := pairsMap(data)
	if len(trimmed) == 0 {
		t.Fatal(data)
	}
	fmt.Println("trimmed", data)

	data = doRange(rdb1, "a", "+inf", -20)
	for _, p := range data {
		if p.Con {
			if _, ok := trimmed[string(p.ID)]; !ok {
				t.Fatal(data)
			}
		}
	}

	data = doRange(rdb2, "a", "0", 4)
	time.Sleep(time.Second)           // consolidation is async-ed
	data = doRange(rdb2, "a", "0", 4) // returns [[1]], [[2]], [[3]], 4
	if !data[0].Con || !data[1].Con || !data[2].Con || data[3].Con {
		t.Fatal(data)
	}

	id3 := string(data[2].IDHex())

	s1.TestFlags.Fail = true
	if x := doRange(rdb2, "a", id3, 1, "all"); !x[0].Equal(data[2]) {
		t.Fatal(x)
	}
	if x := catchPanic(func() { doRange(rdb2, "a", id3, 2, "all") }); x == nil {
		t.Fatal("should fail")
	}
	s1.TestFlags.Fail = false

	doRange(rdb2, "a", id3, 5) // returns [[3]], 4, 10, 11, 12

	s1.TestFlags.Fail = true
	data = doRange(rdb2, "a", id3, 3) // returns [[3]], [[4]], [[10]]
	s1.TestFlags.Fail = false

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

	s1.TestFlags.NoSetMissing = true
	doRange(rdb1, "a", "0", 6) // returns 0, 1, 2, 3, 4, 5
	s1.TestFlags.NoSetMissing = false

	data := doRange(rdb1, "a", "+inf", -7) // returns 15, 14, 13, 12, 11, 10, 9
	if string(data[3].Data) != "12" {
		t.Fatal(data)
	}

	id12 := hex.EncodeToString(data[3].ID)

	s2.TestFlags.Fail = true
	data = doRange(rdb1, "a", id12, -3, "all") // returns 12, 11, 10

	if len(data) != 3 || string(data[1].Data) != "11" {
		t.Fatal(data)
	}

	if err := catchPanic(func() { doRange(rdb1, "a", id12, -4, "all") }); err == nil {
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
		s2pkg.PanicErr(r.Do(ctx, "APPEND", "a", i, "EFFECT").Err())
		time.Sleep(150 * time.Millisecond)
	}

	for i := 0; i < 5; i++ {
		s2pkg.PanicErr(rdb2.Do(ctx, "APPEND", "b", i, "EFFECT").Err())
	}

	doRange(rdb2, "b", "recent", -4)

	s1.TestFlags.IRangeCache = true

	data := doRange(rdb2, "b", "recent", -4, "all")
	if string(data[3].Data) != "1" {
		t.Fatal(data)
	}

	data = doRange(rdb2, "a", "recent", -1, "all")
	if string(data[0].Data) != "9" {
		t.Fatal(data)
	}

	id1 := rdb2.Do(ctx, "APPEND", "c", 0, "AND", 1, "EFFECT").Val().([]any)[1].(string)

	for i := 2; i <= 5; i++ {
		s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "c", i, "EFFECT").Err())
	}
	data = doRange(rdb1, "c", id1, 3) // returns 1, 2, 3
	if len(data) != 3 || string(data[2].Data) != "3" {
		t.Fatal(data)
	}

	fmt.Println(s1.Interop.Scan("", 100, false))
}

// func TestDistinct(t *testing.T) {
// 	rdb1, rdb2, s1, s2 := prepareServers()
// 	defer s1.Close()
// 	defer s2.Close()
//
// 	ctx := context.TODO()
// 	for i := 0; i < 10; i++ {
// 		s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "a", i/2*2, "AND", 100).Err())
// 		s2pkg.PanicErr(rdb2.Do(ctx, "APPEND", "a", i/2*2+1, "AND", "\x00\x03100abc").Err())
// 		time.Sleep(150 * time.Millisecond)
// 	}
//
// 	s2.TestFlags.Fail = true
// 	data := doRange(rdb1, "a", "+Inf", -100)
// 	if len(data) != 20 {
// 		t.Fatal(data)
// 	}
// 	s2.TestFlags.Fail = false
//
// 	data = doRange(rdb1, "a", "+Inf", -100, "distinct")
// 	if len(data) != 11 || string(data[10].Data) != "0" {
// 		t.Fatal(data)
// 	}
//
// 	s1.TestFlags.Fail = true
// 	data = doRange(rdb2, "a", "0", 100)
// 	if len(data) != 6 || string(data[0].Data) != "1" {
// 		t.Fatal(data)
// 	}
// 	s1.TestFlags.Fail = false
//
// 	for i := 0; i < 10; i++ {
// 		s2pkg.PanicErr(rdb1.Do(ctx, "APPEND", "b", i).Err())
// 		time.Sleep(150 * time.Millisecond)
// 	}
// 	s2pkg.PanicErr(rdb2.Do(ctx, "APPEND", "b", 10).Err())
// 	data = doRange(rdb2, "b", "+inf", -2)
// 	if len(data) != 2 {
// 		t.Fatal(data)
// 	}
// }

// func TestTTL(t *testing.T) {
// 	rdb1, rdb2, s1, s2 := prepareServers()
// 	defer s1.Close()
// 	defer s2.Close()
//
// 	ctx := context.TODO()
// 	var ids []string
// 	for i := 0; i <= 20; i++ {
// 		id := rdb2.Do(ctx, "APPEND", "a", i, "TTL", 1).Val().([]any)[0].(string)
// 		ids = append(ids, id)
// 		time.Sleep(time.Duration(rand.Intn(100)+200) * time.Millisecond)
// 	}
//
// 	var expired []string
// 	for i := range ids {
// 		buf, _ := hex.DecodeString(ids[i])
// 		sec := int64(binary.BigEndian.Uint64(buf) / 1e9)
// 		if sec >= future.UnixNano()/1e9-1 {
// 			expired = ids[:i]
// 			ids = ids[i:]
// 			break
// 		}
// 	}
//
// 	data := doRange(rdb1, "a", "0", 100)
//
// 	for i := 0; i < len(data) && i < len(ids); i++ {
// 		if string(data[len(data)-1-i].IDHex()) != ids[len(ids)-1-i] {
// 			t.Fatal(data, ids)
// 		}
// 	}
//
// 	res, _ := client.Begin(rdb1).Lookup(context.TODO(), ids[len(ids)-1])
// 	if string(res) != "20" {
// 		t.Fatal(string(res))
// 	}
//
// 	fmt.Println(expired, rdb1.Do(ctx, "COUNT", "a").Val())
//
// 	data = doRange(rdb1, "a", "0", 100, "raw")
// 	fmt.Println(data)
// 	// for _, ex := range expired {
// 	// 	v := (rdb1.Get(ctx, ex).Val())
// 	// 	if v != "" {
// 	// 		t.Fatal(v, ex)
// 	// 	}
// 	// }
// }

// func TestQuorum(t *testing.T) {
// 	rdb1, rdb2, s1, s2 := prepareServers()
// 	defer s1.Close()
// 	defer s2.Close()
//
// 	ctx := context.TODO()
// 	var ids []string
// 	for i := 0; i < 20; i++ {
// 		id := rdb1.Do(ctx, "APPEND", "a", i, "SYNC").Val().([]any)[0].(string)
// 		ids = append(ids, id)
// 		time.Sleep(150 * time.Millisecond)
// 	}
// 	time.Sleep(time.Second)
//
// 	data := doRange(rdb2, "a", "0", 100)
// 	for i := range ids {
// 		if string(data[i].IDHex()) != ids[i] {
// 			t.Fatal(data, ids)
// 		}
// 	}
//
// 	rdb1.Do(ctx, "APPEND", "a", 100, "TTL", 1, "SYNC")
// 	time.Sleep(time.Second)
//
// 	data2 := doRange(rdb2, "a", "0", 100)
// 	if len(data2) >= len(data) {
// 		t.Fatal(data2)
// 	}
// }

func TestFuzzy1(t *testing.T) {
	rdb1, rdb2, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	ctx := context.TODO()
	ss := []*server.Server{s1, s2}

	const N = 500
	rand.Seed(future.UnixNano())
	for i := 0; i < 5; i++ {
		s := ss[rand.Intn(len(ss))]
		s.TestFlags.Fail = true
		go func() {
			time.Sleep(time.Duration(rand.Intn(400)+500) * time.Millisecond)
			s.TestFlags.Fail = false
		}()

		wg := sync.WaitGroup{}
		for j := 0; j < N/5; j++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				time.Sleep(time.Duration(rand.Intn(1000)+500) * time.Millisecond)
				a := client.Begin(rdb1, rdb2)
				a.Append(ctx, nil, "a", i)
				a.WaitEffect()
			}(i*100 + j)
		}
		wg.Wait()
	}

	s1.TestFlags.Fail = false
	s2.TestFlags.Fail = true
	data1 := doRange(rdb1, "a", "recent", -N)

	s1.TestFlags.Fail = true
	s2.TestFlags.Fail = false
	data2 := doRange(rdb2, "a", "recent", -N)

	fmt.Println(len(data1), len(data2))

	s1.TestFlags.Fail = false
	s2.TestFlags.Fail = false
	data := doRange(rdb1, "a", "recent", -N)

	m := map[string]bool{}
	for _, d := range data {
		m[string(d.Data)] = true
	}

	if len(m) != N {
		t.Fatal(len(m), len(data))
	}
}

func TestSelectUnions(t *testing.T) {
	_, _, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	const N = 400
	const K = 10
	for i := 0; i < N; i++ {
		s := s1
		if rand.Intn(2) == 0 {
			s = s2
		}
		s.Interop.Append(nil, "a"+strconv.Itoa(rand.Intn(K)), i)
		if i%100 == 0 {
			fmt.Println(i)
		}
	}
	// hack
	time.Sleep(time.Second)
	opts := &s2pkg.SelectOptions{}
	for i := 0; i < K; i++ {
		k := "a" + strconv.Itoa(i)
		opts.Unions = append(opts.Unions, k)
		p, _ := s1.Interop.Select(nil, k, nil, 1000)
		fmt.Println("#", i, "=", len(p))
	}
	opts.Unions = opts.Unions[1:]

	m := map[int]bool{}
	var start []byte
MORE:
	p, _ := s1.Interop.Select(opts, "a0", start, 20)
	for _, p := range p {
		v, _ := strconv.Atoi(string(p.Data))
		m[v] = true
	}
	if len(p) > 0 {
		start = p[len(p)-1].ID
		opts.LeftOpen = true
		goto MORE
	}

	if len(m) != N {
		t.Fatal(len(m))
	}
}
