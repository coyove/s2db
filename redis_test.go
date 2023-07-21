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
				a.Close()
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

func TestHashSet(t *testing.T) {
	rdb1, rdb2, s1, s2 := prepareServers()
	defer s1.Close()
	defer s2.Close()

	ctx := context.TODO()
	rdb1.ConfigSet(ctx, "CompressLimit", "1")
	rdb2.ConfigSet(ctx, "CompressLimit", "1")
	fmt.Println(rdb1.ConfigGet(ctx, "CompressLimit").Val())

	for i := 0; i < 10; i++ {
		rdb1.Do(ctx, "HSET", "h", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), "WAIT")
	}
	time.Sleep(time.Second)
	for i := 0; i < 10; i += 2 {
		rdb2.Do(ctx, "HSET", "h", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i*2), "WAIT")
	}

	data1, _ := client.Begin(rdb1).MustHGetAll(ctx, "h", nil)
	s2.TestFlags.Fail = true
	data2, _ := client.Begin(rdb1).HGetAll(ctx, "h", nil)

	for k, v := range data1 {
		if data2[k] != v {
			t.Fatal(data1, data2)
		}
	}

	s2.TestFlags.Fail = false
	data2, _ = client.Begin(rdb2).MustHGetAll(ctx, "h", nil)
	for k, v := range data1 {
		if data2[k] != v {
			t.Fatal(data1, data2)
		}
	}

	for i := 0; i < 10; i += 3 {
		rdb1.Do(ctx, "HSET", "h", fmt.Sprintf("k%d", i), "v16", "WAIT")
		rdb1.Do(ctx, "HSET", "h", fmt.Sprintf("m%d", i), fmt.Sprintf("m%d", i*2), "WAIT")
	}

	rdb2.Do(ctx, "HSYNC", "h")
	data3 := rdb2.Do(ctx, "HGETALL", "h", "MATCH", "v16", "NOCOMPRESS").Val().([]any)
	if len(data3) != 10 {
		t.Fatal(data3)
	}
	for i := 0; i < len(data3); i += 2 {
		k, v := data3[i].(string), data3[i+1].(string)
		if v != "v16" {
			t.Fatal(data3)
		}
		ki, _ := strconv.Atoi(k)
		if ki%3 != 0 {
			t.Fatal(data3)
		}
	}

	// data4 := rdb2.Do(ctx, "HGETALL", "h", "MATCH", "m1?", "NOCOMPRESS").Val().([]any)
	data4, _ := s2.Interop.HGetAll("h", "m1?")
	if len(data4) != 4 || !(string(data4[0]) == "m6" || string(data4[2]) == "m6") {
		t.Fatal(data4)
	}

	if v := rdb1.HGet(ctx, "h", "k2").Val(); v != "v4" {
		t.Fatal(v)
	}

	dedup := map[string]bool{}
	for i := 0; i < len(staticLZ4); i += 2 {
		id, id2 := staticLZ4[i], staticLZ4[i+1]
		if rand.Intn(2) == 1 {
			q, _ := s1.Interop.HSet(true, "t", []byte(id), []byte("1"), []byte(id2), []byte("1"))
			fmt.Println(q)
		} else {
			q, _ := client.Begin(rdb1).HSet(ctx, "t", id, "1", id2, 1)
			fmt.Println(q)
		}
		dedup[id] = true
		dedup[id2] = true
	}

	fmt.Println(rdb1.Info(ctx, "#t").Val())

	if c := rdb1.HLen(ctx, "t").Val(); c != int64(len(dedup)) {
		t.Fatal(len(dedup), c)
	}

	s1.TestFlags.Fail = true
	if c := rdb2.HLen(ctx, "t").Val(); c != int64(len(dedup)) {
		t.Fatal(len(dedup), c)
	}

	bb, _ := s2pkg.DecompressBulks(strings.NewReader(rdb2.Do(ctx, "HGETALL", "t", "timestamp").Val().(string)))
	for _, l := range bb {
		fmt.Println(string(l))
	}
}

var staticLZ4 = []string{
	"6008195d0da0f356bcfd19cc", "5df5dd003fff2201de91327b", "5ed7c055b92129613c54073b", "61591b7de3d6d111317b6eba", "61d6e113bde8d4734aae4982", "623fc44f4c4ee50bf0a5a8a7", "6251a751e8a4854934700f1f", "626bad5d62a19e07f13bcf0a", "626c10e5e444d52c192d9947", "626e7c7d184dd53f083ff2ba", "627d16e3e444d53063c97730", "62d3e142769b6e02830668ea", "60cf4d21d7fcf748af36f9ba", "62208b991dbd5b3418e6da78", "624c5f109299d7628686a450", "62543841ece002069f5013f0", "6271306e5648d50f8f302c5c", "6271638f1642d538737ecdd3", "628066a0184dd568798de88c", "62b1e54b9ca7ba6d3b0df920", "5febab94e2e2900860246c29", "6027fa755cbf790b8effabd7", "602d4aea8a6f3e7d2beb934d", "60c4f7126156fd44016d0167", "60ddb3585a8775395b475ce0", "60e13e7ef8bcdd194d0d8afc", "60ff6936278df50147aff4d1", "6140a856d17d85268b1feccd", "615c85aa46e3477c628447d5", "616d8ffed849f0597a821d8f", "61a8a240ad993642d2cbb88a", "62603c2062a19e3f713c631e", "628d13a38e4cd556cf70d540", "62af5491a8db7d7c9ea06471", "62dbdbb13f18ae244651c01e", "62ea96c1f77a5b7959dbdc62", "62ee168013fd927b99235f83", "631b8b52aaa6216623eacfb0", "63a5eb1bbcf0260d8591621b", "5fa1650d94f5d9072ebc3e47", "5fa297d7e2e2901d098c090b", "5fe9eaf1ff0add3a1c709470", "60af6c709150797831e230ae", "60d2e934695f1260da23d8df", "616224a3c5614c552a240e8d", "61a6e80c35ace139587310d0", "620750e012d69504518e8987", "6218f0a08bd5983db0d7b900", "6237583d5aa01c5d6d611891", "6262bd3a62a19e602b4dbd78", "62667cbd62a19e0bc98417be", "6292ce6ae444d502fcbd28f2", "62d6d3b2bbf94b701ddb541a", "62dbd55348d0ab0d4bd4a80f", "62dbdd1e6db1a825b2b633a0", "6321d70a8286a17cd1bd3d6c", "635a0c360774a75f2366f6cc", "6055b1b68f1187396b02300f", "61a39d4ebee1603495aaf3e5", "61ccf699e37b322650b9bce8", "61f03bcb190aab686353ef13", "620f1baa1709ca3b4f51d84f", "621ddb89a92c7208bf97e62b", "63406aca51e09867040f3e9b", "635cdf0263a8cc274ccb4c80", "63f03a4988e7f01a35e0c63e", "606432b422b580269a42460c", "600d5c0dd2b56d4904c61fc3", "60c4de89ce4354415ffe0d24", "623b2cf2ef81462434e056ac", "63c19494aceceb51d1fcf1d8", "60879c4b191d3f5731f8e1e3", "60883529251ce51fbee7d9e9", "60d288b367d17467cf242e86", "601785808aad2e0c7babfb40", "62c2e88847da0d7c449d5b0f", "604fa45e7eed8042bc396811", "6050c378cb4a41087f8a3e82", "60aa25326947f11305ecb6d1", "644eaaff2a7dd44c6fca9740", "645254d8414dff53660a1204", "6455f94f30456d6ddb7e265c", "64618fb91b59b031035da449", "60028794ca4fcf23d4f9854b", "60653b040fc05a37640616f7", "60f01824688bc55f5463ae13", "611ccbf02353b41b5fcd1899", "61d16313fc9a662fdb542f0b", "61e58d342fcfaf41d0a98a00", "62185ae8f32f421c0406b2ac", "6244be98336f9b386a22e6a2", "627a053c184dd509d6b1b468", "5ff73f0f352fd56788a13d25", "60810a86fbdbd805750dcad5", "60a691f0eb3e2d43274690cb", "60ca3a8ed8f7374446df4490", "60e49e7359396a7d0f03d23d", "61026acb2571793846b1b3ab", "611689859eb479634eb97d9e", "619f1952033a936c2043c180",
	"61a0fd75d4625f79781e042a", "61cb24a2c538de3e6872addf", "62162ef28f05270c63f99e93", "623729fff23b5c42f22afc1a", "624cdc60d07e4647ea091e61", "5e6c3b7897e03d3a4c6fb6aa", "6289dac38e4cd547c3d33924", "61ed1ace39d7134a15812bd2", "6346ef34d04c0b5fce0c1ca7", "64186599de4d732d202bf688", "64316407c432783c0877fc9b", "606ab494911a4c5595ba927d", "60795873cabdc91368fa4b81", "6143d257e2abfe4247d8b266", "619660c515e89326548873e5", "6296083a184dd543a5d3e855", "615911908a87ce3e4ef8e346", "61875b2bd25aa63feadcbd21", "61ae1dec3c79e4781a2370ba", "63244ad87c38254896d8ec75", "6012bb3fed44974be411051d", "6012bb3fed44974be411051d", "60653c4497b86c38e8aff50b", "60e3e6124d6a3c03fdf6849b", "6066fcc1a6af595ca789f38a", "613c74843a448d7fd6dd7979", "5dabe4b53fff22185d904884", "63d3d543b155bc5d6cf791bf", "6430194c1ec98306a5e9671e", "64458979c680b6538227e00a", "605dd0d8b88f2c583689b581", "6213aa2df964e105f83489c0", "62f3eaa1e0abd628b3dbcc9f", "63d52444e8a2be17afbd0f81", "63dd0a8a89c4692b3608dba1", "640f1eac5dd7093f6daf6acc", "641fa045b54abe3262e16f32", "6074d36fa9c4b505aeb30534", "6080348aac815f68b102955c", "60a7acb68f49c1746a7275d9", "60e33350fc27367b8833c675", "60e38a32224cf102a57ec475", "6264b2c962a19e783f212631", "63442a1360d7b338b7ed234a", "635f5d5fa1dd633dba41a23e", "63c5664c3d81f336d94268f7", "63e3599efee80235cfa146c4", "63f2a5f98ea9217140a0ce8a", "6036ed807fe3096110693d6a", "61113eba79b993186ffb0d1b", "61133fa54761972568268ad7", "6142af73e4c9d02d8f4ffbdb", "6151e712332b8907da402943", "624affdc1faa2e23ae3c9422", "62d2c2826916de34dc6ebfd2", "62de5297db090a0de984dd85", "63121fbafccd157c79bb3e54", "631aad8ed1e6230c3df5f22e", "63271b878123932ecbb83793", "63acd2b4e53d4116213f31eb", "64441258cfd57b2f4a68eb61", "61121630b850406d80892952", "6190d676d6a68723a0b66b3a", "6224b2e36f91bb180e5c24c8", "626e08e3e444d536f5849896", "629bce86184dd50ff454f5da", "629f2d54184dd52e66635765", "62a8f6930ba1d2523f91c18b", "62d22afd9105de689a9b81f5", "634bdd94a5a873293f62b4ee", "63d5cbd2a79aa37f3337af5e", "63e1d71a9819f61102b4eacf", "603dd3500dea330ff37c49ba", "60ff68507d34a4663bee4c5a", "6102dde6a4a78c5aa60a01fb", "610a4ce2010a40427245fa05", "6122387bb250801859238efc", "6131ae77b0cebf286ef8d397", "61bfac6a18cc9c4f8e6d98c2", "6270f521e444d531f2fe7caa", "628ebeb9e444d51474479ab7", "62b2b3524e53c44121d6e62c", "62b8309b55d0a425c1930cc8", "62bdcfcacab0c4489017f0ea", "633ded97b884872b1ecd4d25", "63d52ee12ddbb125cdb2d2be", "63d7d8fb7ea58b35453e5383", "61e5ba8e59259119d233334b", "602661b1cb3bf60c9e6f111c", "6232135b7d833e69ec2bc525", "60164f33e4f342174152ef17", "60631da2bdac245c188c54cf", "6064bdbcfacb1f6203ec8c33", "625d6f8d62a19e0bdfcedb20", "5fbcac9bf02e593d1e937194", "6031031ee06eb5289d6d5d1d", "5ffee83a6365f9411cd1826b", "62279f051c8a974bcbdaf291", "607efbfc739ca23317657670", "608c02c83bcb28357aaa2855",
}
