package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	addr             = flag.String("addr", ":6379", "benchmark endpoint")
	benchmark        = flag.String("n", "", "")
	benchmarkClients = flag.Int("clients", 100, "")
	benchmarkOp      = flag.Int("op", 10000, "")
	benchmarkDefer   = flag.Bool("defer", false, "")
	multiKeysNum     = flag.Int("keysnum", 10, "")
)

func panicErr(err error) {
	if err != nil {
		panic(err)
		os.Exit(-1)
	}
}

func main() {
	ctx := context.TODO()

	flag.Parse()
	rand.Seed(time.Now().Unix())

	rdb := redis.NewClient(&redis.Options{
		Addr:        *addr,
		DialTimeout: time.Second / 2,
	})

	start := time.Now()
	switch *benchmark {
	case "zset_write_one":
		wg := sync.WaitGroup{}
		for i := 0; i < *benchmarkClients; i++ {
			wg.Add(1)
			go func(i int) {
				fmt.Println("client #", i)
				for c := 0; c < *benchmarkOp; c++ {
					m := &redis.Z{Member: strconv.Itoa(c + i**benchmarkClients), Score: rand.Float64()*10 - 5}
					rdb.ZAdd(ctx, "bench", m)
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	case "zset_write_multi":
		wg := sync.WaitGroup{}
		for i := 0; i < *benchmarkClients; i++ {
			wg.Add(1)
			go func(i int) {
				fmt.Println("client #", i)
				for c := 0; c < *benchmarkOp; c++ {
					key := fmt.Sprintf("bench%d", rand.Intn(*multiKeysNum))
					m := &redis.Z{Member: strconv.Itoa(c + i**benchmarkClients), Score: rand.Float64()*10 - 5}
					if *benchmarkDefer {
						rdb.Do(ctx, "ZADD", key, "--defer--", m.Score, m.Member)
					} else {
						rdb.ZAdd(ctx, key, m)
					}
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	case "set_write_multi_many", "zset_write_multi_many":
		cmd := "ZADD"
		if *benchmark == "set_write_multi_many" {
			cmd = "SADD"
		}
		N := 100
		wg := sync.WaitGroup{}
		for i := 0; i < *benchmarkClients; i++ {
			wg.Add(1)
			go func(i int) {
				fmt.Println("client #", i)
				for c := 0; c < *benchmarkOp/N; c++ {
					key := fmt.Sprintf("bench%d", rand.Intn(*multiKeysNum))
					args := []interface{}{cmd, key, "--defer--"}
					for ib := 0; ib < N; ib++ {
						if cmd == "SADD" {
							args = append(args, strconv.Itoa((c+i**benchmarkClients)*N+ib))
						} else {
							args = append(args, rand.Float64()*10-5, strconv.Itoa((c+i**benchmarkClients)*N+ib))
						}
					}
					rdb.Do(ctx, args...)
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	case "zset_write_pipe":
		wg := sync.WaitGroup{}
		for i := 0; i < *benchmarkClients; i++ {
			wg.Add(1)
			go func(i int) {
				fmt.Println("client #", i)
				p := rdb.Pipeline()
				for c := 0; c < 100; c++ {
					p.ZAdd(ctx, "bench", &redis.Z{Member: strconv.Itoa(c), Score: rand.Float64()*10 - 5})
				}
				_, err := p.Exec(ctx)
				if err != nil {
					fmt.Println(err)
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	case "zset_seq_write_1":
		for i := 0; i < 1000; i += 1 {
			args := []interface{}{"ZADD", "seqbench"}
			for c := 0; c < 100; c++ {
				args = append(args, i*100+c, fmt.Sprintf("s%09d", i*100+c))
			}
			cmd := redis.NewStringCmd(ctx, args...)
			rdb.Process(ctx, cmd)
			if cmd.Err() != nil {
				fmt.Println(i, cmd.Err())
			}
		}
	}
	fmt.Println("finished in", time.Since(start).Seconds(), "s")
}
