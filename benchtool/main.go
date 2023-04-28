package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	addr      = flag.String("addr", ":6379", "benchmark endpoint")
	clients   = flag.Int("clients", 100, "")
	ops       = flag.Int("op", 10000, "")
	ttl       = flag.Int("ttl", 0, "")
	keyNum    = flag.Int("k", 1, "")
	keyPrefix = flag.String("kp", "", "")
)

func main() {
	ctx := context.TODO()

	flag.Parse()
	rand.Seed(time.Now().Unix())

	rdb := redis.NewClient(&redis.Options{
		Addr:        *addr,
		DialTimeout: time.Second / 2,
	})

	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(i int) {
			fmt.Println("client #", i)
			for c := 0; c < *ops; c++ {
				idx := i**clients + c
				rdb.Do(ctx, "APPEND", "defer", "TTL", *ttl, *keyPrefix+strconv.Itoa(rand.Intn(*keyNum)), idx)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	fmt.Println(time.Since(start))
}
