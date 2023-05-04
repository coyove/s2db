package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
)

var (
	addr      = flag.String("addr", ":6379", "benchmark endpoint")
	clients   = flag.Int("clients", 100, "")
	ops       = flag.Int("op", 10000, "")
	ttl       = flag.Int("ttl", 0, "")
	keyNum    = flag.Int("k", 1, "")
	keyPrefix = flag.String("kp", "", "")
	readMode  = flag.Bool("read", false, "")
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
	var tot atomic.Int64
	wg := sync.WaitGroup{}
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(i int) {
			fmt.Println("client #", i)
			for c := 0; c < *ops; c++ {
				idx := i**clients + c
				start := future.UnixNano()
				if *readMode {
					rdb.Do(ctx, "RANGE", *keyPrefix+strconv.Itoa(rand.Intn(*keyNum)), fmt.Sprintf("@%d", time.Now().Unix()), -10)
				} else {
					rdb.Do(ctx, "APPEND", "defer", "TTL", *ttl, *keyPrefix+strconv.Itoa(rand.Intn(*keyNum)), idx)
				}
				tot.Add(future.UnixNano() - start)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	fmt.Println(time.Since(start))
	fmt.Println(tot.Load()/int64(*ops**clients)/1e6, "ms")
}
