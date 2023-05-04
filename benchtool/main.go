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
	mgetMode  = flag.Bool("mget", false, "")
)

func main() {
	ctx := context.TODO()

	flag.Parse()
	rand.Seed(time.Now().Unix())

	rdb := redis.NewClient(&redis.Options{
		Addr:        *addr,
		DialTimeout: time.Second / 2,
		ReadTimeout: 10 * time.Second,
	})

	start := time.Now()
	var tot, l atomic.Int64
	wg := sync.WaitGroup{}
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(i int) {
			fmt.Println("client #", i)
			start := future.UnixNano()
			lastID := "00000000000000000000000000000000"
			for c := 0; c < *ops; c++ {
				if *readMode {
					m := ""
					if *mgetMode {
						m = "mget"
					}
					k := *keyPrefix + strconv.Itoa(rand.Intn(*keyNum))
					c := fmt.Sprintf("@%d", time.Now().Unix())
					v := rdb.Do(ctx, "RANGE", k, c, -1000, m).Val()
					if v == nil {
						fmt.Println(k, c)
					} else {
						l.Add(int64(len(v.([]any))))
					}
				} else {
					id := rdb.Do(ctx, "APPEND", "defer", "TTL", *ttl, *keyPrefix+strconv.Itoa(rand.Intn(*keyNum)), lastID).Val().([]any)[0].(string)
					lastID = id
				}
			}
			tot.Add(future.UnixNano() - start)
			wg.Done()
		}(i)
	}
	wg.Wait()

	fmt.Println(time.Since(start))
	fmt.Println(tot.Load()/int64(*ops**clients)/1e6, "ms")
	fmt.Println(l.Load() / int64(*ops**clients))
}
