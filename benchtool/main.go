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
	mode      = flag.String("mode", "append", "")
	dedupMode = flag.Bool("distinct", false, "")
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
			for c := 0; c < *ops; c++ {
				switch *mode {
				case "select":
					m := ""
					n := 1000
					if *dedupMode {
						m = "distinct"
						n = 100
					}
					k := *keyPrefix + strconv.Itoa(rand.Intn(*keyNum))
					c := fmt.Sprintf("%d", time.Now().Unix())
					v := rdb.Do(ctx, "select", k, c, n, "desc", m).Val()
					if v == nil {
						fmt.Println(k, c)
					} else {
						l.Add(int64(len(v.([]any))))
					}
				case "append":
					args := []any{"APPEND"}
					args = append(args, *keyPrefix+strconv.Itoa(rand.Intn(*keyNum)), "TTL", *ttl)
					args = append(args, rand.Intn(10))
					rdb.Do(ctx, args...)
				case "hset":
					args := []any{"HSET"}
					args = append(args, *keyPrefix+strconv.Itoa(rand.Intn(*keyNum)))
					args = append(args, fmt.Sprintf("member%d", rand.Intn(1000)), rand.Intn(1000))
					rdb.Do(ctx, args...)
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
