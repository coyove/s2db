package main

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

func TestMetrics(t *testing.T) {
	s := Survey{}
	// for i := range s.data {
	// 	s.data[i] = rand.Int31()
	// }
	for i := 0; i < 3; i++ {
		s.Incr(int64(i) + 1)
		time.Sleep(time.Second * 2)
	}
	fmt.Println(s.QPS())
	fmt.Println(s.data)
}

func BenchmarkChan(b *testing.B) {
	x := make([]uint64, 1e4)
	for i := range x {
		x[i] = rand.Uint64()
	}
	for i := 0; i < b.N; i++ {
		bbolt.SortU8(x)
	}
}

func BenchmarkChan2(b *testing.B) {
	x := make([]int, 1e4)
	for i := range x {
		x[i] = rand.Int()
	}
	for i := 0; i < b.N; i++ {
		sort.Ints(x)
	}
}
