package main

import (
	"fmt"
	"testing"
	"time"
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
