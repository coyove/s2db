package main

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestMetrics(t *testing.T) {
	n := 100
	rand.Seed(time.Now().Unix())

	tests := 100
	total := 0
	for i := 0; i < tests; i++ {
		cnt := 0
		for {
			if rand.Intn(n) == 0 {
				break
			}
			cnt++
		}
		total += cnt
	}
	fmt.Println("Hello, playground", float64(total)/float64(tests))

	fmt.Println(hashStr("feed_square:th") % 32)
	return

	s := &Survey{max: 100}
	s.data = [][2]float64{
		{10, 0},
		{20, 0.1},
		{5, 0.5},
		{20, 2},
		{2, 2.1},
		{50, 4},
	}
	q := (s.QPS())
	if q[0][0] != 17.5 || q[1][0] != 11 {
		t.FailNow()
	}
}
