package main

import (
	"testing"
)

func TestMetrics(t *testing.T) {
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
