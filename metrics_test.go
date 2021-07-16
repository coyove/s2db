package main

import (
	"fmt"
	"testing"
	"time"
)

func TestMetrics(t *testing.T) {
	s := Survey{}
	s.Incr(10)
	s.Incr(1)
	time.Sleep(time.Second)
	s.Incr(1)
	fmt.Println(s.QPS())
}
