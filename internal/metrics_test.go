package internal

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestMetrics(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var s Survey
	m, _ := s.Metrics()
	if m[0] != 0 {
		t.Fatal(s)
	}

	ii, _ := s._i()
	for i := range s.Value {
		if i == int(ii) {
			continue
		}
		s.Value[i] = rand.Int63()
	}

	m, _ = s.Metrics()
	if m[0] != 0 {
		t.Fatal(s)
	}

	s.Incr(60)
	m, q := s.Metrics()
	if q[0] != 60 {
		t.Fatal(s)
	}
	if m[0] != 1.0/60 {
		t.Fatal(s)
	}

	time.Sleep(time.Second)
	s.Incr(30)

	m, q = s.Metrics()
	if q[0] != 45 {
		t.Fatal(s)
	}
	if m[0] != 2.0/60 {
		t.Fatal(s)
	}
}

func TestMetrics2(t *testing.T) {
	var s Survey
	for i := 0; i < 10; i++ {
		go func() {
			for {
				s.Incr(rand.Int63n(100))
				time.Sleep(time.Second * time.Duration(rand.Intn(2)+2))
			}
		}()
	}
	for i := 1; i <= 42; i++ {
		fmt.Println(s)
		time.Sleep(time.Second)
	}
	fmt.Println(s.Value)
	fmt.Println(s.Count)
	fmt.Println(s.Ts)
}
