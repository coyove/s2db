package s2pkg

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestMetrics(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var s Survey
	m := s.Metrics()
	if m.QPS[0] != 0 {
		t.Fatal(s)
	}

	ii, _ := s._i()
	for i := range s.Value {
		if i == int(ii) {
			continue
		}
		s.Value[i] = rand.Int63()
	}

	m = s.Metrics()
	if m.QPS[0] != 0 {
		t.Fatal(s)
	}

	s.Incr(60)
	time.Sleep(surveyIntervalSec * time.Second)
	m = s.Metrics()
	if m.Mean[0] != 60 {
		t.Fatal(s)
	}
	if m.QPS[0] != 1.0/60 {
		t.Fatal(s)
	}

	s.Incr(30)

	time.Sleep(surveyIntervalSec * time.Second)
	m = s.Metrics()
	if m.Mean[0] != 45 {
		t.Fatal(s)
	}
	if m.QPS[0] != 2.0/60 {
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

func TestMatch(t *testing.T) {
	if !Match("^[123\nabc", "abc") {
		t.Fatal()
	}
	if Match("^[\"123\" \nabc", "123abc") {
		t.Fatal()
	}
	if Match("^[\"123\"\n^[456\nabc", "456abc") {
		t.Fatal()
	}
	if !Match("\\^\\[\"[123]\"^\"[456]\"abc", "^[\"3\"^\"6\"abc") {
		t.Fatal()
	}
	if !Match("^[\"\\\"\"\nabc", "abc") {
		t.Fatal()
	}
	if !Match("^[\"\\\"\"\nabc", "abc") {
		t.Fatal()
	}
}
