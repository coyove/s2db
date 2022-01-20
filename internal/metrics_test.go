package internal

import (
	"math/rand"
	"testing"
	"time"
)

func TestMetrics(t *testing.T) {
	rand.Seed(time.Now().Unix())

	var s Survey
	m, _ := s.Calc()
	if m[0] != 0 {
		t.Fatal(s)
	}

	ii, _ := s._i()
	for i := range s.value {
		if i == int(ii) {
			continue
		}
		s.value[i] = rand.Int63()
	}

	m, _ = s.Calc()
	if m[0] != 0 {
		t.Fatal(s)
	}

	s.Incr(60)
	m, q := s.Calc()
	if q[0] != 60 {
		t.Fatal(s)
	}
	if m[0] != 1.0/60 {
		t.Fatal(s)
	}

	time.Sleep(time.Second)
	s.Incr(30)

	m, q = s.Calc()
	if q[0] != 45 {
		t.Fatal(s)
	}
	if m[0] != 2.0/60 {
		t.Fatal(s)
	}
}
