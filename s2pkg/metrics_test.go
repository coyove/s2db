package s2pkg

import (
	"fmt"
	"math/rand"
	"strconv"
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

func TestDoubleMapLRU(t *testing.T) {
	type op struct {
		add bool
		v   int
	}
	ops := []op{}

	const N = 2048
	m := NewMasterLRU(N, nil)
	r := NewLRUCache(N)

	for i := 0; i < 1e5; i++ {
		ops = append(ops, op{true, i})
		if i > 10 && rand.Intn(2) == 0 {
			cnt := rand.Intn(60) + 20
			for z := 0; z < cnt; z++ {
				x := rand.Intn(i)
				ops = append(ops, op{false, x})
			}
		}
	}

	start := time.Now()
	for _, op := range ops {
		if op.add {
			m.Add("", strconv.Itoa(op.v), op.v)
		} else {
			m.Get(strconv.Itoa(op.v))
		}
	}
	hcDiff := time.Since(start)

	start = time.Now()
	for _, op := range ops {
		if op.add {
			r.Add(strconv.Itoa(op.v), op.v)
		} else {
			r.Get(strconv.Itoa(op.v))
		}
	}
	refDiff := time.Since(start)

	miss := 0
	r.Info(func(k LRUKey, v interface{}, a int64, b int64) {
		_, ok := m.Get(k.(string))
		// fmt.Println("ref", k, tv)
		if !ok {
			miss++
		}
	})
	fmt.Println(r.Len(), miss, m.Cap(), "diff:", hcDiff, refDiff)

	const M = 1e4
	for i := 0; i < 1e4; i++ {
		start := time.Now()
		const cap = 100
		m = NewMasterLRU(cap, nil)
		for i := 0; i < 1000; i++ {
			m.Add("master", strconv.Itoa(i), i)
		}
		cnt := m.Delete("master")
		if i == M-1 {
			fmt.Println("delete master: ", cnt, m.Cap(), time.Since(start)*M)
		}
		if m.Len() != 0 {
			t.Fatal(m.Len())
		}
	}

	m = NewMasterLRU(1, func(kv LRUKeyValue) {
		fmt.Println(kv.Slave)
	})
	for i := 0; i < 7; i++ {
		m.Add("", strconv.Itoa(i), i)
	}
	m.Range(func(kv LRUKeyValue) bool {
		fmt.Println(kv.Slave, "=", kv.Value)
		return true
	})
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
