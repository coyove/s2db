package s2pkg

import (
	"container/list"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
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
		fmt.Println(kv.SlaveKey)
	})
	for i := 0; i < 7; i++ {
		m.Add("", strconv.Itoa(i), i)
	}
	m.Range(func(kv LRUKeyValue) bool {
		fmt.Println(kv.SlaveKey, "=", kv.Value)
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

type LRUCache struct {
	// OnEvicted is called when an entry is going to be purged from the cache.
	OnEvicted func(key LRUKey, value interface{})

	maxWeight int64
	curWeight int64

	ll    *list.List
	cache map[interface{}]*list.Element

	sync.Mutex
}

// A LRUKey may be any value that is comparable. See http://golang.org/ref/spec#Comparison_operators
type LRUKey interface{}

type lruEntry struct {
	key    LRUKey
	value  interface{}
	hits   int64
	weight int64
}

var ErrWeightTooBig = fmt.Errorf("weight can't be held by the cache")

// NewLRUCache creates a new Cache.
func NewLRUCache(maxWeight int64) *LRUCache {
	return &LRUCache{
		maxWeight: maxWeight,
		ll:        list.New(),
		cache:     make(map[interface{}]*list.Element),
	}
}

// Clear clears the cache
func (c *LRUCache) Clear() {
	c.Lock()
	c.ll = list.New()
	c.cache = make(map[interface{}]*list.Element)
	c.Unlock()
}

// Info iterates the cache
func (c *LRUCache) Info(callback func(LRUKey, interface{}, int64, int64)) {
	c.Lock()

	for f := c.ll.Front(); f != nil; f = f.Next() {
		e := f.Value.(*lruEntry)
		callback(e.key, e.value, e.hits, e.weight)
	}

	c.Unlock()
}

// Add adds a value to the cache with weight = 1.
func (c *LRUCache) Add(key LRUKey, value interface{}) error {
	return c.AddWeight(key, value, 1)
}

// AddWeight adds a value to the cache with weight.
func (c *LRUCache) AddWeight(key LRUKey, value interface{}, weight int64) error {
	if weight > c.maxWeight || weight < 1 {
		return ErrWeightTooBig
	}

	c.Lock()
	defer c.Unlock()

	controlWeight := func() {
		if c.maxWeight == 0 {
			return
		}

		for c.curWeight > c.maxWeight {
			if ele := c.ll.Back(); ele != nil {
				c.removeElement(ele, true)
			} else {
				panic("shouldn't happen")
			}
		}
		// Since weight <= c.maxWeight, we will always reach here without problems
	}

	if ee, ok := c.cache[key]; ok {
		e := ee.Value.(*lruEntry)
		c.ll.MoveToFront(ee)
		diff := weight - e.weight
		e.weight = weight
		e.value = value
		e.hits++

		c.curWeight += diff
		controlWeight()
		return nil
	}

	c.curWeight += weight
	ele := c.ll.PushFront(&lruEntry{key, value, 1, weight})
	c.cache[key] = ele
	controlWeight()

	if c.curWeight < 0 {
		panic("too many entries, really?")
	}

	return nil
}

// Get gets a key
func (c *LRUCache) Get(key LRUKey) (value interface{}, ok bool) {
	c.Lock()
	defer c.Unlock()

	if ele, hit := c.cache[key]; hit {
		e := ele.Value.(*lruEntry)
		e.hits++
		c.ll.MoveToFront(ele)
		return e.value, true
	}

	return
}

// GetEx returns the extra info of the given key
func (c *LRUCache) GetEx(key LRUKey) (hits int64, weight int64, ok bool) {
	c.Lock()
	defer c.Unlock()

	if ele, hit := c.cache[key]; hit {
		return ele.Value.(*lruEntry).hits, ele.Value.(*lruEntry).weight, true
	}

	return
}

// Remove removes the given key from the cache.
func (c *LRUCache) Remove(key LRUKey) {
	c.Lock()
	c.remove(key, true)
	c.Unlock()
}

// RemoveSlient removes the given key without triggering OnEvicted
func (c *LRUCache) RemoveSlient(key LRUKey) {
	c.Lock()
	c.remove(key, false)
	c.Unlock()
}

// Len returns the number of items in the cache.
func (c *LRUCache) Len() (len int) {
	c.Lock()
	len = c.ll.Len()
	c.Unlock()
	return
}

// MaxWeight returns max weight
func (c *LRUCache) MaxWeight() int64 {
	return c.maxWeight
}

// Weight returns current weight
func (c *LRUCache) Weight() int64 {
	return c.curWeight
}

func (c *LRUCache) remove(key LRUKey, doCallback bool) {
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele, doCallback)
	}
}

func (c *LRUCache) removeElement(e *list.Element, doCallback bool) {
	kv := e.Value.(*lruEntry)

	if c.OnEvicted != nil && doCallback {
		c.OnEvicted(kv.key, kv.value)
	}

	c.ll.Remove(e)
	c.curWeight -= e.Value.(*lruEntry).weight
	delete(c.cache, kv.key)
}
