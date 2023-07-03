package s2

import (
	"bytes"
	"container/list"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coyove/sdss/future"
)

func TestThrottler(t *testing.T) {
	e := ErrorThrottler{}
	wg := sync.WaitGroup{}
	for i := 0; i < 1e3; i++ {
		wg.Add(1)
		go func() {
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(1200)+400))
			if !e.Throttle("test", syscall.ECONNREFUSED) {
				fmt.Println("not throttled")
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestHashStr(t *testing.T) {
	fmt.Println(HashStr128("a"))
	fmt.Println(HashStr128("b"))
}

func TestHLL(t *testing.T) {
	a := NewHyperLogLog()
	var b HyperLogLog
	m := map[uint32]int{}
	rand.Seed(future.UnixNano())
	for i := 0; i < 1e7; i++ {
		x := rand.Uint32()
		a.Add(x)
		m[x] = 1
	}

	for i := 0; i < 1e6; i++ {
		x := rand.Uint32()
		b.Add(x)
		m[x] = 1
	}
	fmt.Println(len(m), a.Count(), b.Count())
	a.Merge(b)
	fmt.Println(a.Count())
}

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

type onebytereader struct{ io.Reader }

func (obr onebytereader) Read(p []byte) (int, error) { return obr.Reader.Read(p[:1]) }

func TestCrc32Reader(t *testing.T) {
	rand.Seed(time.Now().Unix())
	gen := func(n int) ([]byte, io.Reader) {
		buf := make([]byte, n)
		rand.Read(buf)
		h := crc32.NewIEEE()
		h.Write(buf)
		x := buf
		buf = h.Sum(buf)
		return x, bytes.NewReader(buf)
	}

	for i := 0; i < 1e5; i++ {
		n := rand.Intn(10)
		buf, r := gen(n)
		buf2 := &bytes.Buffer{}

		if rand.Intn(2) == 0 {
			r = onebytereader{r}
		}

		_, ok, err := CopyCrc32(buf2, r, nil)
		if !ok || err != nil || !bytes.Equal(buf, buf2.Bytes()) {
			t.Fatal(buf, buf2.Bytes(), err)
		}
	}
}

func TestDoubleMapLRU(t *testing.T) {
	type op struct {
		add bool
		v   int
	}
	ops := []op{}

	const N = 1024
	m := NewLRUCache[string, int](N, nil)
	r := NewLRUCacheTest(N)

	for i := 0; i < 1e6; i++ {
		ops = append(ops, op{true, i})
		if i > 10 && rand.Intn(2) == 0 {
			cnt := rand.Intn(60) + 20
			for z := 0; z < cnt; z++ {
				x := rand.Intn(i)
				ops = append(ops, op{false, x})
			}
		}
	}

	wg := sync.WaitGroup{}
	start := time.Now()
	for _, o := range ops {
		wg.Add(1)
		f := func(op op) {
			if op.add {
				m.Add(strconv.Itoa(op.v), op.v)
			} else {
				m.Get(strconv.Itoa(op.v))
			}
			wg.Done()
		}
		f(o)
	}
	wg.Wait()
	hcDiff := time.Since(start)

	start = time.Now()
	for _, o := range ops {
		wg.Add(1)
		f := func(op op) {
			if op.add {
				r.Add(strconv.Itoa(op.v), op.v)
			} else {
				r.Get(strconv.Itoa(op.v))
			}
			wg.Done()
		}
		f(o)
	}
	wg.Wait()
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

	m = NewLRUCache(1, func(k string, v int) {
		fmt.Println("evict", k, v) // kv.SlaveKey)
	})
	for i := 0; i < 7; i++ {
		m.Add(strconv.Itoa(i), i)
	}
	m.Range(func(k string, v int) bool {
		fmt.Println(k, v)
		return true
	})
}

type LRUCacheTest struct {
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

// NewLRUCacheTest creates a new Cache.
func NewLRUCacheTest(maxWeight int64) *LRUCacheTest {
	return &LRUCacheTest{
		maxWeight: maxWeight,
		ll:        list.New(),
		cache:     make(map[interface{}]*list.Element),
	}
}

// Clear clears the cache
func (c *LRUCacheTest) Clear() {
	c.Lock()
	c.ll = list.New()
	c.cache = make(map[interface{}]*list.Element)
	c.Unlock()
}

// Info iterates the cache
func (c *LRUCacheTest) Info(callback func(LRUKey, interface{}, int64, int64)) {
	c.Lock()

	for f := c.ll.Front(); f != nil; f = f.Next() {
		e := f.Value.(*lruEntry)
		callback(e.key, e.value, e.hits, e.weight)
	}

	c.Unlock()
}

// Add adds a value to the cache with weight = 1.
func (c *LRUCacheTest) Add(key LRUKey, value interface{}) error {
	return c.AddWeight(key, value, 1)
}

// AddWeight adds a value to the cache with weight.
func (c *LRUCacheTest) AddWeight(key LRUKey, value interface{}, weight int64) error {
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
func (c *LRUCacheTest) Get(key LRUKey) (value interface{}, ok bool) {
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
func (c *LRUCacheTest) GetEx(key LRUKey) (hits int64, weight int64, ok bool) {
	c.Lock()
	defer c.Unlock()

	if ele, hit := c.cache[key]; hit {
		return ele.Value.(*lruEntry).hits, ele.Value.(*lruEntry).weight, true
	}

	return
}

// Remove removes the given key from the cache.
func (c *LRUCacheTest) Remove(key LRUKey) {
	c.Lock()
	c.remove(key, true)
	c.Unlock()
}

// RemoveSlient removes the given key without triggering OnEvicted
func (c *LRUCacheTest) RemoveSlient(key LRUKey) {
	c.Lock()
	c.remove(key, false)
	c.Unlock()
}

// Len returns the number of items in the cache.
func (c *LRUCacheTest) Len() (len int) {
	c.Lock()
	len = c.ll.Len()
	c.Unlock()
	return
}

// MaxWeight returns max weight
func (c *LRUCacheTest) MaxWeight() int64 {
	return c.maxWeight
}

// Weight returns current weight
func (c *LRUCacheTest) Weight() int64 {
	return c.curWeight
}

func (c *LRUCacheTest) remove(key LRUKey, doCallback bool) {
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele, doCallback)
	}
}

func (c *LRUCacheTest) removeElement(e *list.Element, doCallback bool) {
	kv := e.Value.(*lruEntry)

	if c.OnEvicted != nil && doCallback {
		c.OnEvicted(kv.key, kv.value)
	}

	c.ll.Remove(e)
	c.curWeight -= e.Value.(*lruEntry).weight
	delete(c.cache, kv.key)
}

func TestBitsMask(t *testing.T) {
	if v := fmt.Sprintf("%b", BitsMask(63, 0)); v != "-1" {
		t.Fatal(v)
	}
	if v := fmt.Sprintf("%b", BitsMask(63, 1)); v != "-10" {
		t.Fatal(v)
	}
	for i := 0; i < 10; i++ {
		if v := fmt.Sprintf("%b", BitsMask(62, int64(i))); v != strings.Repeat("1", 63-i)+strings.Repeat("0", i) {
			t.Fatal(v)
		}
	}
}

func TestLRUPerf(t *testing.T) {
	const N = 3000000
	const C = 1e4
	m := NewLRUCache[string, int](N, nil)
	for i := 0; i < N+C; i++ {
		m.Add(strconv.Itoa(i), 0)
	}

	start := time.Now()
	for i := 0; i < C; i++ {
		m.Add(strconv.Itoa(i), 0)
	}
	t.Log(time.Since(start))

	m2 := NewLRUCacheTest(N)
	for i := 0; i < N+C; i++ {
		m2.Add(i, 0)
	}

	start = time.Now()
	for i := 0; i < C; i++ {
		m2.Add(i, 0)
	}
	t.Log(time.Since(start))
}

func TestLRUMem(t *testing.T) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Println(mem.HeapAlloc)

	l := NewLRUCache[uint64, uint64](1e6, nil)
	for i := 0; i < 1e6; i++ {
		l.Add(uint64(i), uint64(i))
	}
	runtime.GC()
	runtime.ReadMemStats(&mem)
	fmt.Println(mem.HeapAlloc)

	m := map[uint64]uint64{}
	for i := 0; i < 1e6; i++ {
		m[uint64(i)] = uint64(i)
	}
	runtime.GC()
	runtime.ReadMemStats(&mem)
	fmt.Println(mem.HeapAlloc, len(m), l.Len())
}

func TestXor(t *testing.T) {
	rand.Seed(future.UnixNano())
	var a []Pair
	for i := 0; i < 1e2; i++ {
		v := uint64(1666666666+rand.Uint32()%864000)<<32 | uint64(rand.Uint32())
		// v = rand.Uint64()
		a = append(a, Pair{ID: Uint64ToBytes(v)})
	}
	x := KeyHashPack(a)
	fmt.Println(len(x))

	y := KeyHashUnpack(x)
	for _, p := range a {
		if !KeyHashContains(y, p.ID) {
			t.Fatal(y)
		}
		if KeyHashContains(y, IncBytesInplace(p.ID)) {
			t.Fatal(y)
		}
	}
}

func TestCSVPrefix(t *testing.T) {
	a := SortCSVPrefix("")
	fmt.Println(a, SearchCSVPrefix(a, "a"))
	a = SortCSVPrefix("a,1,ab,2")
	fmt.Println(a, SearchCSVPrefix(a, "abc"))
}
