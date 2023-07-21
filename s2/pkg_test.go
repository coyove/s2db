package s2

import (
	"bytes"
	"container/list"
	"encoding/hex"
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

func TestRetention(t *testing.T) {
	fmt.Println(ParseRetentionTable("a:1d,bb:+12,aa:2d"))
}

func TestNZ(t *testing.T) {
	test := func(a []byte) {
		e := NZEncode(nil, a)
		d := NZDecode(nil, e)
		if !bytes.Equal(a, d) {
			t.Fatal(a, d, fmt.Sprintf("%08b", e))
		}
		fmt.Println(a, "->", e)
	}
	test([]byte{0, 1, 2, 3})
	test([]byte{0, 1, 2, 254})
	test([]byte{0, 1, 2, 255})
	test([]byte{0})
	test([]byte{255})
	test([]byte{255, 1})

	rand.Seed(future.UnixNano())
	for i := 0; i < 256; i++ {
		for j := i + 1; j < 256; j++ {
			anz := NZEncode(nil, []byte{byte(i)})
			bnz := NZEncode(nil, []byte{byte(j)})
			if bytes.Compare(anz, bnz) != -1 {
				t.Fatal(i, j, " <=> ", anz, bnz)
			}
		}
	}
	for i := 0; i < 1e6; i++ {
		a := make([]byte, 5+rand.Intn(12))
		b := make([]byte, 5+rand.Intn(12))
		rand.Read(a)
		rand.Read(b)
		anz := NZEncode(nil, a)
		bnz := NZEncode(nil, b)
		less := bytes.Compare(a, b)
		lessnz := bytes.Compare(anz, bnz)
		if less != lessnz {
			t.Fatal(a, b, " <=> ", anz, bnz)
		}
	}
}

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
	var ids = []string{
		"1773d860051d15a71cc92162ef1d1100", "1773d85e5200d6841cc921628e1d1100", "1773d85db70712301cc92162411d1100", "1773d85cda7e776c1cc921626a1d1100", "1773d85cda7d8db31cc921621c1d1100", "1773d859d3a14b931cc921620d1d1100", "1773d859cdab6ac51cc92162951d1100", "1773d854e403e16d1cc92162db1d1100", "1773d852d187be9b1cc921624f1d1100", "1773d852cb91ddca1cc92162da1d1100",
		"1773d84edc4054741cc92162c61d1100", "1773d84e591efec21cc92162a11d1100", "1773d7b035a576d31cc92162b71d1100", "1773d7a0d7bd62381cc92162551d1100", "1773d799bdcc516e1cc92162761d1100", "1773d7952d8c0d511cc92162781d1100", "1773d7867092b47b1cc92162831d1100", "1773d7825788ee6f1cc921622a1d1100", "1773d780c23840631cc92162191d1100", "1773d78050f88dd91cc92162451d1100",
		"1773d77d79cb53231cc92162d11d1100", "1773d77d73d5723e1cc92162961d1100", "1773d77bea7170891cc921626f1d1100", "1773d77aba759d9a1cc921629e1d1100", "1773d776a16aed721cc92162b51d1100", "1773d7727089a3701cc92162a91d1100", "1773d7707bdae5fd1cc92162d71d1100", "1773d77075e5ee671cc92162a51d1100", "1773d76fab3d223e1cc92162881d1100", "1773d76e872d11421cc92162061d1100",
		"1773d765c60a99ad1cc92162961d1100", "1773d75f2f3adf051cc921625a1d1100", "1773d75f2f3ade691cc92162fe1d1100", "1773d75a09f7a1dd1cc921629c1d1100", "1773d7595131628c1cc92162971d1100", "1773d757eb90a6911cc92162c91d1100", "1773d757e59ac5f31cc92162d31d1100", "1773d7562691da411cc92162421d1100", "1773d756209bf9fb1cc92162361d1100", "1773d74e59d06b2a1cc92162cc1d1100",
		"1773d74d35c05ad11cc92162d51d1100", "1773d745c2671a881cc92162f41d1100", "1773d7428bdbd0411cc921620b1d1100", "1773d7428bdbd0361cc92162ce1d1100", "1773d741859a0e771cc92162ba1d1100", "1773d73f13bfdbb71cc921626f1d1100", "1773d73ee410d3551cc92162151d1100", "1773d73d669893ad1cc92162271d1100", "1773d73c664bc8ba1cc92162a41d1100", "1773d73b95ae056e1cc921628f1d1100",
		"1773d73b1e7872141cc92162451d1100", "1773d73b1e7871631cc92162341d1100", "1773d739becd96ca1cc92162391d1100", "1773d73911f319411cc92162cf1d1100", "1773d737888f17f31cc92162f41d1100", "1773d736942e0e391cc921625b1d1100", "1773d735ff2b15b91cc921627e1d1100", "1773d735ff2b15911cc92162f51d1100", "1773d734b161dd4c1cc92162521d1100", "1773d734ab6ce66a1cc92162401d1100",
		"1773d73475c713511cc92162e41d1100", "1773d73086758b081cc921622e1d1100", "1773d72eb58afce31cc92162df1d1100", "1773d72e6218ae741cc92162e41d1100", "1773d72c559357011cc92162541d1100", "1773d72bba9b66631cc921623d1d1100", "1773d72b7effb2cb1cc92162561d1100", "1773d7291913156d1cc92162f11d1100", "1773d7290d2669721cc92162711d1100", "1773d729013aa7361cc921621a1d1100",
		"1773d728f54ee6251cc92162961d1100", "1773d728604bec781cc92162fb1d1100", "1773d728309ce4321cc921623d1d1100", "1773d72800eddcb31cc921624a1d1100", "1773d7273c3bdb651cc92162ea1d1100", "1773d7273c3bdb5f1cc92162861d1100", "1773d726e2d3aca41cc921622c1d1100", "1773d7259b0055471cc92162941d1100", "1773d7250bf33d541cc92162c41d1100", "1773d7245922df851cc92162c91d1100",
		"1773d724119c53321cc921620b1d1100", "1773d723b83424db1cc92162ea1d1100", "1773d723ac48628a1cc92162f11d1100", "1773d7230563c6c21cc92162f21d1100", "1773d7225e7f2a661cc92162df1d1100", "1773d721c97c31431cc92162f51d1100", "1773d72075bd18c91cc92162d61d1100", "1773d6d8602400471cc921628d1d1100", "1773c3fd845430a71cc92162b31d1100", "1773ba53bce6283d1cc92162141d1100",
		"177373e63daa6f631cc92162b31d1100", "17736e4121845c7e1cc92162441d1100", "17736e4044fad78c1cc92162b51d1100", "17736e3e97d38f961cc92162341d1100", "17736e3b7f15aa871cc92162b01d1100", "17736e3afbf53e721cc921627a1d1100", "177343deaa04f50f1cc92162211d1100", "1772f31ce2dff8681cc92162a4001100", "1772ce34d9ff846b1cc9216210001100", "1773d865fafe159b1cc921628c1d1100"}
	var a []Pair
	for _, id := range ids {
		id, _ := hex.DecodeString(id)
		a = append(a, Pair{ID: id})
	}
	x := PackIDs(a)
	fmt.Println(len(x), len(ids)*8)

	y := UnpackIDs(x)
	for _, p := range a {
		if !y(p.ID) {
			t.Fatal(p.ID)
		}
	}
}
