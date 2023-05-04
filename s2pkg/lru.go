package s2pkg

import (
	"math"
	"reflect"
	"sync"

	"github.com/coyove/s2db/clock"
)

// func init() {
// 	t := btree.NewG(256, btree.LessFunc[[2]string](func(a, b [2]string) bool {
// 		return a[0] < b[0]
// 	}))
//
// 	var mem runtime.MemStats
// 	runtime.ReadMemStats(&mem)
// 	fmt.Println(mem.HeapAlloc)
// 	for i := 0; i < 1e6; i++ {
// 		t.ReplaceOrInsert([2]string{
// 			strconv.Itoa(i),
// 			strconv.Itoa(i),
// 		})
// 	}
// 	runtime.ReadMemStats(&mem)
// 	fmt.Println(mem.HeapAlloc)
//
// 	m := map[string]string{}
// 	for i := 0; i < 1e6; i++ {
// 		m[strconv.Itoa(i)] = strconv.Itoa(i)
// 	}
// 	runtime.ReadMemStats(&mem)
// 	fmt.Println(mem.HeapAlloc)
//
// 	l := NewLRUCache[string, string](1e6, nil)
// 	for i := 0; i < 1e6; i++ {
// 		l.Add(strconv.Itoa(i), strconv.Itoa(i))
// 	}
// 	runtime.ReadMemStats(&mem)
// 	fmt.Println(mem.HeapAlloc)
// 	os.Exit(0)
// }

type LRUValue[V any] struct {
	Time  int64
	Value V
}

type LRUCache[K comparable, V any] struct {
	mu        sync.RWMutex
	onEvict   func(K, V)
	storeCap  int
	store     map[K]LRUValue[V]
	storeIter *reflect.MapIter
}

func NewLRUCache[K comparable, V any](cap int, onEvict func(K, V)) *LRUCache[K, V] {
	if cap < 2 {
		cap = 2
	}
	if onEvict == nil {
		onEvict = func(K, V) {}
	}
	c := &LRUCache[K, V]{
		onEvict:  onEvict,
		storeCap: cap,
		store:    map[K]LRUValue[V]{},
	}
	c.storeIter = reflect.ValueOf(c.store).MapRange()
	return c
}

func (m *LRUCache[K, V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.store)
}

func (m *LRUCache[K, V]) Cap() int {
	return m.storeCap
}

func (m *LRUCache[K, V]) Add(key K, value V) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.store[key] = LRUValue[V]{
		Time:  clock.UnixNano(),
		Value: value,
	}

	for len(m.store) > m.storeCap {
		var k0 K
		v0 := LRUValue[V]{Time: math.MaxInt64}
		for i := 0; i < 2; i++ {
			k, v := m.advance()
			if v.Time < v0.Time {
				k0, v0 = k, v
			}
		}
		delete(m.store, k0)
		m.onEvict(k0, v0.Value)
	}
	return true
}

func (m *LRUCache[K, V]) advance() (K, LRUValue[V]) {
	if !m.storeIter.Next() {
		m.storeIter = reflect.ValueOf(m.store).MapRange()
		m.storeIter.Next()
	}
	k := m.storeIter.Key().Interface().(K)
	v := m.storeIter.Value().Interface().(LRUValue[V])
	return k, v
}

func (m *LRUCache[K, V]) Get(k K) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.store[k]
	if ok {
		v.Time = clock.UnixNano()
		m.store[k] = v
	}
	return v.Value, ok
}

func (m *LRUCache[K, V]) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store = map[K]LRUValue[V]{}
}

func (m *LRUCache[K, V]) Delete(key K) V {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.store[key]
	delete(m.store, key)
	if ok {
		m.onEvict(key, old.Value)
	}
	return old.Value
}

func (m *LRUCache[K, V]) Range(f func(K, V) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.store {
		if !f(k, v.Value) {
			return
		}
	}
}
