package s2pkg

import (
	"math"
	"reflect"
	"sync"

	"github.com/coyove/s2db/clock"
)

type LRUValue struct {
	Time  int64
	Hash  uint64
	Value interface{}
}

type LRUCache struct {
	mu        sync.RWMutex
	onEvict   func(string, LRUValue)
	storeCap  int
	store     map[string]LRUValue
	storeIter *reflect.MapIter
	watermark [65536]int64
}

func NewLRUCache(cap int, onEvict func(string, LRUValue)) *LRUCache {
	if cap < 2 {
		cap = 2
	}
	if onEvict == nil {
		onEvict = func(string, LRUValue) {}
	}
	c := &LRUCache{
		onEvict:  onEvict,
		storeCap: cap,
		store:    map[string]LRUValue{},
	}
	c.storeIter = reflect.ValueOf(c.store).MapRange()
	return c
}

func (m *LRUCache) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.store)
}

func (m *LRUCache) Cap() int {
	return m.storeCap
}

func (m *LRUCache) GetWatermark(key string) int64 {
	return m.watermark[HashStr(key)%uint64(len(m.watermark))]
}

func (m *LRUCache) AddSimple(key string, value interface{}) {
	m.Add(key, 0, value, 0)
}

func (m *LRUCache) Add(key string, hash uint64, value interface{}, wm int64) bool {
	if key == "" {
		panic("key can't be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if wm != 0 && wm < m.GetWatermark(key) {
		return false
	}
	m.store[key] = LRUValue{
		Time:  clock.UnixNano(),
		Hash:  hash,
		Value: value,
	}

	for len(m.store) > m.storeCap {
		k0, v0 := "", LRUValue{Time: math.MaxInt64}
		for i := 0; i < 2; i++ {
			k, v := m.advance()
			if v.Time < v0.Time {
				k0, v0 = k, v
			}
		}
		delete(m.store, k0)
		m.onEvict(k0, v0)
	}
	return true
}

func (m *LRUCache) advance() (string, LRUValue) {
	if !m.storeIter.Next() {
		m.storeIter = reflect.ValueOf(m.store).MapRange()
		m.storeIter.Next()
	}
	k := m.storeIter.Key().Interface().(string)
	v := m.storeIter.Value().Interface().(LRUValue)
	return k, v
}

func (m *LRUCache) GetSimple(k string) (interface{}, bool) {
	v, ok := m.Get(k, 0)
	return v, ok
}

func (m *LRUCache) Get(k string, hash uint64) (interface{}, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.store[k]
	if ok {
		if v.Hash != hash {
			return nil, false
		}
		v.Time = clock.UnixNano()
		m.store[k] = v
	}
	return v.Value, ok
}

func (m *LRUCache) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store = map[string]LRUValue{}
}

func (m *LRUCache) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.watermark[HashStr(key)%uint64(len(m.watermark))]++
	old, ok := m.store[key]
	delete(m.store, key)
	if ok {
		m.onEvict(key, old)
	}
}

func (m *LRUCache) Range(f func(string, LRUValue) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.store {
		if !f(k, v) {
			return
		}
	}
}
