package s2pkg

import (
	"hash/crc32"
	"math"
	"reflect"
	"sync"

	"github.com/coyove/s2db/clock"
)

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

type LRUShard[T any] struct {
	s [32]*LRUCache[[16]byte, T]
}

func NewLRUShardCache[V any](cap int) *LRUShard[V] {
	l := &LRUShard[V]{}
	for i := range l.s {
		l.s[i] = NewLRUCache[[16]byte, V](cap/len(l.s), nil)
	}
	return l
}

func (l *LRUShard[T]) Add(key []byte, value T) {
	var k [16]byte
	_ = key[15]
	copy(k[:], key)
	l.Add16(k, value)
}

func (l *LRUShard[T]) Add16(k [16]byte, value T) {
	idx := crc32.ChecksumIEEE(k[:]) % 32
	l.s[idx].Add(k, value)
}

func (l *LRUShard[T]) Get(key []byte) (value T, ok bool) {
	var k [16]byte
	_ = key[15]
	copy(k[:], key)
	return l.Get16(k)
}

func (l *LRUShard[T]) Get16(k [16]byte) (value T, ok bool) {
	idx := crc32.ChecksumIEEE(k[:]) % 32
	return l.s[idx].Get(k)
}

func (l *LRUShard[T]) Len() (c int) {
	for i := range l.s {
		c += l.s[i].Len()
	}
	return
}
