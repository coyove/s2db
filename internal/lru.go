package internal

import (
	"container/list"
	"fmt"
	"sync"
)

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

type WeakCacheItem struct {
	Time int64
	Data interface{}
}

type KeyedCacheItem struct {
	Key     string
	CmdHash [2]uint64
	Data    interface{}
}

type KeyedLRUCache struct {
	maxWeight int64
	curWeight int64

	ll    *list.List
	cache map[[2]uint64]*list.Element
	keyed map[string][]*list.Element

	sync.RWMutex
}

type entry struct {
	value  *KeyedCacheItem
	hits   int64
	weight int64
}

// NewKeyedLRUCache creates a new Cache.
func NewKeyedLRUCache(maxWeight int64) *KeyedLRUCache {
	c := &KeyedLRUCache{maxWeight: maxWeight}
	c.Clear()
	return c
}

// Clear clears the cache
func (c *KeyedLRUCache) Clear() {
	c.Lock()
	c.ll = list.New()
	c.cache = make(map[[2]uint64]*list.Element)
	c.keyed = make(map[string][]*list.Element)
	c.curWeight = 0
	c.Unlock()
}

func (c *KeyedLRUCache) Weight() int64 {
	return c.curWeight
}

func (c *KeyedLRUCache) Add(key string, h [2]uint64, data interface{}, keyMaxLen int) error {
	value := &KeyedCacheItem{Key: key, CmdHash: h, Data: data}
	weight := int64(1)
	if p, ok := value.Data.([]Pair); ok {
		weight = int64(SizePairs(p))
	}
	if p, ok := value.Data.([][]byte); ok {
		weight = int64(SizeBytes(p))
	}

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
				c.remove(ele, true)
			} else {
				panic("shouldn't happen")
			}
		}
		// Since weight <= c.maxWeight, we will always reach here without problems
	}

	if ee, ok := c.cache[value.CmdHash]; ok {
		e := ee.Value.(*entry)
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
	ele := c.ll.PushFront(&entry{value, 1, weight})
	c.cache[value.CmdHash] = ele
	c.keyed[value.Key] = append(c.keyed[value.Key], ele)
	if ck := c.keyed[value.Key]; len(ck) > keyMaxLen {
		c.remove(ck[0], true)
	}
	controlWeight()

	if c.curWeight < 0 {
		panic("too many entries, really?")
	}

	return nil
}

// Get gets a key
func (c *KeyedLRUCache) Get(h [2]uint64) (value *KeyedCacheItem, ok bool) {
	c.RLock()
	defer c.RUnlock()

	if ele, hit := c.cache[h]; hit {
		e := ele.Value.(*entry)
		e.hits++
		c.ll.MoveToFront(ele)
		// fmt.Println("in cache")
		return e.value, true
	}

	return
}

func (c *KeyedLRUCache) Len() (ln int) {
	c.RLock()
	ln = len(c.cache)
	c.RUnlock()
	return ln
}

func (c *KeyedLRUCache) KeyInfo(key string) (ln, size, hits int) {
	c.RLock()
	ln = len(c.keyed[key])
	for _, x := range c.keyed[key] {
		e := x.Value.(*entry)
		if d, ok := e.value.Data.([]Pair); ok {
			size += SizePairs(d)
		} else {
			size += 1
		}
		hits += int(e.hits)
	}
	c.RUnlock()
	return
}

func (c *KeyedLRUCache) Remove(key string) {
	c.Lock()
	for _, e := range c.keyed[key] {
		c.remove(e, false)
	}
	delete(c.keyed, key)
	c.Unlock()
}

func (c *KeyedLRUCache) remove(e *list.Element, clearKeyed bool) {
	kv := e.Value.(*entry)
	c.ll.Remove(e)
	c.curWeight -= kv.weight

	v := kv.value
	delete(c.cache, v.CmdHash)

	if !clearKeyed {
		return
	}

	for i, k := range c.keyed[v.Key] {
		if k == e {
			tmp := c.keyed[v.Key]
			if len(tmp) == 1 {
				delete(c.keyed, v.Key)
			} else {
				c.keyed[v.Key] = append(tmp[:i], tmp[i+1:]...)
			}
			break
		}
	}
}

func (c *KeyedLRUCache) Info(callback func(*KeyedCacheItem, int64, int64)) {
	c.Lock()
	for f := c.ll.Front(); f != nil; f = f.Next() {
		e := f.Value.(*entry)
		callback(e.value, e.hits, e.weight)
	}
	c.Unlock()
}
