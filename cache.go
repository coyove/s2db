package main

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
)

type WeakCacheItem struct {
	Time int64
	Data interface{}
}

type CacheItem struct {
	Key     string
	CmdHash [2]uint64
	Data    interface{}
}

type Cache struct {
	maxWeight int64
	curWeight int64
	watermark int64

	ll    *list.List
	cache map[[2]uint64]*list.Element
	keyed map[string][]*list.Element

	sync.RWMutex
}

type entry struct {
	value  *CacheItem
	hits   int64
	weight int64
}

var ErrWeightTooBig = fmt.Errorf("weight can't be held by the cache")

// NewCache creates a new Cache.
func NewCache(maxWeight int64) *Cache {
	c := &Cache{maxWeight: maxWeight}
	c.Clear()
	return c
}

// Clear clears the cache
func (c *Cache) Clear() {
	c.Lock()
	c.ll = list.New()
	c.cache = make(map[[2]uint64]*list.Element)
	c.keyed = make(map[string][]*list.Element)
	c.curWeight = 0
	c.Unlock()
}

func (c *Cache) nextWatermark() int64 { return atomic.AddInt64(&c.watermark, 1) }

func (c *Cache) Add(value *CacheItem) error {
	weight := int64(1)
	if p, ok := value.Data.([]Pair); ok {
		weight = int64(sizePairs(p))
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
	controlWeight()

	if c.curWeight < 0 {
		panic("too many entries, really?")
	}

	return nil
}

// Get gets a key
func (c *Cache) Get(h [2]uint64) (value *CacheItem, ok bool) {
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

func (c *Cache) CacheLen() int {
	c.RLock()
	ln := len(c.cache)
	c.RUnlock()
	return ln
}

func (c *Cache) KeyCacheLen(key string) int {
	c.RLock()
	ln := len(c.keyed[key])
	c.RUnlock()
	return ln
}

// Remove removes the given key from the cache.
func (c *Cache) Remove(key string, s *Server) {
	c.Lock()
	for _, e := range c.keyed[key] {
		c.remove(e, false)
	}
	delete(c.keyed, key)
	s.db[s.shardIndex(key)].writeWatermark = c.nextWatermark()
	c.Unlock()
}

func (c *Cache) remove(e *list.Element, clearKeyed bool) {
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
