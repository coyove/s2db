package main

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coyove/common/lru"
)

type weakCacheItem struct {
	Time int64
	Data interface{}
}

type cacheItem struct {
	Key     string
	CmdHash [2]uint64
	Data    interface{}
}

type keyedCache struct {
	maxWeight int64
	curWeight int64
	watermark int64

	ll    *list.List
	cache map[[2]uint64]*list.Element
	keyed map[string][]*list.Element

	sync.RWMutex
}

type entry struct {
	value  *cacheItem
	hits   int64
	weight int64
}

// newKeyedCache creates a new Cache.
func newKeyedCache(maxWeight int64) *keyedCache {
	c := &keyedCache{maxWeight: maxWeight}
	c.Clear()
	return c
}

// Clear clears the cache
func (c *keyedCache) Clear() {
	c.Lock()
	c.ll = list.New()
	c.cache = make(map[[2]uint64]*list.Element)
	c.keyed = make(map[string][]*list.Element)
	c.curWeight = 0
	c.Unlock()
}

func (c *keyedCache) nextWatermark() int64 {
	return atomic.AddInt64(&c.watermark, 1)
}

func (c *keyedCache) Add(value *cacheItem, keyMaxLen int) error {
	weight := int64(1)
	if p, ok := value.Data.([]Pair); ok {
		weight = int64(sizePairs(p))
	}

	if weight > c.maxWeight || weight < 1 {
		return lru.ErrWeightTooBig
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
func (c *keyedCache) Get(h [2]uint64) (value *cacheItem, ok bool) {
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

func (c *keyedCache) Len() (ln int) {
	c.RLock()
	ln = len(c.cache)
	c.RUnlock()
	return ln
}

func (c *keyedCache) KeyInfo(key string) (ln, size, hits int) {
	c.RLock()
	ln = len(c.keyed[key])
	for _, x := range c.keyed[key] {
		e := x.Value.(*entry)
		if d, ok := e.value.Data.([]Pair); ok {
			size += sizePairs(d)
		} else {
			size += 1
		}
		hits += int(e.hits)
	}
	c.RUnlock()
	return
}

func (s *Server) removeCache(key string) {
	c := s.cache
	c.Lock()
	for _, e := range c.keyed[key] {
		c.remove(e, false)
	}
	delete(c.keyed, key)
	s.db[shardIndex(key)].writeWatermark = c.nextWatermark()
	c.Unlock()
}

func (c *keyedCache) remove(e *list.Element, clearKeyed bool) {
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

func (s *Server) canUpdateCache(key string, wm int64) bool {
	return wm >= s.db[shardIndex(key)].writeWatermark
}

func (s *Server) getCache(h [2]uint64) interface{} {
	v, ok := s.cache.Get(h)
	if !ok {
		return nil
	}
	s.survey.cache.Incr(1)
	return v.Data
}

func (s *Server) getWeakCache(h [2]uint64) interface{} {
	v, ok := s.weakCache.Get(h)
	if !ok {
		return nil
	}
	if i := v.(*weakCacheItem); time.Since(time.Unix(i.Time, 0)) <= time.Duration(s.WeakTTL)*time.Second {
		s.survey.weakCache.Incr(1)
		return i.Data
	}
	return nil
}

func (s *Server) addCache(watermark int64, key string, h [2]uint64, data interface{}) error {
	if !s.canUpdateCache(key, watermark) {
		return nil
	}
	return s.cache.Add(&cacheItem{
		Key:     key,
		CmdHash: h,
		Data:    data,
	}, s.ServerConfig.CacheKeyMaxLen)
}
