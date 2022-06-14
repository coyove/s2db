package s2pkg

import (
	"sync"
)

type WeakCacheItem struct {
	Time int64
	Data interface{}
}

type lruValue struct {
	// If has slaves, then it's a master
	slaves map[string]struct{}

	// Otherwise it's a slave
	master     string
	slaveStore interface{}
}

type LRUKeyValue struct {
	MasterKey string
	SlaveKey  string
	Value     interface{}
}

type MasterLRU struct {
	mu        sync.RWMutex
	onEvict   func(LRUKeyValue)
	lruCap    int64
	lenMax    int64
	hot, cold map[string]lruValue
	mwm       [65536]int64
}

func NewMasterLRU(cap int64, onEvict func(LRUKeyValue)) *MasterLRU {
	if cap <= 0 {
		cap = 1
	}
	if onEvict == nil {
		onEvict = func(LRUKeyValue) {}
	}
	return &MasterLRU{
		onEvict: onEvict,
		lruCap:  cap,
		hot:     map[string]lruValue{},
		cold:    map[string]lruValue{},
	}
}

func (m *MasterLRU) Len() int { return len(m.hot) + len(m.cold) }

func (m *MasterLRU) Cap() int { return int(m.lenMax) }

func (m *MasterLRU) SetNewCap(cap int64) {
	m.mu.Lock()
	m.lruCap = cap
	m.mu.Unlock()
}

func (m *MasterLRU) GetWatermark(key string) int64 {
	return m.mwm[HashStr(key)%uint64(len(m.mwm))]
}

func (m *MasterLRU) GetWatermarks(keys []string) []int64 {
	res := make([]int64, len(keys))
	for i, k := range keys {
		res[i] = m.GetWatermark(k)
	}
	return res
}

func (m *MasterLRU) Add(masterKey, slaveKey string, value interface{}, masterWatermark int64) bool {
	if slaveKey == "" {
		panic("slave key can't be empty")
	}
	if masterKey == slaveKey {
		panic("master and slave key can't be identical")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if masterWatermark != 0 && masterWatermark < m.GetWatermark(masterKey) {
		return false
	}
	if masterKey == "" {
		m.hot[slaveKey] = lruValue{slaveStore: value}
		delete(m.cold, slaveKey)
	} else {
		master := m.getByKey(masterKey)
		if master.slaves != nil {
			master.slaves[slaveKey] = struct{}{}
		} else {
			master.slaves = map[string]struct{}{slaveKey: {}}
		}
		m.hot[masterKey] = master
		m.hot[slaveKey] = lruValue{master: masterKey, slaveStore: value}
		delete(m.cold, masterKey)
		delete(m.cold, slaveKey)
	}

	sz := int64(m.Len())
	if sz > m.lenMax {
		m.lenMax = sz
	}
	if sz > m.lruCap && len(m.hot) > len(m.cold)*2/3 {
		for k, v := range m.cold {
			m.delete(k, v, true, true)
			if int64(m.Len()) <= m.lruCap {
				break
			}
		}
		if len(m.cold) == 0 {
			m.hot, m.cold = m.cold, m.hot
		}
	}
	return true
}

func (m *MasterLRU) Get(key string) (interface{}, bool) {
	e, ok := m.find(key)
	return e.slaveStore, ok
}

func (m *MasterLRU) find(k string) (lruValue, bool) {
	m.mu.RLock()
	v, ok := m.hot[k]
	m.mu.RUnlock()
	if ok {
		return v, true
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	v, ok = m.cold[k]
	if ok {
		m.hot[k] = v
		delete(m.cold, k)
		return v, true
	}
	return lruValue{}, false
}

func (m *MasterLRU) getByKey(key string) lruValue {
	x, ok := m.hot[key]
	if !ok {
		x = m.cold[key]
	}
	return x
}

func (m *MasterLRU) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hot = map[string]lruValue{}
	m.cold = map[string]lruValue{}
	m.lenMax = 0
}

func (m *MasterLRU) Delete(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mwm[HashStr(key)%uint64(len(m.mwm))]++
	return m.delete(key, lruValue{}, false, false)
}

func (m *MasterLRU) delete(key string, value lruValue, valueProvided, emit bool) int {
	cnt := 0
	if !valueProvided {
		value = m.getByKey(key)
	}
	if value.slaves != nil {
		// To delete a master key, delete all slaves of it
		for slave := range value.slaves {
			cnt++
			if emit {
				m.onEvict(LRUKeyValue{key, slave, m.getByKey(slave).slaveStore})
			}
			delete(m.hot, slave)
			delete(m.cold, slave)
		}
	} else {
		// To delete a slave key, remove it from its master's records
		if value.master != "" {
			master := m.getByKey(value.master)
			if master.slaves == nil {
				panic("missing master on deletion")
			}
			delete(master.slaves, key)
		}
		if emit {
			m.onEvict(LRUKeyValue{value.master, key, value.slaveStore})
		}
	}
	delete(m.hot, key)
	delete(m.cold, key)
	return 1 + cnt
}

func (m *MasterLRU) Range(f func(LRUKeyValue) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, m := range []map[string]lruValue{m.hot, m.cold} {
		for k, v := range m {
			if v.slaves != nil {
				continue
			}
			if !f(LRUKeyValue{v.master, k, v.slaveStore}) {
				return
			}
		}
	}
}

func (m *MasterLRU) RangeMaster(f func(string, map[string]struct{}) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, m := range []map[string]lruValue{m.hot, m.cold} {
		for k, v := range m {
			if v.slaves == nil {
				continue
			}
			if !f(k, v.slaves) {
				return
			}
		}
	}
}
