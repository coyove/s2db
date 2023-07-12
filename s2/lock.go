package s2

import (
	"sync"
	"sync/atomic"
)

const lockShards = 32

type KeyLock struct {
	m [lockShards]sync.Map
	c [lockShards]atomic.Int64
}

func (kl *KeyLock) Lock(key string) bool {
	i := HashStr(key) % lockShards
	_, loaded := kl.m[i].LoadOrStore(key, 1)
	if !loaded {
		kl.c[i].Add(1)
		return true
	}
	return false
}

func (kl *KeyLock) Unlock(key string) {
	i := HashStr(key) % lockShards
	_, loaded := kl.m[i].LoadAndDelete(key)
	if !loaded {
		panic("unlock non-existed key: shouldn't happen")
	}
	kl.c[i].Add(-1)
}

func (kl *KeyLock) Count() (c int64) {
	for i := range kl.c {
		c += kl.c[i].Load()
	}
	return
}
