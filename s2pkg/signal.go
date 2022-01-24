package s2pkg

import (
	"sync"
	"unsafe"
)

type Locker struct {
	mu sync.Mutex
}

func (l *Locker) Unlock() {
	l.mu.Unlock()
}

func (l *Locker) Lock(waiting func()) {
	if *(*int32)(unsafe.Pointer(l)) != 0 && waiting != nil {
		waiting()
	}
	l.mu.Lock()
}

type LockBox struct {
	mu sync.Mutex
	v  interface{}
}

func (b *LockBox) Lock(v interface{}) (interface{}, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.v != nil {
		return b.v, false
	}
	b.v = v
	return v, true
}

func (b *LockBox) Unlock() {
	b.mu.Lock()
	b.v = nil
	b.mu.Unlock()
}

func PanicErr(err error) {
	if err != nil {
		panic(err)
	}
}
