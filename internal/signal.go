package internal

import (
	"sync"
	"unsafe"
)

type Locker struct {
	mu sync.Mutex
}

func (l *Locker) Lock() {
	l.mu.Lock()
}

func (l *Locker) Unlock() {
	l.mu.Unlock()
}

func (l *Locker) Wait(waiting func()) {
	if *(*int32)(unsafe.Pointer(l)) == 0 {
		return
	}
	l.mu.Lock()
	if waiting != nil {
		waiting()
	}
	l.mu.Unlock()
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
