package s2pkg

import (
	"fmt"
	"sync"
	"time"
)

type BuoyTimeoutError struct {
	Value interface{}
}

func (bte *BuoyTimeoutError) Error() string { return "buoy wait timeout" }

type buoyCell struct {
	watermark uint64
	ts        int64
	ch        chan interface{}
	v         interface{}
}

type BuoySignal struct {
	mu      sync.Mutex
	list    []buoyCell
	closed  bool
	metrics *Survey
}

func NewBuoySignal(timeout time.Duration, metrics *Survey) *BuoySignal {
	bs := &BuoySignal{metrics: metrics}
	var watcher func()
	watcher = func() {
		if bs.closed {
			return
		}

		bs.mu.Lock()
		defer func() {
			bs.mu.Unlock()
			time.AfterFunc(timeout/2, watcher)
		}()

		now := time.Now().UnixNano()
		for len(bs.list) > 0 {
			cell := bs.list[0]
			if time.Duration(now-cell.ts) > timeout {
				cell.ch <- &BuoyTimeoutError{Value: cell.v}
				bs.list = bs.list[1:]
			} else {
				break
			}
		}
	}
	time.AfterFunc(timeout/2, watcher)
	return bs
}

func (bs *BuoySignal) Close() {
	bs.closed = true
}

func (bs *BuoySignal) WaitAt(watermark uint64, ch chan interface{}, v interface{}) int {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	c := buoyCell{
		watermark: watermark,
		ch:        ch,
		v:         v,
		ts:        time.Now().UnixNano(),
	}
	if len(bs.list) > 0 {
		last := bs.list[len(bs.list)-1]
		if watermark <= last.watermark {
			panic("buoy watermark error")
		}
	}
	bs.list = append(bs.list, c)
	return len(bs.list)
}

func (bs *BuoySignal) RaiseTo(watermark uint64) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	now := time.Now().UnixNano()
	for len(bs.list) > 0 {
		if cell := bs.list[0]; cell.watermark <= watermark {
			cell.ch <- cell.v
			bs.list = bs.list[1:]
			bs.metrics.Incr((now - cell.ts) / 1e6)
		} else {
			break
		}
	}
}

func (bs *BuoySignal) Len() int {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return len(bs.list)
}

func (bs *BuoySignal) String() string {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if len(bs.list) == 0 {
		return "0-0"
	}
	return fmt.Sprintf("%d-%d", bs.list[0].watermark, bs.list[len(bs.list)-1].watermark)
}
