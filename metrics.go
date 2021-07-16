package main

import (
	"bytes"
	"container/heap"
	"math"
	"sync/atomic"

	"gitlab.litatom.com/zhangzezhong/zset/nanotime"
	"go.etcd.io/bbolt"
)

type BigKeysHeap []Pair

func (h BigKeysHeap) Len() int           { return len(h) }
func (h BigKeysHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h BigKeysHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *BigKeysHeap) Push(x interface{}) {
	*h = append(*h, x.(Pair))
}

func (h *BigKeysHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (s *Server) BigKeys(n int) (keys []Pair, err error) {
	if n <= 0 {
		n = 10
	}
	h := &BigKeysHeap{}
	heap.Init(h)
	for _, db := range s.db {
		err = db.View(func(tx *bbolt.Tx) error {
			return tx.ForEach(func(name []byte, bk *bbolt.Bucket) error {
				if bytes.HasPrefix(name, []byte("zset.score.")) {
					return nil
				}
				if bytes.HasPrefix(name, []byte("zset.")) {
					heap.Push(h, Pair{Key: string(name[5:]), Score: float64(bk.Stats().KeyN)})
					if h.Len() > n {
						heap.Pop(h)
					}
				}
				return nil
			})
		})
		if err != nil {
			return
		}
	}
	for h.Len() > 0 {
		keys = append(keys, heap.Pop(h).(Pair))
	}
	reversePairs(keys)
	return
}

type ShardSurvey struct {
	Read, Write Survey
}

type Survey struct {
	data [900]int32
}

func (s *Survey) _i() uint64 {
	return (nanotime.Now() / 1e9) % uint64(len(s.data))
}

func (s *Survey) Incr(c int32) {
	atomic.AddInt32(&s.data[s._i()], c)
}

func (s *Survey) QPS() (q1, q5, q15 float64) {
	idx := s._i()
	sec := []int32{}
	for {
		sec = append(sec, s.data[idx])
		idx--
		if idx == math.MaxUint64 {
			idx = uint64(len(s.data) - 1)
		}
		if idx == s._i() {
			break
		}
	}

	sum := 0.0
	for i := 0; i < len(sec); i++ {
		sum += float64(sec[i])
		if i == 59 {
			q1 = sum / 60
		} else if i == 299 {
			q5 = sum / 300
		} else if i == len(sec)-1 {
			q15 = sum / 900
		}
	}
	return
}
