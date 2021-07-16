package main

import (
	"bytes"
	"container/heap"
	"sync"
	"time"

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
			tx.ForEach(func(name []byte, bk *bbolt.Bucket) error {
				if bytes.HasPrefix(name, []byte("zset.score.")) {
					return nil
				}
				if bytes.HasPrefix(name, []byte("zset.")) {
					heap.Push(h, Pair{Key: string(name[5:]), Score: float64(bk.Stats().KeyN)})
					if h.Len() > n {
						heap.Pop(h)
					}
					return nil
				}
				return nil
			})
			return nil
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

type Survey struct {
	mu   sync.Mutex
	max  int
	data [][2]float64
}

func (s *Survey) Append(v float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = append(s.data, [2]float64{v, float64(time.Now().Unix())})
	if len(s.data) > s.max {
		s.data = s.data[len(s.data)-s.max:]
	}
}

func (s *Survey) QPS() (data [][2]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) < 2 {
		return
	}

	x := s.data

	t := x[0][1]
	sum := x[0][0]

	for x = x[1:]; len(x) > 0; x = x[1:] {
		if x[0][1]-t >= 1 || len(x) == 1 {
			data = append(data, [2]float64{sum / (x[0][1] - t), t})
			t = x[0][1]
			sum = x[0][0]
		} else {
			sum += x[0][0]
		}
	}
	return
}

func (s *Survey) MinuteAvg() (data [][2]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) < 1 {
		return
	}

	x := s.data

	t := x[0][1]
	sum := x[0][0]
	count := 1.0

	for x = x[1:]; len(x) > 0; x = x[1:] {
		if x[0][1]-t >= 60 || len(x) == 1 {
			data = append(data, [2]float64{sum / count, t})
			t = x[0][1]
			count = 1
			sum = x[0][0]
		} else {
			sum += x[0][0]
		}
	}
	return
}
