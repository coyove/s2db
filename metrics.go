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

const SurveyRange = 900

type Survey struct {
	data [SurveyRange]int32
	ts   [SurveyRange]uint32
}

func (s *Survey) _i() (uint64, uint32) {
	ts := (nanotime.Now() / 1e9)
	return ts % SurveyRange, uint32(ts)
}

func (s *Survey) _decr(x uint64) uint64 {
	x--
	if x == math.MaxUint64 {
		return SurveyRange - 1
	}
	return x
}

func (s *Survey) Incr(c int32) {
	idx, ts := s._i()
	s.data[(idx+1)%SurveyRange] = 0
	atomic.AddInt32(&s.data[idx], c)
	s.ts[idx] = ts
}

func (s *Survey) QPS() (q1, q5, q15 float64) {
	idx, ts := s._i()
	sec := []int32{}

	for startIdx := idx; ; {
		if s.ts[idx] >= ts-SurveyRange {
			sec = append(sec, s.data[idx])
		} else {
			sec = append(sec, 0)
		}
		idx = s._decr(idx)
		if idx == startIdx+1 {
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
