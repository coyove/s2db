package fts

import (
	"container/heap"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/s2db/s2pkg"
)

type Result struct {
	ID    string
	Score float64
	Document
}

type ResultHeap struct {
	Rev   bool
	dedup *roaring.Bitmap
	p     []Result
}

func (h ResultHeap) Len() int {
	return len(h.p)
}

func (h ResultHeap) Less(i, j int) bool {
	if h.Rev {
		return h.p[i].Score > h.p[j].Score
	}
	return h.p[i].Score < h.p[j].Score
}

func (h ResultHeap) Swap(i, j int) {
	h.p[i], h.p[j] = h.p[j], h.p[i]
}

func (h *ResultHeap) Push(x interface{}) {
	if h.dedup == nil {
		h.dedup = roaring.New()
	}
	r := x.(Result)
	idhash := uint32(s2pkg.HashStr(r.ID))
	if h.dedup.Contains(idhash) {
		return
	}
	h.dedup.Add(idhash)
	h.p = append(h.p, r)
}

func (h *ResultHeap) Pop() interface{} {
	old := h.p
	n := len(old)
	x := old[n-1]
	h.p = old[0 : n-1]
	return x
}

func (h *ResultHeap) ToArray() (res []Result, ids []string) {
	for h.Len() > 0 {
		r := heap.Pop(h).(Result)
		res = append(res, r)
		ids = append(ids, r.ID)
	}
	return res, ids
}
