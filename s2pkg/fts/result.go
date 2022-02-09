package fts

import (
	"container/heap"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/s2db/s2pkg"
	"github.com/golang/protobuf/proto"
)

type Result struct {
	ID    uint32
	Score float32
	Document
}

type resultHeap struct {
	rev   bool
	dedup *roaring.Bitmap
	p     []Result
}

func (h resultHeap) Len() int {
	return len(h.p)
}

func (h resultHeap) Less(i, j int) bool {
	if h.rev {
		return h.p[i].Score > h.p[j].Score
	}
	return h.p[i].Score < h.p[j].Score
}

func (h resultHeap) Swap(i, j int) {
	h.p[i], h.p[j] = h.p[j], h.p[i]
}

func (h *resultHeap) Push(x interface{}) {
	if h.dedup == nil {
		h.dedup = roaring.New()
	}
	r := x.(Result)
	if h.dedup.Contains(r.ID) {
		return
	}
	h.dedup.Add(r.ID)
	h.p = append(h.p, r)
}

func (h *resultHeap) Pop() interface{} {
	old := h.p
	n := len(old)
	x := old[n-1]
	h.p = old[0 : n-1]
	return x
}

func (h *resultHeap) ToArray() (ids []Result) {
	for h.Len() > 0 {
		ids = append(ids, heap.Pop(h).(Result))
	}
	return ids
}

func pmarshal(v proto.Message) []byte {
	buf, err := proto.Marshal(v)
	s2pkg.PanicErr(err)
	return buf
}
