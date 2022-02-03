package fts

import (
	"container/heap"
	"math"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/nj/bas"
)

type silo struct {
	bitmap      *roaring.Bitmap
	cardinality int
}

type Indexer struct {
	mu          sync.RWMutex
	revert      map[uint32]silo
	root        map[uint32]Document
	totalTokens int
}

func hash(t bas.Value) uint32 {
	return uint32(t.HashCode())
	// return uint32(s2pkg.HashStr(t))
}

func New() *Indexer {
	return &Indexer{
		root:   map[uint32]Document{},
		revert: map[uint32]silo{},
	}
}

func (idx *Indexer) Add(id uint32, content string) {
	doc := Split(content)
	if !doc.Valid() {
		return
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if oldDoc, ok := idx.root[id]; ok {
		for _, seg := range oldDoc.Tokens {
			idx.revert[hash(seg.Token)].bitmap.Remove(id)
			idx.totalTokens--
		}
	}
	for _, seg := range doc.Tokens {
		h := hash(seg.Token)
		s, ok := idx.revert[h]
		if !ok {
			s.bitmap = roaring.BitmapOf(id)
			s.cardinality = 1
		} else {
			if !s.bitmap.CheckedAdd(id) {
				s.cardinality++
			}
		}
		idx.revert[h] = s
		idx.totalTokens++
	}
	idx.root[id] = doc
}

func (idx *Indexer) Remove(id uint32) (Document, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	oldDoc, ok := idx.root[id]
	if !ok {
		return oldDoc, false
	}
	delete(idx.root, id)
	for _, seg := range oldDoc.Tokens {
		h := hash(seg.Token)
		s := idx.revert[h]
		s.bitmap.Remove(id)
		s.cardinality--
		idx.totalTokens--
		idx.revert[h] = s
	}
	return oldDoc, true
}

type Result struct {
	ID    uint32
	Score float32
	Document
}

func (idx *Indexer) TopN(bm25 bool, n int, query ...string) (ids []Result) {
	if len(query) == 0 {
		return
	}

	var h resultHeap
	h.rev = true
	for _, q := range query {
		idx.search(&h, q, bm25, n)
	}
	return h.ToArray()
}

func (idx *Indexer) search(h *resultHeap, content string, bm25 bool, n int) {
	q := Split(content)
	if !q.Valid() {
		return
	}
	if n <= 0 {
		n = 20
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var first *roaring.Bitmap
	for _, seg := range q.Tokens {
		second, ok := idx.revert[hash(seg.Token)]
		if !ok {
			return
		}
		if first == nil {
			first = second.bitmap.Clone()
		} else {
			first.And(second.bitmap)
		}
	}

	if first != nil {
		idfs := make([]float64, len(q.Tokens))
		for i, s := range q.Tokens {
			idfs[i] = math.Log2(float64(len(idx.root)) / float64(idx.revert[hash(s.Token)].cardinality+1))
		}
		avgDocLength := idx.AvgDocNumTokens()
		first.Iterate(func(id uint32) bool {
			sum := 0.0
			doc := idx.root[id]
			for i, s := range q.Tokens {
				frequency := doc.TermFreq(s.Token)
				if bm25 {
					k1 := 2.0
					b := 0.75
					d := float64(doc.NumTokens)
					sum += idfs[i] * frequency * (k1 + 1) / (frequency + k1*(1-b+b*d/avgDocLength))
				} else {
					sum += frequency * idfs[i]
				}
			}
			heap.Push(h, Result{id, float32(sum), doc})
			if h.Len() > n {
				heap.Pop(h)
			}
			return true
		})
	}
}

func (idx *Indexer) SizeBytes() (total int) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for _, b := range idx.revert {
		total += int(b.bitmap.GetSizeInBytes()) + 4
	}
	for _, doc := range idx.root {
		total += doc.SizeBytes() + 4
	}
	return
}

func (idx *Indexer) Cardinality() (total int) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.root)
}

func (idx *Indexer) AvgDocNumTokens() float64 {
	return float64(idx.totalTokens) / float64(len(idx.root)+1)
}

type resultHeap struct {
	rev bool
	p   []Result
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
	h.p = append(h.p, x.(Result))
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
