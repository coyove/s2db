package fts

import (
	"container/heap"
	"math"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/s2pkg"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

const (
	// Reverse index: max number of recorded douments under a single token
	// The final upper bound is calculated as: max(MaxTokenDocIDs, TotalDocumentsInIndex / 16)
	MaxTokenDocIDs = 65536
)

type Indexer struct {
	// mu sync.RWMutex
	// ri             map[uint32]RevertedIndex
	// docs           map[uint32]Document
	// totalDocTokens int
	name string
	db   *bbolt.DB
}

func New(name, path string) *Indexer {
	db, _ := bbolt.Open(path, 0666, nil)
	return &Indexer{
		db: db,
	}
}

func (idx *Indexer) delDocRI(ri *bbolt.Bucket, token bas.Value, id uint32) bool {
	h := token.HashCode()
	b := ri.Get(s2pkg.Uint64ToBytes(h))
	if len(b) == 0 {
		return false
	}
	var i RevertedIndex
	s2pkg.PanicErr(i.Unmarshal(b))
	deleted := i.Bitmap.CheckedRemove(id)
	if deleted {
		i.Cardinality--
	}
	s2pkg.PanicErr(ri.Put(s2pkg.Uint64ToBytes(h), i.Marshal()))
	return deleted
}

func (idx *Indexer) addDocRI(ri *bbolt.Bucket, token bas.Value, id uint32, numDocs int) {
	h := token.HashCode()
	var i RevertedIndex

	b := ri.Get(s2pkg.Uint64ToBytes(h))
	if len(b) == 0 {
		i.Bitmap = roaring.BitmapOf(id)
		i.Cardinality = 1
	} else {
		s2pkg.PanicErr(i.Unmarshal(b))
		if i.Bitmap.CheckedAdd(id) {
			i.Cardinality++
			// Too many documents under this token, remove oldest one
			if float64(i.Cardinality) > math.Max(float64(numDocs)/16, MaxTokenDocIDs) {
				i.Bitmap.Remove(i.Bitmap.Minimum())
				i.Cardinality--
			}
		}
	}
	s2pkg.PanicErr(ri.Put(s2pkg.Uint64ToBytes(h), i.Marshal()))
}

func (idx *Indexer) getBucket(tx *bbolt.Tx, readonly bool) (*bbolt.Bucket, *bbolt.Bucket) {
	if readonly {
		return tx.Bucket([]byte("index." + idx.name)), tx.Bucket([]byte("index.ri." + idx.name))
	}
	bk1, err := tx.CreateBucketIfNotExists([]byte("index." + idx.name))
	s2pkg.PanicErr(err)
	bk2, err := tx.CreateBucketIfNotExists([]byte("index.ri." + idx.name))
	s2pkg.PanicErr(err)
	return bk1, bk2
}

func (idx *Indexer) getCounter(g *bbolt.Bucket) (int, int) {
	b := g.Get([]byte("_total_tokens"))
	c := g.Get([]byte("_total_docs"))
	return int(s2pkg.BytesToUint64(b)), int(s2pkg.BytesToUint64(c))
}

func (idx *Indexer) setCounter(g *bbolt.Bucket, v, v2 int) {
	g.Put([]byte("_total_tokens"), s2pkg.Uint64ToBytes(uint64(v)))
	g.Put([]byte("_total_docs"), s2pkg.Uint64ToBytes(uint64(v2)))
}

func (idx *Indexer) getDoc(g *bbolt.Bucket, id uint32) (d Document, found bool) {
	b := g.Get(s2pkg.Uint64ToBytes(uint64(id)))
	if len(b) == 0 {
		return d, false
	}
	s2pkg.PanicErr(d.Unmarshal(b))
	return d, true
}

func (idx *Indexer) setDoc(g *bbolt.Bucket, id uint32, d Document) {
	s2pkg.PanicErr(g.Put(s2pkg.Uint64ToBytes(uint64(id)), d.Marshal()))
}

func (idx *Indexer) delDoc(g *bbolt.Bucket, id uint32) {
	s2pkg.PanicErr(g.Delete(s2pkg.Uint64ToBytes(uint64(id))))
}

func (idx *Indexer) getRI(ri *bbolt.Bucket, token bas.Value) (i RevertedIndex, found bool) {
	b := ri.Get(s2pkg.Uint64ToBytes(token.HashCode()))
	if len(b) == 0 {
		return i, false
	}
	s2pkg.PanicErr(i.Unmarshal(b))
	return i, true
}

func (idx *Indexer) Index(docs map[uint32]string) int {
	tDocs := make(map[uint32]Document, len(docs))
	for idx, d := range docs {
		doc := Split(d)
		if !doc.Valid() {
			continue
		}
		tDocs[idx] = doc
	}

	s2pkg.PanicErr(idx.db.Update(func(tx *bbolt.Tx) error {
		g, ri := idx.getBucket(tx, false)
		numTokens, numDocs := idx.getCounter(g)

		for id, doc := range tDocs {
			oldDoc, found := idx.getDoc(g, id)
			if found {
				for _, seg := range oldDoc.Tokens {
					if idx.delDocRI(ri, seg.Token, id) {
						numTokens--
					}
				}
				numDocs -= 1
			}
			for _, seg := range doc.Tokens {
				idx.addDocRI(ri, seg.Token, id, numDocs)
			}
			numDocs += 1
			numTokens += len(doc.Tokens)
			idx.setDoc(g, id, doc)
		}
		idx.setCounter(g, numTokens, numDocs)
		return nil
	}))
	return len(tDocs)
}

func (idx *Indexer) Remove(ids []uint32) {
	s2pkg.PanicErr(idx.db.Update(func(tx *bbolt.Tx) error {
		g, ri := idx.getBucket(tx, false)
		numTokens, numDocs := idx.getCounter(g)
		for _, id := range ids {
			oldDoc, found := idx.getDoc(g, id)
			if found {
				for _, seg := range oldDoc.Tokens {
					idx.delDocRI(ri, seg.Token, id)
				}
				numDocs -= 1
				numTokens -= len(oldDoc.Tokens)
				idx.delDoc(g, id)
			}
		}
		idx.setCounter(g, numTokens, numDocs)
		return nil
	}))
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

	idx.db.View(func(tx *bbolt.Tx) error {
		g, ri := idx.getBucket(tx, true)
		if g == nil || ri == nil {
			return nil
		}
		var first *roaring.Bitmap
		var riCardinality = map[uint64]int{}
		for _, s := range q.Tokens {
			second, ok := idx.getRI(ri, s.Token)
			if !ok {
				return nil
			}
			riCardinality[s.Token.HashCode()] = second.Cardinality
			if first == nil {
				first = second.Bitmap.Clone()
			} else {
				first.And(second.Bitmap)
			}
		}

		if first != nil {
			_, numDocs := idx.getCounter(g)
			idfs := make([]float64, len(q.Tokens))
			for i, s := range q.Tokens {
				idfs[i] = math.Log2(float64(numDocs) / float64(riCardinality[s.Token.HashCode()]+1))
			}
			avgDocLength := idx.avgDocNumTokens(g)
			first.Iterate(func(id uint32) bool {
				sum := 0.0
				doc, ok := idx.getDoc(g, id)
				if !ok {
					logrus.Errorf("Indexer: doc %d not found", id)
					return true
				}
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
		return nil
	})
}

func (idx *Indexer) SizeBytes() (total int) {
	return int(idx.db.Size())
}

func (idx *Indexer) NumTokens() (total int) {
	s2pkg.PanicErr(idx.db.View(func(tx *bbolt.Tx) error {
		if g, _ := idx.getBucket(tx, true); g != nil {
			total, _ = idx.getCounter(g)
		}
		return nil
	}))
	return
}

func (idx *Indexer) NumDocuments() (total int) {
	s2pkg.PanicErr(idx.db.View(func(tx *bbolt.Tx) error {
		if g, _ := idx.getBucket(tx, true); g != nil {
			_, total = idx.getCounter(g)
		}
		return nil
	}))
	return
}

func (idx *Indexer) AvgDocNumTokens() (r float64) {
	s2pkg.PanicErr(idx.db.View(func(tx *bbolt.Tx) error {
		g, _ := idx.getBucket(tx, true)
		r = idx.avgDocNumTokens(g)
		return nil
	}))
	return
}

func (idx *Indexer) avgDocNumTokens(g *bbolt.Bucket) (r float64) {
	if g != nil {
		nt, nd := idx.getCounter(g)
		r = float64(nt) / float64(nd+1)
	}
	return r
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
