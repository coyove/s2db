package fts

import (
	"container/heap"
	"math"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/s2db/s2pkg"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

var (
	// Reverse index: max number of recorded douments under a single token
	// The final upper bound is calculated as: max(MaxTokenDocIDs, TotalDocumentsInIndex / 16)
	MaxTokenDocIDs = 65536
)

type Index struct {
	name   string
	db     *bbolt.DB
	corpus *Corpus
}

type Corpus struct {
	db      *bbolt.DB
	Timeout struct {
		Search time.Duration
	}
}

func Open(path string) (*Corpus, error) {
	db, err := bbolt.Open(path, 0666, &bbolt.Options{FreelistType: bbolt.FreelistMapType})
	if err != nil {
		return nil, err
	}
	return &Corpus{
		db: db,
	}, nil
}

func (crp *Corpus) Open(name string) *Index {
	return &Index{
		corpus: crp,
		db:     crp.db,
		name:   name,
	}
}

func (crp *Corpus) Close() error {
	return crp.db.Close()
}

func (crp *Corpus) SizeBytes() (total int) {
	return int(crp.db.Size())
}

func delDocRI(ri *bbolt.Bucket, token string, id uint32) bool {
	b := ri.Get([]byte(token))
	if len(b) == 0 {
		return false
	}
	var i RevertedIndex
	s2pkg.PanicErr(i.Unmarshal(b))
	deleted := i.Bitmap.CheckedRemove(id)
	if deleted {
		i.Cardinality--
	}
	s2pkg.PanicErr(ri.Put([]byte(token), i.Marshal()))
	return deleted
}

func addDocRI(ri *bbolt.Bucket, token string, id uint32, numDocs int) {
	var i RevertedIndex

	b := ri.Get([]byte(token))
	if len(b) == 0 {
		i.Bitmap = roaring.BitmapOf(id)
		i.Cardinality = 1
	} else {
		s2pkg.PanicErr(i.Unmarshal(b))
		if i.Bitmap.CheckedAdd(id) {
			i.Cardinality++
			// Too many documents under this token, remove oldest one
			if float64(i.Cardinality) > math.Max(float64(numDocs)/16, float64(MaxTokenDocIDs)) {
				i.Bitmap.Remove(i.Bitmap.Minimum())
				i.Cardinality--
			}
		}
	}
	s2pkg.PanicErr(ri.Put([]byte(token), i.Marshal()))
}

func getCounter(g *bbolt.Bucket) (int, int) {
	b := g.Get([]byte("_total_tokens"))
	c := g.Get([]byte("_total_docs"))
	return int(s2pkg.BytesToUint64(b)), int(s2pkg.BytesToUint64(c))
}

func setCounter(g *bbolt.Bucket, v, v2 int) {
	g.Put([]byte("_total_tokens"), s2pkg.Uint64ToBytes(uint64(v)))
	g.Put([]byte("_total_docs"), s2pkg.Uint64ToBytes(uint64(v2)))
}

func getDoc(g *bbolt.Bucket, id uint32) (d Document, found bool) {
	b := g.Get(s2pkg.Uint64ToBytes(uint64(id)))
	if len(b) == 0 {
		return d, false
	}
	s2pkg.PanicErr(proto.Unmarshal(b, &d))
	return d, true
}

func setDoc(g *bbolt.Bucket, id uint32, d Document) {
	s2pkg.PanicErr(g.Put(s2pkg.Uint64ToBytes(uint64(id)), pmarshal(&d)))
}

func delDoc(g *bbolt.Bucket, id uint32) {
	s2pkg.PanicErr(g.Delete(s2pkg.Uint64ToBytes(uint64(id))))
}

func getRI(ri *bbolt.Bucket, token string) (i RevertedIndex, found bool) {
	b := ri.Get([]byte(token))
	if len(b) == 0 {
		return i, false
	}
	s2pkg.PanicErr(i.Unmarshal(b))
	return i, true
}

func (idx *Index) getBucket(tx *bbolt.Tx, readonly bool) (*bbolt.Bucket, *bbolt.Bucket) {
	if readonly {
		return tx.Bucket([]byte("index." + idx.name)), tx.Bucket([]byte("index.ri." + idx.name))
	}
	bk1, err := tx.CreateBucketIfNotExists([]byte("index." + idx.name))
	s2pkg.PanicErr(err)
	bk2, err := tx.CreateBucketIfNotExists([]byte("index.ri." + idx.name))
	s2pkg.PanicErr(err)
	return bk1, bk2
}

func (idx *Index) Index(docs map[uint32]string) int {
	tDocs := make(map[uint32]Document, len(docs))
	for idx, d := range docs {
		tDocs[idx] = Split(d)
	}

	added := 0
	s2pkg.PanicErr(idx.db.Update(func(tx *bbolt.Tx) error {
		g, ri := idx.getBucket(tx, false)
		numTokens, numDocs := getCounter(g)

		for id, doc := range tDocs {
			oldDoc, found := getDoc(g, id)
			if found {
				for _, seg := range oldDoc.Tokens {
					delDocRI(ri, seg.Token, id)
					numTokens--
				}
				numDocs -= 1
				if !doc.Valid() {
					delDoc(g, id)
					continue
				}
			}
			if !doc.Valid() {
				continue
			}
			for _, seg := range doc.Tokens {
				addDocRI(ri, seg.Token, id, numDocs)
			}
			added++
			numDocs += 1
			numTokens += len(doc.Tokens)
			setDoc(g, id, doc)
		}

		setCounter(g, numTokens, numDocs)
		return nil
	}))
	return added
}

func (idx *Index) TopN(bm25 bool, n int, query ...string) (ids []Result) {
	if len(query) == 0 {
		return
	}
	if n <= 0 {
		n = 20
	}

	var h resultHeap
	h.rev = true
	for _, q := range query {
		idx.search(&h, q, bm25, n)
	}
	return h.ToArray()
}

func (idx *Index) search(h *resultHeap, content string, bm25 bool, n int) {
	q := Split(content)
	if !q.Valid() {
		return
	}

	idx.db.View(func(tx *bbolt.Tx) error {
		g, ri := idx.getBucket(tx, true)
		if g == nil || ri == nil {
			return nil
		}
		var first *roaring.Bitmap
		var riCardinality = map[string]int{}
		for _, s := range q.Tokens {
			second, ok := getRI(ri, s.Token)
			if !ok {
				return nil
			}
			riCardinality[s.Token] = second.Cardinality
			if first == nil {
				first = second.Bitmap.Clone()
			} else {
				first.And(second.Bitmap)
			}
		}

		if first != nil {
			_, numDocs := getCounter(g)
			idfs := make([]float64, len(q.Tokens))
			for i, s := range q.Tokens {
				idfs[i] = math.Log2(float64(numDocs) / float64(riCardinality[s.Token]+1))
			}
			avgDocLength := avgDocNumTokens(g)

			start := time.Now()
			first.Iterate(func(id uint32) bool {
				if to := idx.corpus.Timeout.Search; to > 0 && time.Since(start) > to {
					logrus.Errorf("Indexer: timeout")
					return true
				}
				sum := 0.0
				doc, ok := getDoc(g, id)
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

func (idx *Index) Stat() (out struct {
	NumTokens          int
	NumDocuments       int
	AvgNumTokensPerDoc float64
}) {
	s2pkg.PanicErr(idx.db.View(func(tx *bbolt.Tx) error {
		if g, _ := idx.getBucket(tx, true); g != nil {
			out.NumTokens, out.NumDocuments = getCounter(g)
			out.AvgNumTokensPerDoc = float64(out.NumTokens) / float64(out.NumDocuments+1)
		}
		return nil
	}))
	return
}

func (idx *Index) Name() string {
	return idx.name
}

func avgDocNumTokens(g *bbolt.Bucket) (r float64) {
	if g != nil {
		nt, nd := getCounter(g)
		r = float64(nt) / float64(nd+1)
	}
	return r
}
