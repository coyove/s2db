package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"time"

	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/s2pkg/fts"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) runIndexBuild(key string, command *redisproto.Command) (int64, error) {
	// IDXADD id content key1 ... keyN
	return int64(s.IndexDoc(key, command.Get(2), restCommandsToKeys(3, command))), nil
}

func (s *Server) IndexDoc(id, content string, riKeys []string) int {
	if len(riKeys) == 0 {
		return 0
	}

	doc := fts.Split(content)
	if !doc.Valid() {
		return 0
	}

	doc.Prefixs = riKeys

	var tokens []s2pkg.Pair
	for _, s := range doc.Tokens {
		tokens = append(tokens, s2pkg.Pair{Member: s.Token, Score: doc.TermFreq(s.Token)})
	}

	// Remove existed document
	f := s.indexRemoveFuncs(id)

	// (Re)add document
	f = append(f, func() {
		s.ZAdd(s.DocsStoreKey, true, []s2pkg.Pair{{
			Score:  float64(doc.NumTokens),
			Member: id,
			Data:   doc.MarshalBinary(),
		}})
	})

	// (Re)add document into reverted indices
	for _, p := range riKeys {
		for _, t := range tokens {
			k := p + t.Member
			ri := s2pkg.Pair{Member: id, Score: t.Score}
			f = append(f, func() { s.ZAdd(k, true, []s2pkg.Pair{ri}) })
		}
	}

	for _, ff := range f {
		ff()
	}
	return len(f)
}

func (s *Server) IndexDel(id string) int {
	f := s.indexRemoveFuncs(id)
	for _, ff := range f {
		ff()
	}
	return len(f)
}

func (s *Server) indexRemoveFuncs(id string) (f []func()) {
	tOldDocBytes, err := s.ZMData(s.DocsStoreKey, []string{id}, redisproto.Flags{})
	s2pkg.PanicErr(err)
	buf := tOldDocBytes[0]
	if len(buf) == 0 {
		return
	}

	f = append(f, func() {
		s.ZRem(s.DocsStoreKey, true, []string{id})
	})

	// Remove document in reverted index
	var oldDoc fts.Document
	s2pkg.PanicErr(proto.Unmarshal(buf, &oldDoc))
	for _, t := range oldDoc.Tokens {
		for _, p := range oldDoc.Prefixs {
			k := p + t.Token
			f = append(f, func() { s.ZRem(k, true, []string{id}) })
		}
	}
	return f
}

func (s *Server) runIndexDocsInfo(docIds []string) (infos []interface{}) {
	for _, s := range s.IndexDocsInfo(docIds) {
		infos = append(infos, s)
	}
	return
}

func (s *Server) IndexDocsInfo(docIds []string) (infos [][]string) {
	tOldDocsBytes, err := s.ZMData(s.DocsStoreKey, docIds, redisproto.Flags{})
	s2pkg.PanicErr(err)

	for i, buf := range tOldDocsBytes {
		if len(buf) == 0 {
			continue
		}

		var lines []string
		var oldDoc fts.Document
		s2pkg.PanicErr(proto.Unmarshal(buf, &oldDoc))

		lines = append(lines, docIds[i])
		for _, t := range oldDoc.Tokens {
			lines = append(lines, fmt.Sprintf("%s:%d", t.Token, t.Count))
			for _, p := range oldDoc.Prefixs {
				lines = append(lines, fmt.Sprintf("<prefix>%s:%d", p, s.ZCard(p+t.Token)))
			}
		}
		infos = append(infos, lines)
	}
	return
}

func (s *Server) IndexSearch(prefix string, content string, flags redisproto.Flags) (docs []s2pkg.Pair) {
	q := fts.Split(content)
	if !q.Valid() {
		return
	}

	numDocs := s.ZCard(s.DocsStoreKey)
	if numDocs == 0 {
		return
	}

	idfs := make([]float64, 0, len(q.Tokens))
	txs := make([]*bbolt.Tx, 0, len(q.Tokens))
	cursors := make([]*bbolt.Cursor, 0, len(q.Tokens))
	cursorMaxShift := make([]int, 0, len(q.Tokens))

	defer func() {
		for _, tx := range txs {
			if tx != nil {
				tx.Rollback()
			}
		}
	}()

	for _, tok := range q.Tokens {
		riKey := prefix + tok.Token

		tx, err := s.pick(riKey).Begin(false)
		if err != nil {
			return
		}

		bkName := tx.Bucket([]byte("zset." + riKey))
		bkScore := tx.Bucket([]byte("zset.score." + riKey))
		if bkScore == nil || bkName == nil {
			return
		}

		numDocsOfToken := bkScore.Sequence()
		if numDocsOfToken == 0 {
			return
		}

		txs = append(txs, tx)
		idfs = append(idfs, math.Log2(float64(numDocs)/float64(numDocsOfToken+1)))
		cursors = append(cursors, bkName.Cursor())
		cursorMaxShift = append(cursorMaxShift, int(math.Log2(float64(numDocsOfToken))+1))
	}

	move := func(start []byte) ([]byte, float64) {
		head, tf := seekCursor(cursors[0], start, cursorMaxShift[0])
		if len(head) == 0 {
			return nil, math.NaN()
		}

		aligned := 1
		idftf := s2pkg.BytesToFloat(tf) * idfs[0]

		for i := 1; i < len(cursors); i++ {
			name, tf := seekCursor(cursors[i], start, cursorMaxShift[i])
			if len(name) == 0 {
				return nil, math.NaN()
			}
			if cmp := bytes.Compare(name, head); cmp == 0 {
				aligned++
			} else if cmp < 0 {
				head = name
			}
			idftf += s2pkg.BytesToFloat(tf) * idfs[i]
		}
		if aligned == len(cursors) {
			return head, idftf
		}
		return head, math.NaN()
	}

	var start []byte
	var idftf float64
	var h fts.ResultHeap
	h.Rev = true

	for startTime := time.Now(); time.Since(startTime) < flags.TIMEOUT; {
		start, idftf = move(start)
		if len(start) == 0 {
			break
		}
		if idftf == idftf {
			heap.Push(&h, fts.Result{ID: string(start), Score: idftf})
			if h.Len() > flags.COUNT {
				heap.Pop(&h)
			}
		}
	}

	res, ids := h.ToArray()
	scores, err := s.ZMScore(s.DocsStoreKey, ids, 0)
	s2pkg.PanicErr(err)

	for i := range res {
		if scores[i] == scores[i] {
			docs = append(docs, s2pkg.Pair{Member: ids[i], Score: res[i].Score})
		}
	}
	return
}

func seekCursor(c *bbolt.Cursor, key []byte, max int) ([]byte, []byte) {
	if key == nil {
		return c.Last()
	}

	w := 0
	for k, v := c.Prev(); len(k) > 0; k, v = c.Prev() {
		if bytes.Compare(k, key) <= 0 {
			return k, v
		}
		if w++; max > 0 && w > max {
			k, v := c.Seek(key)
			if !bytes.Equal(k, key) {
				k, v = c.Prev()
			}
			return k, v
		}
	}
	return nil, nil
}

func (s *Server) ReloadDict() {
	fts.LoadDict(s.loadDict(), true)
}

func (s *Server) loadDict() (words []string) {
	p, err := s.ZRange(false, s.DocsStoreKey+"words", 0, -1, redisproto.Flags{LIMIT: s2pkg.RangeHardLimit})
	s2pkg.PanicErr(err)
	for _, p := range p {
		words = append(words, p.Member)
	}
	log.Info("loadDict: count=", len(words))
	return
}
