package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"

	"github.com/AndreasBriese/bbloom"
	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/ranges"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
)

func (s *Server) Get(key string) (value []byte, err error) {
	return extdb.GetKey(s.DB, ranges.GetKVKey(key))
}

func (s *Server) MGet(keys ...string) (values [][]byte, err error) {
	if len(keys) == 0 {
		return nil, nil
	}
	values = make([][]byte, len(keys))
	iter := s.DB.NewIter(ranges.KVFullRange)
	defer iter.Close()
	for i, key := range keys {
		tmp := ranges.GetKVKey(key)
		iter.SeekGE(tmp)
		if iter.Valid() && bytes.Equal(iter.Key(), tmp) {
			values[i] = s2pkg.Bytes(iter.Value())
		}
	}
	return
}

func (s *Server) ForeachKV(cursor string, f func(string, []byte) bool) {
	opts := &pebble.IterOptions{}
	opts.LowerBound = []byte("zkv_____" + cursor)
	opts.UpperBound = []byte("zkv____\xff")
	c := s.DB.NewIter(opts)
	defer c.Close()
	for c.First(); c.Valid(); {
		key := string(c.Key()[8:])
		if !f(key, c.Value()) {
			return
		}
		c.SeekGE([]byte("zkv_____" + key + "\x01"))
	}
}

func (s *Server) RI(terms []string, flags wire.Flags) (res []s2pkg.Pair, err error) {
	if len(terms) == 0 {
		return nil, nil
	}

	deadline := clock.Now().Add(flags.Timeout)

	c := s.DB.NewIter(&pebble.IterOptions{})
	defer c.Close()

	idfs := make([]float64, len(terms))
	total := 0.0

	termKS := make([][]byte, len(terms))
	termCard := make([]int64, len(terms))
	termCardHeap := &s2pkg.PairHeap{}
	must := map[string]byte{}
	for i := range terms {
		if len(terms[i]) <= 1 {
			return nil, fmt.Errorf("invalid key name")
		}

		switch terms[i][0] {
		case '+', '-':
			must[terms[i][1:]] = terms[i][0]
			terms[i] = terms[i][1:]
		case '?':
			terms[i] = terms[i][1:]
		}

		var card int64
		if v, ok := extdb.CursorGetKey(c, ranges.GetZSetCounterKey(terms[i])); ok {
			card = int64(s2pkg.BytesToUint64(v))
		}
		if card > 0 {
			heap.Push(termCardHeap, s2pkg.Pair{Member: terms[i], Score: float64(card)})
		}
		termKS[i] = ranges.GetZSetNameKey(terms[i])
		termCard[i] = card
		total += float64(card) + 1
	}
	for i := range idfs {
		df := float64(termCard[i] + 1)
		if df == total {
			idfs[i] = 1
		} else {
			idfs[i] = math.Log(total / df)
		}
	}

	c2 := s.DB.NewIter(&pebble.IterOptions{})
	defer c2.Close()

	h := s2pkg.SortedPairArray{Limit: flags.Count + 1}
	first := s2pkg.Pair{}
	dedupMembers := bbloom.New(total, 0.001)

	for clock.Now().Before(deadline) && termCardHeap.Len() > 0 {
		termMin, _ := heap.Pop(termCardHeap).(s2pkg.Pair)
		coeff := 1.0
		if first.Score != 0 {
			coeff = first.Score / termMin.Score
		} else {
			first = termMin
		}

		if must[termMin.Member] == '-' {
			continue
		}

		_, bkScore, _ := ranges.GetZSetRangeKey(termMin.Member)

	NEXT:
		for c.SeekGE(bkScore); c.Valid() && clock.Now().Before(deadline); c.Next() {
			if !bytes.HasPrefix(c.Key(), bkScore) {
				break
			}
			// score := s2pkg.BytesToFloat(c.Key()[len(bkScore):])
			member := c.Key()[len(bkScore)+8:]
			if dedupMembers.Has(member) {
				continue
			}

			var total float64
			var hits, allHits int = 0, len(terms)
			for i, ks := range termKS {
				if v, ok := extdb.CursorGetKey(c2, append(ks, member...)); ok {
					if must[terms[i]] == '-' {
						allHits--
						continue NEXT
					}
					tf := s2pkg.BytesToFloat(v)
					total += tf * idfs[i]
					hits++
				} else {
					if must[terms[i]] == '+' {
						continue NEXT
					}
				}
			}
			total *= math.Pow(float64(hits)/float64(allHits), 2.5)
			total *= coeff

			if termCardHeap.Len() == 0 && len(terms) > 1 && total < 0.001 {
				break
			}

			h.Add(s2pkg.Pair{Member: string(member), Score: total})
			dedupMembers.Add(member)
		}
	}
	return h.ToPairs(flags.Count), nil
}
