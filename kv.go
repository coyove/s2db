package main

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/ranges"
	s2pkg "github.com/coyove/s2db/s2pkg"
)

func (s *Server) Get(key string) (value []byte, err error) {
	return extdb.GetKey(s.DB, ranges.GetKVKey(key))
}

func (s *Server) MGet(keys []string) (values [][]byte, err error) {
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

func (s *Server) RI(count int, terms []string) (res []s2pkg.Pair, err error) {
	if len(terms) == 0 {
		return nil, nil
	}
	c := s.DB.NewIter(&pebble.IterOptions{})
	defer c.Close()

	idfs := make([]float64, len(terms))
	total := 0.0

	termKS := make([][]byte, len(terms))
	termCard := make([]int64, len(terms))
	termMinCard := int64(math.MaxInt64)
	termMin := ""
	for i := range terms {
		if terms[i] == "" {
			return nil, fmt.Errorf("empty key name")
		}
		var card int64
		if v, ok := extdb.GetKeyCursor(c, ranges.GetZSetCounterKey(terms[i])); ok {
			card = int64(s2pkg.BytesToUint64(v))
		}
		if card <= termMinCard && card > 0 {
			termMin, termMinCard = terms[i], card
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

	if termMin == "" {
		return nil, nil
	}

	c2 := s.DB.NewIter(&pebble.IterOptions{})
	defer c2.Close()

	_, bkScore, _ := ranges.GetZSetRangeKey(termMin)
	for c.SeekGE(bkScore); c.Valid() && len(res) < count; c.Next() {
		if !bytes.HasPrefix(c.Key(), bkScore) {
			break
		}
		// score := s2pkg.BytesToFloat(c.Key()[len(bkScore):])
		member := c.Key()[len(bkScore)+8:]
		var total float64
		for i, ks := range termKS {
			if v, ok := extdb.GetKeyCursor(c2, append(ks, member...)); ok {
				tf := s2pkg.BytesToFloat(v)
				total += tf * idfs[i]
			}
		}
		res = append(res, s2pkg.Pair{Member: string(member), Score: total})
	}
	sort.Slice(res, func(i, j int) bool { return res[i].Score > res[j].Score })
	return
}
