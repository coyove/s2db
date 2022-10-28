package main

import (
	"bytes"
	"math"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/bitmap"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/ranges"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
)

func (s *Server) SCard(key string) (count int64) {
	_, i, _, err := extdb.GetKeyNumber(s.DB, ranges.GetSetCounterKey(key))
	s2pkg.PanicErr(err)
	return int64(i)
}

func (s *Server) SMIsMember(key string, members ...string) (res []int) {
	if len(members) == 0 {
		return nil
	}
	res = make([]int, len(members))

	bkName, _ := ranges.GetSetRangeKey(key)
	iter := ranges.NewPrefixIter(s.DB, bkName)
	defer iter.Close()

	for i, m := range members {
		tmp := append(bkName, m...)
		if iter.SeekGE(tmp) && bytes.Equal(tmp, iter.Key()) {
			res[i] = 1
		}
	}
	return
}

func (s *Server) SMembers(key string, flags wire.Flags) (res [][]byte) {
	bkName, _ := ranges.GetSetRangeKey(key)
	iter := ranges.NewPrefixIter(s.DB, bkName)
	defer iter.Close()
	if !math.IsNaN(flags.MemberBF) {
		m := bitmap.NewBloomFilter(int(s.SCard(key)), flags.MemberBF)
		for iter.First(); iter.Valid() && len(res) < ranges.HardLimit; iter.Next() {
			m.AddBinary(iter.Key()[len(bkName):])
		}
		res = [][]byte{m.MarshalBinary()}
	} else {
		for iter.First(); iter.Valid() && len(res) < ranges.HardLimit; iter.Next() {
			res = append(res, s2pkg.Bytes(iter.Key()[len(bkName):]))
		}
	}
	return
}

func (s *Server) ForeachSet(cursor string, f func(string) bool) {
	opts := &pebble.IterOptions{}
	opts.LowerBound = []byte("zpset___" + cursor)
	opts.UpperBound = []byte("zpset__\xff")
	c := s.DB.NewIter(opts)
	defer c.Close()
	for c.First(); c.Valid(); {
		k := c.Key()[8:]
		key := string(k[:bytes.IndexByte(k, 0)])
		if !f(key) {
			return
		}
		c.SeekGE([]byte("zpset___" + key + "\x01"))
	}
}

func (s *Server) SScan(key, cursor string, flags wire.Flags) (pairs []s2pkg.Pair, nextCursor string) {
	count, start := flags.Count+1, clock.Now()
	bkName, _ := ranges.GetSetRangeKey(key)
	iter := ranges.NewPrefixIter(s.DB, bkName)
	defer iter.Close()

	for iter.SeekGE(append(bkName, cursor...)); iter.Valid() && len(pairs) < count; iter.Next() {
		if time.Since(start) > flags.Timeout {
			break
		}
		m := iter.Key()[bytes.IndexByte(iter.Key(), 0)+1:]
		if flags.Match != "" && !s2pkg.MatchBinary(flags.Match, m) {
			continue
		}
		pairs = append(pairs, s2pkg.Pair{Member: string(m)})
	}
	if len(pairs) >= count {
		pairs, nextCursor = pairs[:count-1], pairs[count-1].Member
	}
	return
}
