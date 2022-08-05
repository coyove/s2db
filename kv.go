package main

import (
	"bytes"

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
