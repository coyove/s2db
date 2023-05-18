package main

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/s2"
)

const lockShards = 32

func hexEncode(k []byte) []byte {
	_ = k[15]
	k0 := make([]byte, 32)
	hex.Encode(k0, k)
	return k0
}

func hexDecode(k []byte) []byte {
	_ = k[31]
	k0 := make([]byte, 16)
	if len(k) == 33 && k[16] == '_' {
		hex.Decode(k0[:8], k[:16])
		hex.Decode(k0[8:], k[17:])
	} else {
		hex.Decode(k0, k)
	}
	return k0
}

func sortPairs(p []s2.Pair, asc bool) []s2.Pair {
	sort.Slice(p, func(i, j int) bool {
		return p[i].Less(p[j]) == asc
	})

	for i := len(p) - 1; i > 0; i-- {
		if p[i].Equal(p[i-1]) {
			p[i-1].C = p[i-1].C || p[i].C // inherit the consolidation mark if any
			p = append(p[:i], p[i+1:]...)
		}
	}
	return p
}

func distinctPairsData(p []s2.Pair) []s2.Pair {
	m := map[string]bool{}
	for i := 0; i < len(p); {
		if m[p[i].DataForDistinct()] {
			p = append(p[:i], p[i+1:]...)
		} else {
			m[p[i].DataForDistinct()] = true
			i++
		}
	}
	return p
}

func buildFlag(c []bool, f []int) (flag int) {
	for i, c := range c {
		if c {
			flag |= f[i]
		}
	}
	return flag
}

func moveIter(iter *pebble.Iterator, desc bool) bool {
	if desc {
		return iter.Prev()
	}
	return iter.Next()
}

func kkp(key string) (prefix []byte) {
	prefix = append(append(append(make([]byte, 64)[:0], 'l'), key...), 0)
	return
}

func skp(key string) (prefix []byte) {
	prefix = append(append(make([]byte, 64)[:0], "h"...), key...)
	return
}

func newPrefixIter(db *pebble.DB, key []byte) *pebble.Iterator {
	return db.NewIter(&pebble.IterOptions{
		LowerBound: key,
		UpperBound: s2.IncBytes(key),
	})
}

func readBytes(r io.Reader) (out []byte, err error) {
	var n uint16
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	out = make([]byte, n)
	_, err = io.ReadFull(r, out)
	return
}

type keyLock struct {
	m [lockShards]sync.Map
	c [lockShards]atomic.Int64
}

func (kl *keyLock) lock(key string) bool {
	i := s2.HashStr(key) % lockShards
	_, loaded := kl.m[i].LoadOrStore(key, 1)
	if !loaded {
		kl.c[i].Add(1)
		return true
	}
	return false
}

func (kl *keyLock) unlock(key string) {
	i := s2.HashStr(key) % lockShards
	_, loaded := kl.m[i].LoadAndDelete(key)
	if !loaded {
		panic("unlock non-existed key: shouldn't happen")
	}
	kl.c[i].Add(-1)
}

func (kl *keyLock) count() (c int64) {
	for i := range kl.c {
		c += kl.c[i].Load()
	}
	return
}
