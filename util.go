package main

import (
	"encoding/hex"
	"sort"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/s2"
)

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

func skp(key string) (live, watermark []byte) {
	live = append(append(append(make([]byte, 64)[:0], "s"...), key...), 0)
	watermark = append(append(make([]byte, 64)[:0], "s"...), key...)
	return
}

func newPrefixIter(db *pebble.DB, key []byte) *pebble.Iterator {
	return db.NewIter(&pebble.IterOptions{
		LowerBound: key,
		UpperBound: s2.IncBytes(key),
	})
}

func ssbb(in []string) (out [][]byte) {
	x := make([]struct {
		a string
		c int
	}, len(in))
	for i := range x {
		x[i].a = in[i]
		x[i].c = len(in[i])
	}
	return *(*[][]byte)(unsafe.Pointer(&x))
}
