package server

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/wire"
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

func parseAPPEND(K *wire.Command) (data, ids [][]byte, ttl int64, sync, wait bool) {
	data = [][]byte{K.BytesRef(2)}
	for i := 3; i < K.ArgCount(); i++ {
		if K.StrEqFold(i, "ttl") {
			ttl = K.Int64(i + 1)
			i++
		} else if K.StrEqFold(i, "and") {
			data = append(data, K.BytesRef(i+1))
			i++
		} else if K.StrEqFold(i, "setid") {
			ids = K.Argv[i+1:]
			break
		}
		wait = wait || K.StrEqFold(i, "wait")
		sync = sync || K.StrEqFold(i, "sync")
	}
	for i := len(data) - 1; i >= 0; i-- {
		if len(data[i]) == 0 {
			data = append(data[:i], data[i+1:]...)
		}
	}
	return
}

func parseSELECT(K *wire.Command) (n int, desc, distinct, raw bool, flag int) {
	// SELECT key start n [...]
	n = K.Int(3)
	for i := 4; i < K.ArgCount(); i++ {
		desc = desc || K.StrEqFold(i, "desc")
		distinct = distinct || K.StrEqFold(i, "distinct")
		raw = raw || K.StrEqFold(i, "raw")
	}
	if desc {
		flag |= RangeDesc
	}
	if distinct {
		flag |= RangeDistinct
	}
	if raw {
		flag |= RangeRaw
	}
	return
}

func parseHSET(K *wire.Command) (kvs, ids [][]byte, sync, wait bool) {
	kvs = [][]byte{K.BytesRef(2), K.BytesRef(3)}
	for i := 2; i < K.ArgCount(); i++ {
		if K.StrEqFold(i, "set") {
			kvs = append(kvs, K.BytesRef(i+1), K.BytesRef(i+2))
			i += 2
		} else if K.StrEqFold(i, "setid") {
			ids = K.Argv[i+1:]
			break
		} else {
			wait = wait || K.StrEqFold(i, "wait")
			sync = sync || K.StrEqFold(i, "sync")
		}
	}
	return
}

func parseHGETALL(K *wire.Command) (noCompress, ts, keysOnly bool, match []byte) {
	for i := 2; i < K.ArgCount(); i++ {
		noCompress = noCompress || K.StrEqFold(i, "nocompress")
		ts = ts || K.StrEqFold(i, "timestamp")
		keysOnly = keysOnly || K.StrEqFold(i, "keysonly")
		if K.StrEqFold(i, "match") {
			match = K.BytesRef(i + 1)
			i++
		}
	}
	return
}

func parseSCAN(K *wire.Command) (hash, index bool, count int) {
	for i := 2; i < K.ArgCount(); i++ {
		if K.StrEqFold(i, "count") {
			count = K.Int(i + 1)
			i++
		} else {
			index = index || K.StrEqFold(i, "index")
			hash = hash || K.StrEqFold(i, "hash")
		}
	}
	if count > 65536 {
		count = 65536
	}
	return
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

func tkp(id uint64) (prefix []byte) {
	prefix = strconv.AppendUint(append(make([]byte, 64)[:0], "t"...), id, 10)
	return
}

func trkp(id uint64) (prefix []byte) {
	prefix = strconv.AppendUint(append(make([]byte, 64)[:0], "t\x00"...), id, 10)
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

func hexEncodeBulks(ids [][]byte) [][]byte {
	hexIds := make([][]byte, len(ids))
	for i := range hexIds {
		hexIds[i] = hexEncode(ids[i])
	}
	return hexIds
}

func bbany(b [][]byte) []any {
	res := make([]any, len(b))
	for i := range res {
		res[i] = s2.Bytes(b[i])
	}
	return res
}
