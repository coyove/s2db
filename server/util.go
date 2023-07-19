package server

import (
	"encoding/hex"
	"reflect"
	"sort"
	"strconv"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/wire"
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
			p[i-1].Con = p[i-1].Con || p[i].Con // inherit the consolidation mark if any
			p = append(p[:i], p[i+1:]...)
		}
	}
	return p
}

func parseAPPEND(K *wire.Command) (data, ids [][]byte, dpLen byte, sync, wait bool) {
	data = [][]byte{K.BytesRef(2)}
	for i := 3; i < K.ArgCount(); i++ {
		if K.StrEqFold(i, "and") {
			data = append(data, K.BytesRef(i+1))
			i++
		} else if K.StrEqFold(i, "setid") {
			ids = K.Argv[i+1 : i+1+len(data)]
			i += len(data)
		} else if K.StrEqFold(i, "dp") {
			dpLen = byte(K.Int64(i + 1))
			i++
		}

		wait = wait || K.StrEqFold(i, "wait")
		sync = sync || K.StrEqFold(i, "sync")
	}
	return
}

func parseSELECT(K *wire.Command) (n int, desc, raw bool, flag int) {
	// SELECT key start n [...]
	n = K.Int(3)
	for i := 4; i < K.ArgCount(); i++ {
		desc = desc || K.StrEqFold(i, "desc")
		raw = raw || K.StrEqFold(i, "raw")
	}
	if desc {
		flag |= RangeDesc
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

func makeHashmapKey(key string) (prefix []byte) {
	prefix = append(append(make([]byte, 64)[:0], "h"...), key...)
	return
}

func makeSSTableWMKey(id uint64) (prefix []byte) {
	prefix = strconv.AppendUint(append(make([]byte, 64)[:0], "t"...), id, 10)
	return
}

func makeSSTableDedupKey(id uint64) (prefix []byte) {
	prefix = strconv.AppendUint(append(make([]byte, 64)[:0], "td"...), id, 10)
	return
}

func newPrefixIter(db *pebble.DB, key []byte) *pebble.Iterator {
	return db.NewIter(&pebble.IterOptions{
		LowerBound: key,
		UpperBound: s2.IncBytes(key),
	})
}

func hexEncodeBulks(ids [][]byte) [][]byte {
	hexIds := make([][]byte, len(ids))
	for i := range hexIds {
		hexIds[i] = hexEncode(ids[i])
	}
	return hexIds
}

func hexDecodeBulks(ids [][]byte) [][]byte {
	hexIds := make([][]byte, len(ids))
	for i := range hexIds {
		hexIds[i] = hexDecode(ids[i])
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

func rvToFloat64(v reflect.Value) float64 {
	if v.Kind() >= reflect.Int && v.Kind() <= reflect.Int64 {
		return float64(v.Int())
	}
	if v.Kind() >= reflect.Uint && v.Kind() <= reflect.Uint64 {
		return float64(v.Uint())
	}
	return 0
}
