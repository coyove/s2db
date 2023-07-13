package server

import (
	"fmt"
	"math"
	"strings"
	"time"
	"unsafe"

	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/wire"
)

type interop struct{}

func (i *interop) s() *Server { return (*Server)(unsafe.Pointer(i)) }

func findKV(a, b string) float64 {
	for len(a) > 0 {
		eq := strings.IndexByte(a, '=')
		if eq == -1 {
			break
		}
		start := eq - len(b)
		if start >= 0 && strings.EqualFold(a[start:eq], b) {
			var x float64
			var neg bool
			var deci int
			for a := a[eq+1:]; len(a) > 0; a = a[1:] {
				if a[0] >= '0' && a[0] <= '9' {
					x = x*10 + float64(a[0]-'0')
					if deci > 0 {
						deci++
					}
				} else if a[0] == '-' {
					neg = true
				} else if a[0] == '.' {
					if deci > 0 {
						break
					}
					deci++
				} else {
					break
				}
			}
			if deci > 0 {
				x = x / math.Pow10(deci-1)
			}
			if neg {
				return -x
			}
			return x
		}
		a = a[eq+1:]
	}
	return 0
}

func (i *interop) Append(flag string, key string, data []byte, more ...[]byte) ([][]byte, error) {
	out := &wire.DummySink{}
	defer i.s().recoverLogger(time.Now(), "APPEND", out, nil)
	var v [][]byte
	if len(data) > 0 {
		v = append(v, data)
	}
	v = append(v, more...)
	i.s().execAppend(out, key, nil, v, int64(findKV(flag, "ttl")), true, s2.FoldIndex(flag, "wait"))

	if err := out.Err(); err != nil {
		return nil, err
	}
	return hexDecodeBulks(out.Val().([][]byte)), nil
}

func (i *interop) Select(flag string, key string, start []byte, n int) ([]s2.Pair, error) {
	out := &wire.DummySink{}
	defer i.s().recoverLogger(time.Now(), "SELECT", out, nil)

	var fi int
	var desc bool
	if s2.FoldIndex(flag, "raw") {
		fi |= RangeRaw
	}
	if s2.FoldIndex(flag, "desc") {
		fi |= RangeDesc
		desc = true
	}
	all := s2.FoldIndex(flag, "allpeers")
	i.s().execSelect(out, key, i.s().translateCursor(start, desc), n, fi)
	if err := out.Err(); err != nil {
		return nil, err
	}
	buf := out.Val().([][]byte)
	res := make([]s2.Pair, 0, len(buf)/3)
	for i := 0; i < len(buf); i += 3 {
		var x s2.Pair
		x.ID = hexDecode(buf[i])
		x.Data = buf[i+2]
		t := s2.ParseUint64(string(buf[i+1])) % 10
		x.Con = t&1 > 0
		x.All = t&2 > 0
		if all && !x.All {
			return nil, fmt.Errorf("not all peers respond")
		}
		res = append(res, x)
	}
	return res, nil
}

func (i *interop) HSet(flag string, key string, member, value []byte, more ...[]byte) error {
	out := &wire.DummySink{}
	defer i.s().recoverLogger(time.Now(), "HSET", out, nil)
	i.s().execHSet(out, key, nil, append([][]byte{member, value}, more...), true, s2.FoldIndex(flag, "wait"))

	if err := out.Err(); err != nil {
		return err
	}
	return nil
}

func (i *interop) HGetAll(key string, match string) ([][]byte, error) {
	out := &wire.DummySink{}
	defer i.s().recoverLogger(time.Now(), "HGETALL", out, nil)

	i.s().execHGetAll(out, key, true, false, false, []byte(match))
	if err := out.Err(); err != nil {
		return nil, err
	}
	return out.Val().([][]byte), nil
}

func (i *interop) ScanHash(cursor string, count int) (string, []string) {
	return i.s().ScanHash(cursor, count)
}

func (i *interop) ScanLookupIndex(cursor string, count int) (string, []string) {
	return i.s().ScanLookupIndex(cursor, count)
}

func (i *interop) ScanList(cursor string, count int) (string, []string) {
	return i.s().ScanList(cursor, count)
}
