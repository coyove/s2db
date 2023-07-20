package server

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/wire"
)

type interop struct{}

func (i *interop) s() *Server { return (*Server)(unsafe.Pointer(i)) }

func (i *interop) Append(key string, dpLen byte, data ...any) ([][]byte, error) {
	return i.append(false, key, dpLen, data...)
}

func (i *interop) AppendEffect(key string, dpLen byte, data ...any) ([][]byte, error) {
	return i.append(true, key, dpLen, data...)
}

func (i *interop) append(wait bool, key string, dpLen byte, data ...any) ([][]byte, error) {
	out := &wire.DummySink{}
	defer i.s().recoverLogger(time.Now(), "APPEND", out, nil)

	v := make([][]byte, len(data))
	for i := range data {
		v[i] = toBytes(data[i])
	}
	i.s().execAppend(out, key, dpLen, nil, v, true, wait)

	if err := out.Err(); err != nil {
		return nil, err
	}
	return hexDecodeBulks(out.Val().([][]byte)), nil
}

func (i *interop) Select(key string, start []byte, n int, flag int) ([]s2.Pair, error) {
	out := &wire.DummySink{}
	defer i.s().recoverLogger(time.Now(), "SELECT", out, nil)

	desc := flag&RangeDesc > 0
	i.s().execSelect(out, key, i.s().translateCursor(start, desc), n, flag)
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
		res = append(res, x)
	}
	return res, nil
}

func (i *interop) HSet(wait bool, key string, kvs ...[]byte) ([][]byte, error) {
	out := &wire.DummySink{}
	defer i.s().recoverLogger(time.Now(), "HSET", out, nil)
	i.s().execHSet(out, key, nil, kvs, true, wait)

	if err := out.Err(); err != nil {
		return nil, err
	}
	res := out.Val().([][]byte)
	for i := range res {
		res[i] = hexDecode(res[i])
	}
	return res, nil
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

func (i *interop) Scan(cursor string, count int, local bool) (string, []string) {
	out := &wire.DummySink{}
	defer i.s().recoverLogger(time.Now(), "SCAN", out, nil)

	i.s().execScan(out, cursor, count, local)
	res := out.Val().([]any)
	return res[0].(string), res[1].([]string)
}

func toBytes(v any) []byte {
	switch v := v.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	case int, int8, int16, int32, int64:
		return strconv.AppendInt(nil, reflect.ValueOf(v).Int(), 10)
	case uint, uint8, uint16, uint32, uint64, uintptr:
		return strconv.AppendUint(nil, reflect.ValueOf(v).Uint(), 10)
	case float32, float64:
		return strconv.AppendFloat(nil, reflect.ValueOf(v).Float(), 'f', -1, 64)
	default:
		return fmt.Append(nil, v)
	}
}
