package server

import (
	"time"
	"unsafe"

	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/wire"
)

type interop struct{}

func (i *interop) s() *Server { return (*Server)(unsafe.Pointer(i)) }

func (i *interop) Append(opts *s2.AppendOptions, key string, data ...any) ([][]byte, error) {
	out := &wire.DummySink{}
	defer i.s().recoverLogger(time.Now(), "APPEND", out, nil)

	if opts == nil {
		opts = &s2.AppendOptions{}
	}

	v := make([][]byte, len(data))
	for i := range data {
		v[i] = s2.ToBytes(data[i])
	}
	i.s().execAppend(out, key, nil, v, *opts)

	if err := out.Err(); err != nil {
		return nil, err
	}
	return hexDecodeBulks(out.Val().([][]byte)), nil
}

func (i *interop) Select(opts *s2.SelectOptions, key string, start []byte, n int) ([]s2.Pair, error) {
	out := &wire.DummySink{}
	defer i.s().recoverLogger(time.Now(), "SELECT", out, nil)

	if opts == nil {
		opts = &s2.SelectOptions{}
	}

	data, err := i.s().execSelect(key, i.s().translateCursor(start, opts.Desc), n, *opts)
	if err != nil {
		return nil, err
	}
	if len(data) > n {
		data = data[:n]
	}
	return data, nil
}

func (i *interop) Scan(cursor string, count int, local bool) (string, []string) {
	out := &wire.DummySink{}
	defer i.s().recoverLogger(time.Now(), "SCAN", out, nil)

	i.s().execScan(out, cursor, count, local)
	res := out.Val().([]any)
	return res[0].(string), res[1].([]string)
}
