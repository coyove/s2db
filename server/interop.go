package server

import (
	"time"
	"unsafe"

	"github.com/coyove/s2db/s2"
)

type interop struct{}

func (i *interop) s() *Server { return (*Server)(unsafe.Pointer(i)) }

func (i *interop) Append(opts *s2.AppendOptions, key string, data ...any) (_out [][]byte, _err error) {
	defer i.s().recoverLogger(time.Now(), "APPEND", nil, nil, &_err)

	if opts == nil {
		opts = &s2.AppendOptions{}
	}

	v := make([][]byte, len(data))
	for i := range data {
		v[i] = s2.ToBytes(data[i])
	}
	res, err := i.s().execAppend(key, nil, v, *opts)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (i *interop) Select(opts *s2.SelectOptions, key string, start []byte, n int) (_out []s2.Pair, _err error) {
	defer i.s().recoverLogger(time.Now(), "SELECT", nil, nil, &_err)

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

func (i *interop) Count(key string, start, end []byte, max int) (_out int, _err error) {
	defer i.s().recoverLogger(time.Now(), "COUNT", nil, nil, &_err)

	return i.s().execCount(key, i.s().translateCursor(start, false), i.s().translateCursor(end, false), max)
}

func (i *interop) Lookup(id []byte) (_data []byte, _err error) {
	defer i.s().recoverLogger(time.Now(), "LOOKUP", nil, nil, &_err)

	return i.s().execLookup(id)
}

func (i *interop) Scan(cursor string, count int, local bool) (_next string, _keys []string, _err error) {
	defer i.s().recoverLogger(time.Now(), "SCAN", nil, nil, &_err)

	_next, _keys = i.s().execScan(cursor, count, local)
	return
}
