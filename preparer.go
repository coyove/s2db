package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/ranges"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/sdss/future"
	"github.com/sirupsen/logrus"
)

func (s *Server) Append(key string, data [][]byte) ([][]byte, error) {
	id := future.Get(s.Channel)
	defer id.Wait()
	return s.runAppend(id, key, data)
}

func (s *Server) runAppend(id future.Future, key string, data [][]byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) >= 65536 {
		return nil, fmt.Errorf("too many elements to append")
	}

	bkPrefix, bkHash := ranges.GetKey(key)

	mu := &s.locks[s2pkg.HashStr(key)&0xffff]
	mu.Lock()
	defer mu.Unlock()

	hash, err := extdb.GetKey(s.DB, bkHash)
	if err != nil {
		return nil, err
	}
	switch len(hash) {
	case 0:
		hash = make([]byte, 16)
	case 16:
		// ok
	default:
		return nil, fmt.Errorf("fatal: invalid hash length: %d", len(hash))
	}

	tx := s.DB.NewBatch()
	defer tx.Close()

	idx := make([]byte, 16) // id(8b) + random(4b) + cmd(2b) + index(2b)
	binary.BigEndian.PutUint64(idx[:], uint64(id))
	rand.Read(idx[8:12])
	idx[12] = 1 // 'append' comamnd code

	var kk [][]byte
	for i, p := range data {
		binary.BigEndian.PutUint16(idx[14:], uint16(i))
		kk = append(kk, s2pkg.Bytes(idx))

		if err := tx.Set(append(bkPrefix, idx...), p, pebble.Sync); err != nil {
			return nil, err
		}
		hash = s2pkg.AddBytesInplace(hash, idx)
	}

	if err := tx.Set(bkHash, hash, pebble.Sync); err != nil {
		return nil, err
	}
	return kk, tx.Commit(pebble.Sync)
}

func (s *Server) setMissing(key string, kvs [][2]string) {
	if len(kvs) == 0 {
		s.Survey.PeerOnOK.Incr(1)
		return
	}
	go func(start time.Time) {
		mu := &s.locks[s2pkg.HashStr(key)&0xffff]
		mu.Lock()
		defer mu.Unlock()
		if err := s.rawSetStringsHexKey(key, kvs); err != nil {
			logrus.Errorf("setMissing: %v", err)
		}
		s.Survey.PeerOnMissing.Incr(time.Since(start).Milliseconds())
	}(time.Now())
}

func (s *Server) rawSetStringsHexKey(key string, kvs [][2]string) error {
	if len(kvs) == 0 {
		return nil
	}
	tx := s.DB.NewBatch()
	defer tx.Close()

	bkPrefix, _ := ranges.GetKey(key)

	var k, v []byte
	c := 0
	for _, kv := range kvs {
		if _, ok := s.fillCache.GetSimple(kv[0]); ok {
			continue
		}
		k = append(append(k[:0], bkPrefix...), hexDecode([]byte(kv[0]))...)
		v = append(v[:0], kv[1]...)
		if err := tx.Set(k, v, pebble.Sync); err != nil {
			return err
		}
		c++
		s.fillCache.AddSimple(kv[0], 1)
	}
	if c == 0 {
		return nil
	}
	return tx.Commit(pebble.Sync)
}

func (s *Server) Range(key string, start []byte, n int) (data [][]byte, err error) {
	desc := false
	if n < 0 {
		desc, n = true, -n
	}

	bkPrefix, _ := ranges.GetKey(key)

	c := s.DB.NewIter(&pebble.IterOptions{
		LowerBound: bkPrefix,
		UpperBound: s2pkg.IncBytes(bkPrefix),
	})
	defer c.Close()

	// for c.First(); c.Valid(); c.Next() {
	// 	fmt.Println(c.Key())
	// }

	start = append(bkPrefix, start...)
	if desc {
		c.SeekLT(s2pkg.IncBytesInplace(start))
	} else {
		c.SeekGE(start)
	}
	for c.Valid() && len(data) < n*3 {
		k := bytes.TrimPrefix(c.Key(), bkPrefix)

		ns := binary.BigEndian.Uint64(k)
		ts := float64(ns/1e8) / 10
		tsbuf := []byte(strconv.FormatFloat(ts, 'f', -1, 64))

		data = append(data, hexEncode(k), tsbuf, s2pkg.Bytes(c.Value()))
		if desc {
			c.Prev()
		} else {
			c.Next()
		}
	}
	return
}

func hexEncode(k []byte) []byte {
	k0 := make([]byte, len(k)*2)
	hex.Encode(k0, k)
	return k0
}

func hexDecode(k []byte) []byte {
	k0 := make([]byte, len(k)/2)
	hex.Decode(k0, k)
	return k0
}

func sortAndSubtract(merged []string, orig []string, desc bool) (sorted []string, subtracted [][2]string) {
	type foo struct{ id, ts, data string }
	convert := func(in []string) (out []foo) {
		sout := (*reflect.SliceHeader)(unsafe.Pointer(&out))
		sin := (*reflect.SliceHeader)(unsafe.Pointer(&in))
		sout.Data = sin.Data
		sout.Len = sin.Len / 3
		sout.Cap = sin.Cap / 3
		return
	}

	m0 := convert(merged)
	orig0 := convert(orig)

	sort.Slice(m0, func(i, j int) bool {
		return m0[i].id > m0[j].id == desc
	})
	for i := len(m0) - 1; i > 0; i-- {
		if m0[i] == m0[i-1] {
			m0 = append(m0[:i], m0[i+1:]...)
		}
	}

	for foo := m0; len(foo) > 0; foo = foo[1:] {
		head := foo[0]
		if len(orig0) > 0 && head == orig0[0] {
			orig0 = orig0[1:]
		} else {
			subtracted = append(subtracted, [2]string{head.id, head.data})
		}
	}

	return merged[:len(m0)*3], subtracted
}
