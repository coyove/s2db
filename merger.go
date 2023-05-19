package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/sdss/future"
)

type hashmapData struct {
	ts   int64
	key  []byte
	data []byte
}

type hashmapMerger struct {
	s *Server
	m map[string]hashmapData
}

func hashmapMergerIter(p []byte, f func(d hashmapData) bool) (err error) {
	if p[0] != 0x01 {
		return fmt.Errorf("setMerger: invalid opcode %x", p)
	}
	rd := bufio.NewReader(bytes.NewReader(p[1+4:]))
	for {
		// ts (8b) + keylen + key + valuelen + value
		var ts int64
		if err := binary.Read(rd, binary.BigEndian, &ts); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		kl, err := binary.ReadUvarint(rd)
		if err != nil {
			return err
		}
		k := make([]byte, kl)
		if _, err := io.ReadFull(rd, k); err != nil {
			return err
		}

		vl, err := binary.ReadUvarint(rd)
		if err != nil {
			return err
		}

		v := make([]byte, vl)
		if _, err := io.ReadFull(rd, v); err != nil {
			return err
		}

		if !f(hashmapData{ts, k, v}) {
			break
		}
	}
	return
}

func (a *hashmapMerger) MergeNewer(value []byte) error {
	return a.MergeOlder(value)
}

func (s *hashmapMerger) MergeOlder(value []byte) error {
	return hashmapMergerIter(value, func(d hashmapData) bool {
		k := *(*string)(unsafe.Pointer(&d.key))
		if d.ts > s.m[k].ts {
			s.m[k] = d
		}
		return true
	})
}

func (s *hashmapMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	start := future.UnixNano()
	x := hashmapMergerBytes(s.m)
	s.s.Survey.HashMerger.Incr((future.UnixNano() - start) / 1e6)
	return x, nil, nil
}

func hashmapMergerBytes(m map[string]hashmapData) []byte {
	tmp := make([]byte, 0, len(m)*32)
	tmp = append(tmp, 0x01)
	tmp = binary.BigEndian.AppendUint32(tmp, uint32(len(m)))
	for k, v := range m {
		tmp = binary.BigEndian.AppendUint64(tmp, uint64(v.ts))
		tmp = binary.AppendUvarint(tmp, uint64(len(k)))
		tmp = append(tmp, k...)
		tmp = binary.AppendUvarint(tmp, uint64(len(v.data)))
		tmp = append(tmp, v.data...)
	}
	return tmp
}

func (s *Server) createMerger() *pebble.Merger {
	return &pebble.Merger{
		Merge: func(key, value []byte) (pebble.ValueMerger, error) {
			if len(value) < 1 {
				return nil, fmt.Errorf("Merger: too short")
			}
			switch value[0] {
			case 1:
				res := &hashmapMerger{}
				res.s = s
				res.m = map[string]hashmapData{}
				err := hashmapMergerIter(value, func(d hashmapData) bool {
					res.m[*(*string)(unsafe.Pointer(&d.key))] = d
					return true
				})
				return res, err
			}
			return nil, fmt.Errorf("Merger: invalid opcode: %x", value[0])
		},

		Name: "pebble.concatenate", // keep the name same as the default one for data backward compatibilities
	}
}
