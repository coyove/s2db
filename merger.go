package main

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/s2"
	"github.com/coyove/sdss/future"
)

type valueMerger struct {
	opcode byte
	buf    []byte
}

func (a *valueMerger) MergeNewer(value []byte) error {
	switch a.opcode {
	case 0x01:
		return a.MergeOlder(value)
	}
	panic("not implemented")
}

func (a *valueMerger) MergeOlder(value []byte) error {
	if len(value) < 1 {
		return fmt.Errorf("valueMerger: too short")
	}
	if value[0] != a.opcode {
		return fmt.Errorf("valueMerger: invalid operands: %x and %x", value[0], a.opcode)
	}
	switch value[0] {
	case 0x01:
		if len(value) < 9 {
			return fmt.Errorf("valueMerger 0x01: too short")
		}
		ts0 := future.Future(binary.BigEndian.Uint64(a.buf[1:]))
		ts1 := future.Future(binary.BigEndian.Uint64(value[1:]))
		if ts0 > ts1 {
			return nil
		}
		a.buf = s2.Bytes(value)
	default:
		return fmt.Errorf("valueMerger: invalid opcode %x", value[0])
	}
	return nil
}

// Finish returns the buffer that was constructed on-demand in `Merge{OlderNewer}()` calls.
func (a *valueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return a.buf, nil, nil
}

var crdbMerger = &pebble.Merger{
	Merge: func(key, value []byte) (pebble.ValueMerger, error) {
		if len(value) < 1 {
			return nil, fmt.Errorf("valueMerger: too short")
		}
		res := &valueMerger{}
		res.opcode = value[0]
		res.buf = append(res.buf, value...)
		return res, nil
	},

	Name: "pebble.concatenate", // keep the name same as the default one for data backward compatibilities
}
