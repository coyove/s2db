package s2

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"unsafe"

	"github.com/coyove/sdss/future"
)

const (
	PairCmdAppend = 1
	PairCmdAmend  = 2
)

type Pair struct {
	ID   []byte
	Data []byte
	C    bool
}

func (p Pair) UnixMilli() int64 {
	ns := p.UnixNano()
	ts := ns / 1e6 / 10 * 10
	if p.C {
		ts += 1
	}
	return ts
}

func (p Pair) UnixMilliBytes() []byte {
	return strconv.AppendInt(nil, p.UnixMilli(), 10)
}

func (p Pair) IDHex() []byte {
	k := p.ID
	k0 := make([]byte, len(k)*2)
	hex.Encode(k0, k)
	return k0
}

func (p Pair) UnixNano() int64 {
	return int64(binary.BigEndian.Uint64(p.ID))
}

func (p Pair) Future() future.Future {
	return future.Future(p.UnixNano())
}

func (p Pair) Less(p2 Pair) bool {
	return bytes.Compare(p.ID, p2.ID) < 0
}

func (p Pair) Equal(p2 Pair) bool {
	return bytes.Equal(p.ID, p2.ID)
}

func (p Pair) Cmd() int {
	return int(p.ID[14] & 0xf)
}

func (p Pair) DataForDistinct() string {
	v := *(*string)(unsafe.Pointer(&p.Data))
	if len(p.Data) >= 2 && p.Data[0] == 0 {
		return v[2 : 2+v[1]]
	}
	return v
}

func (p Pair) String() string {
	id := fmt.Sprintf("%016x_%016x", p.ID[:8], p.ID[8:16])
	if p.Cmd() == PairCmdAmend {
		id += "_amend"
	}
	if p.C {
		return fmt.Sprintf("[[%s:%q]]", id, p.Data)
	}
	return fmt.Sprintf("<%s:%q>", id, p.Data)
}

func TrimPairsForConsolidation(p []Pair, left, right bool) (t []Pair) {
	if len(p) == 0 {
		return nil
	}

	head := p[0].UnixNano() / future.Block
	tail := p[len(p)-1].UnixNano() / future.Block

	for _, p := range p {
		sec := p.UnixNano() / future.Block
		if sec == head && left {
			continue
		}
		if sec == tail && right {
			continue
		}
		t = append(t, p)
	}

	sort.Slice(t, func(i, j int) bool { return t[i].Less(t[j]) })
	return t
}

func AllPairsConsolidated(p []Pair) bool {
	if len(p) == 0 {
		return false
	}
	for _, p := range p {
		if !p.C {
			return false
		}
	}
	return true
}

func ConvertFutureTo16B(f future.Future) (idx [16]byte) {
	binary.BigEndian.PutUint64(idx[:], uint64(f))
	return
}

func Convert16BToFuture(idx []byte) (f future.Future) {
	_ = idx[15]
	v := binary.BigEndian.Uint64(idx)
	return future.Future(v)
}