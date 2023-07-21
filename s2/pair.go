package s2

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"

	"github.com/coyove/sdss/future"
	"github.com/sirupsen/logrus"
)

const (
	PairCmdAppend         = 1
	PairCmdAppendNoExpire = 2
	PairCmdHSet           = 2
)

type Pair struct {
	ID   []byte
	Data []byte
	Con  bool
	All  bool
}

func (p Pair) UnixMilli() int64 {
	ns := p.UnixNano()
	ts := ns / 1e6 / 10 * 10
	var x int64
	if p.Con {
		x |= 1
	}
	if p.All {
		x |= 2
	}
	ts += x
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

func (p Pair) DistinctPrefix() []byte {
	dpLen := p.ID[13]
	if ok := dpLen > 0 && p.Cmd()&PairCmdAppend > 0; !ok {
		return nil
	}
	if int(dpLen) > len(p.Data) {
		logrus.Errorf("fatal distinct prefix: %v %q", p.ID, p.Data)
		return nil
	}
	return p.Data[:dpLen]
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

func (p Pair) String() (s string) {
	id := fmt.Sprintf("%016x_%016x", p.ID[:8], p.ID[8:16])
	if p.Con {
		s = fmt.Sprintf("[%s:%q]", id, p.Data)
	} else {
		s = fmt.Sprintf("<%s:%q>", id, p.Data)
	}
	if p.All {
		s = "[" + s + "]"
	}
	return
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

func PackIDs(p []Pair) (x []byte) {
	if len(p) == 0 {
		return nil
	}
	sort.Slice(p, func(i, j int) bool { return bytes.Compare(p[i].ID, p[j].ID) < 0 })
	x = binary.BigEndian.AppendUint64(x, binary.BigEndian.Uint64(p[0].ID[:8]))
	for i := 1; i < len(p); i++ {
		pi := binary.BigEndian.Uint64(p[i].ID[:8])
		pj := binary.BigEndian.Uint64(p[i-1].ID[:8])
		x = binary.AppendUvarint(x, pi-pj)
	}
	return
}

func UnpackIDs(x []byte) func(id []byte) bool {
	if len(x) == 0 {
		return nil
	}
	ids := []uint64{binary.BigEndian.Uint64(x)}
	x = x[8:]
	for len(x) > 0 {
		v, n := binary.Uvarint(x)
		x = x[n:]
		ids = append(ids, ids[len(ids)-1]+v)
	}
	return func(id []byte) bool {
		y := binary.BigEndian.Uint64(id)
		idx := sort.Search(len(ids), func(i int) bool { return ids[i] >= y })
		return idx < len(ids) && ids[idx] == y
	}
}

func AllPairsConsolidated(p []Pair) bool {
	if len(p) == 0 {
		return false
	}
	for _, p := range p {
		if !p.Con {
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

func CreatePairDeduper() func([]Pair) []Pair {
	dedup := map[[20]byte]future.Future{}
	return func(p []Pair) (res []Pair) {
		res = make([]Pair, 0, len(p))
		for i := range p {
			dpLen := int(p[i].ID[13])
			if dpLen == 0 {
				res = append(res, p[i])
				continue
			}
			h := sha1.Sum(p[i].Data[:dpLen])
			if p[i].Future() <= dedup[h] {
				continue
			}
			dedup[h] = p[i].Future()
			res = append(res, p[i])
		}
		return
	}
}
