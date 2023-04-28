package s2pkg

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/coyove/sdss/future"
)

type Pair struct {
	ID   []byte
	Data []byte
	C    bool
}

func (p Pair) UnixSec() float64 {
	ns := p.UnixNano()
	ts := float64(ns/1e8) / 10
	if p.C {
		ts += 0.01
	}
	return ts
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

func (p Pair) String() string {
	id := fmt.Sprintf("%016x_%016x", p.ID[:8], p.ID[8:16])
	if p.C {
		return fmt.Sprintf("[[%s:%q]]", id, p.Data)
	}
	return fmt.Sprintf("<%s:%q>", id, p.Data)
}

func ConvertPairsToBulks(p []Pair) (a [][]byte) {
	for _, p := range p {
		i := p.IDHex()
		a = append(a, i, []byte(strconv.FormatFloat(p.UnixSec(), 'f', -1, 64)), p.Data)
	}
	return
}

func ConvertPairsToBulksNoTimestamp(p []Pair) (a [][]byte) {
	x := []byte("0")
	for _, p := range p {
		i := p.IDHex()
		a = append(a, i, x, p.Data)
	}
	return
}

func ConvertBulksToPairs(a []string) (p []Pair) {
	for i := 0; i < len(a); i += 3 {
		var x Pair
		x.ID, _ = hex.DecodeString(a[i])
		x.Data = []byte(a[i+2])
		ts100 := int64(math.Round(MustParseFloat(a[i+1]) * 100))
		x.C = ts100%2 == 1
		p = append(p, x)
	}
	return
}

func TrimPairs(p []Pair) (t []Pair) {
	if len(p) <= 2 {
		return nil
	}

	head := p[0].UnixNano() / future.Block
	tail := p[len(p)-1].UnixNano() / future.Block

	for _, p := range p {
		sec := p.UnixNano() / future.Block
		if sec != head && sec != tail {
			t = append(t, p)
		}
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
