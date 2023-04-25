package s2pkg

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
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
	if p.C {
		return fmt.Sprintf("[[%s:%q]]", p.IDHex(), p.Data)
	}
	return fmt.Sprintf("<%s:%q>", p.IDHex(), p.Data)
}

func ConvertPairsToBulks(p []Pair) (a [][]byte) {
	for _, p := range p {
		i := p.IDHex()
		a = append(a, i, []byte(strconv.FormatFloat(p.UnixSec(), 'f', -1, 64)), p.Data)
	}
	return
}

func ConvertBulksToPairs(a []string) (p []Pair) {
	for i := 0; i < len(a); i += 3 {
		var x Pair
		x.ID, _ = hex.DecodeString(a[i])
		x.Data = []byte(a[i+2])
		p = append(p, x)
	}
	return
}

func TrimPairs(p []Pair) (t []Pair) {
	if len(p) <= 2 {
		return nil
	}

	head := p[0].UnixNano() / 1e9
	tail := p[len(p)-1].UnixNano() / 1e9

	for _, p := range p {
		sec := p.UnixNano() / 1e9
		if sec != head && sec != tail {
			t = append(t, p)
		}
	}
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
