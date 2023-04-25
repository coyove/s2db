package s2pkg

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/gogo/protobuf/proto"
)

type Logs struct {
	Logs    []*Log `protobuf:"bytes,1,rep,name=logs"`
	PrevSig uint32 `protobuf:"varint,2,opt,name=prevsig"`
}

func (doc *Logs) Reset() {
	*doc = Logs{}
}

func (doc *Logs) String() string {
	return proto.CompactTextString(doc)
}

func (doc *Logs) MarshalBytes() []byte {
	buf, err := proto.Marshal(doc)
	PanicErr(err)
	return buf
}

func (doc *Logs) UnmarshalBytes(buf []byte) error {
	return proto.Unmarshal(buf, doc)
}

func (*Logs) ProtoMessage() {}

type Log struct {
	Cmd    int64    `protobuf:"varint,3,opt,name=cmd"`
	Keys   [][]byte `protobuf:"bytes,2,rep,name=k"`
	Values [][]byte `protobuf:"bytes,1,rep,name=v"`
}

func (doc *Log) Reset() {
	*doc = Log{}
}

func (doc *Log) String() string {
	return proto.CompactTextString(doc)
}

func (doc *Log) MarshalAppend(buf []byte) []byte {
	var info proto.InternalMessageInfo
	buf, err := info.Marshal(buf, doc, false)
	PanicErr(err)
	return buf
}

func (doc *Log) UnmarshalBytes(buf []byte) error {
	return proto.Unmarshal(buf, doc)
}

func (*Log) ProtoMessage() {}

type Pair struct {
	Member   string  `protobuf:"bytes,1,opt,name=member"`
	Score    float64 `protobuf:"fixed64,2,opt,name=score"`
	Data     []byte  `protobuf:"bytes,3,opt,name=data"`
	Children *[]Pair
}

type PairHeap struct {
	Desc        bool
	MemberOrder bool
	DataOrder   bool
	Pairs       []Pair
}

func (h *PairHeap) Len() int {
	return len(h.Pairs)
}

func (h *PairHeap) Less(i, j int) bool {
	if h.DataOrder {
		if h.Desc {
			return bytes.Compare(h.Pairs[i].Data, h.Pairs[j].Data) == 1
		}
		return bytes.Compare(h.Pairs[i].Data, h.Pairs[j].Data) == -1
	}
	if h.MemberOrder {
		if h.Desc {
			return h.Pairs[i].Member > h.Pairs[j].Member
		}
		return h.Pairs[i].Member < h.Pairs[j].Member
	}
	if h.Desc {
		return h.Pairs[i].Score > h.Pairs[j].Score
	}
	return h.Pairs[i].Score < h.Pairs[j].Score
}

func (h *PairHeap) Swap(i, j int) {
	h.Pairs[i], h.Pairs[j] = h.Pairs[j], h.Pairs[i]
}

func (h *PairHeap) Push(x interface{}) {
	h.Pairs = append(h.Pairs, x.(Pair))
}

func (h *PairHeap) Pop() interface{} {
	old := h.Pairs
	n := len(old)
	x := old[n-1]
	h.Pairs = old[0 : n-1]
	return x
}

func (h *PairHeap) ToPairs(n int, reversed bool) (p []Pair) {
	for len(p) < n && h.Len() > 0 {
		p = append(p, heap.Pop(h).(Pair))
	}
	if reversed {
		for i := 0; i < len(p)/2; i++ {
			j := len(p) - i - 1
			p[i], p[j] = p[j], p[i]
		}
	}
	return
}

func PairFromSKVCursor(c *pebble.Iterator) (key string, p Pair) {
	k := c.Key()[8:]
	idx := bytes.IndexByte(k, 0)
	key = string(k[:idx])
	p.Score = BytesToFloat(k[idx+1 : idx+9])
	p.Member = string(k[idx+9:])
	p.Data = Bytes(c.Value())
	return
}

type SortedPairArray struct {
	Limit int
	p     []Pair
}

func (spa *SortedPairArray) Add(p Pair) int {
	idx := sort.Search(len(spa.p), func(i int) bool {
		return (spa.p)[i].Score <= p.Score
	})
	spa.p = append(spa.p, Pair{})
	copy(spa.p[idx+1:], spa.p[idx:])
	spa.p[idx] = p

	if len(spa.p) > spa.Limit {
		spa.p = spa.p[:spa.Limit]
	}
	return idx
}

func (spa *SortedPairArray) ToPairs(count int) []Pair {
	if len(spa.p) <= count {
		return spa.p
	}
	return spa.p[:count]
}

func (spa *SortedPairArray) Len() int {
	return len(spa.p)
}

func (spa *SortedPairArray) MinScore() float64 {
	if len(spa.p) > 0 {
		return spa.p[len(spa.p)-1].Score
	}
	return math.Inf(-1)
}

type FixedRange [][2][16]byte

func NewFixedRange(lower, upper []byte) (a FixedRange) {
	a.Append(lower, upper)
	return
}

func (f *FixedRange) Append(lower, upper []byte) {
	_, _ = lower[16-1], upper[16-1]
	if bytes.Compare(upper, lower) < 0 {
		lower, upper = upper, lower
	}
	var p [2][16]byte
	copy(p[0][:], lower)
	copy(p[1][:], upper)
	*f = append(*f, p)
}

func (f *FixedRange) Sort() {
	if len(*f) > 1 {
		sort.Slice(*f, func(i, j int) bool {
			return bytes.Compare((*f)[i][0][:], (*f)[j][0][:]) < 0
		})
	}
	for len(*f) > 0 {
		if (*f)[0][0] == [16]byte{} && (*f)[0][1] == [16]byte{} {
			*f = (*f)[1:]
		} else {
			break
		}
	}
	for i := len(*f) - 1; i > 0; i-- {
		cur := (*f)[i]
		prev := &(*f)[i-1]
		if bytes.Compare(cur[0][:], prev[1][:]) <= 0 {
			prev[1] = cur[1]
			*f = append((*f)[:i], (*f)[i+1:]...)
		}
	}
}

func (f *FixedRange) Merge(f2 FixedRange) {
	*f = append(*f, f2...)
	f.Sort()
	if len(*f) > 4 {
		*f = append((*f)[:2], (*f)[len(*f)-2:]...)
	}
}

func (f FixedRange) containsTuple(lower, upper [16]byte) bool {
	for _, p := range f {
		if bytes.Compare(p[0][:], lower[:]) <= 0 &&
			bytes.Compare(lower[:], p[1][:]) <= 0 &&
			bytes.Compare(p[0][:], upper[:]) <= 0 &&
			bytes.Compare(upper[:], p[1][:]) <= 0 {
			return true
		}
	}
	return false
}

func (f FixedRange) Contains(f2 FixedRange) bool {
	for _, p := range f2 {
		if !f.containsTuple(p[0], p[1]) {
			return false
		}
	}
	return true
}

func (f FixedRange) String() string {
	buf := bytes.NewBufferString("(FixedRange")
	for _, p := range f {
		fmt.Fprintf(buf, " %032x-%032x", p[0], p[1])
	}
	buf.WriteString(")")
	return buf.String()
}

func (f FixedRange) Bytes() []byte {
	buf := bytes.Buffer{}
	for _, p := range f {
		buf.Write(p[0][:])
		buf.Write(p[1][:])
	}
	return buf.Bytes()
}
