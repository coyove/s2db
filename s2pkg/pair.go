package s2pkg

import (
	"bytes"
	"container/heap"

	"github.com/cockroachdb/pebble"
	"github.com/gogo/protobuf/proto"
)

type Logs struct {
	Logs    []*Log `protobuf:"bytes,1,rep,name=logs"`
	PrevSig uint32 `protobuf:"varint,2,opt,name=prevsig"`
}

type Log struct {
	Id   uint64 `protobuf:"fixed64,1,opt,name=id"`
	Data []byte `protobuf:"bytes,2,opt,name=data"`
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

type BytesArray struct {
	Data [][]byte `protobuf:"bytes,1,rep,name=data"`
}

func (doc *BytesArray) Reset() {
	*doc = BytesArray{}
}

func (doc *BytesArray) String() string {
	return proto.CompactTextString(doc)
}

func (doc *BytesArray) MarshalAppend(buf []byte) []byte {
	x, err := proto.Marshal(doc)
	PanicErr(err)
	return append(buf, x...)
}

func (doc *BytesArray) UnmarshalBytes(buf []byte) error {
	return proto.Unmarshal(buf, doc)
}

func (*BytesArray) ProtoMessage() {}

type Pair struct {
	Member string  `protobuf:"bytes,1,opt,name=member"`
	Score  float64 `protobuf:"fixed64,2,opt,name=score"`
	Data   []byte  `protobuf:"bytes,3,opt,name=data"`
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
