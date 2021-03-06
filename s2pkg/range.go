package s2pkg

import (
	"bytes"
	"strings"
)

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

type RangeLimit struct {
	Value     string
	Float     float64
	Inclusive bool
	LexEnd    bool
}

type RangeOptions struct {
	OffsetStart    int    // used by Z[REV]RANGE
	OffsetEnd      int    // used by Z[REV]RANGE
	Limit          int    // upper bound in: LIMIT 0 limit
	WithData       bool   // return attached data
	Rev            bool   // reversed range
	LexMatch       string // match member
	ScoreMatchData string // only available in ZRANGEBYSCORE
	DeleteLog      []byte // used by ZREM...
	Append         func(pairs *[]Pair, p Pair) bool
}

func DefaultRangeAppend(pairs *[]Pair, p Pair) bool {
	*pairs = append(*pairs, p)
	return true
}

func NewLexRL(v string) (r RangeLimit) {
	r.Value = v
	r.Inclusive = true
	if strings.HasPrefix(v, "[") {
		r.Value = r.Value[1:]
	} else if strings.HasPrefix(v, "(") {
		r.Value = r.Value[1:]
		r.Inclusive = false
	} else if v == "+" {
		r.Value = "\xff"
		r.LexEnd = true
	} else if v == "-" {
		r.Value = ""
	}
	return r
}

func NewScoreRL(v string) (r RangeLimit) {
	r.Inclusive = true
	if strings.HasPrefix(v, "[") {
		r.Float = MustParseFloat(v[1:])
	} else if strings.HasPrefix(v, "(") {
		r.Float = MustParseFloat(v[1:])
		r.Inclusive = false
	} else {
		r.Float = MustParseFloat(v)
	}
	return r
}

func (o *RangeOptions) TranslateOffset(keyName string, sizeof func() int) {
	if o.OffsetStart < 0 || o.OffsetEnd < 0 {
		n := sizeof()
		if o.OffsetStart < 0 {
			o.OffsetStart += n
		}
		if o.OffsetEnd < 0 {
			o.OffsetEnd += n
		}
	}
}

var RangeHardLimit = 65535
