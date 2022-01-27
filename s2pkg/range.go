package s2pkg

import (
	"strings"

	"go.etcd.io/bbolt"
)

type Pair struct {
	Member string
	Score  float64
	Data   []byte
}

type PairHeap []Pair

func (h PairHeap) Len() int           { return len(h) }
func (h PairHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h PairHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *PairHeap) Push(x interface{}) {
	*h = append(*h, x.(Pair))
}

func (h *PairHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
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

func (o *RangeOptions) TranslateOffset(keyName string, bk *bbolt.Bucket) {
	if o.OffsetStart < 0 || o.OffsetEnd < 0 {
		n := bk.KeyN()
		if o.OffsetStart < 0 {
			o.OffsetStart += n
		}
		if o.OffsetEnd < 0 {
			o.OffsetEnd += n
		}
	}
}

var RangeHardLimit = 65535
