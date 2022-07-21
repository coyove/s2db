package ranges

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/bitmap"
	"github.com/coyove/s2db/s2pkg"
)

var (
	MinScoreRange = Limit{Float: math.Inf(-1), Inclusive: true}
	MaxScoreRange = Limit{Float: math.Inf(1), Inclusive: true}

	ZSetKeyScoreFullRange = &pebble.IterOptions{
		LowerBound: []byte("zsetks__"),
		UpperBound: []byte("zsetks_\xff"),
	}
	ZSetScoreKeyValueFullRange = &pebble.IterOptions{
		LowerBound: []byte("zsetskv_"),
		UpperBound: []byte("zsetskv\xff"),
	}
)

type Limit struct {
	Float     float64
	Inclusive bool
	LexValue  string
	LexPlus   bool
}

type Options struct {
	OffsetStart int      // Z[REV]RANGE *start* ...
	OffsetEnd   int      // Z[REV]RANGE ... *end*
	Limit       int      // ... LIMIT 0 *limit*
	ILimit      *float64 //
	WithData    bool     // return attached data
	Rev         bool     // reversed range
	Match       string   // BYLEX: match member name, BYSCORE: match member name and its data
	DeleteLog   []byte   // if provided, returned pairs will be deleted first
	Append      func(*Result, s2pkg.Pair) error
	FanoutStart Limit
	FanoutEnd   Limit
}

var ErrAppendSafeExit = fmt.Errorf("exit")
var HardLimit = 65535
var HardMatchTimeout = time.Second * 30

type Result struct {
	Pairs []s2pkg.Pair
	Count int
	Bloom *bitmap.Bloom
}

func (rr *Result) BFToFakePair() {
	if rr.Bloom == nil {
		return
	}
	rr.Pairs = append(rr.Pairs, s2pkg.Pair{
		Member: "mbrbitmap",
		Score:  float64(rr.Count),
		Data:   rr.Bloom.MarshalBinary(),
	})
}

func DefaultAppend(r *Result, p s2pkg.Pair) error {
	r.Pairs = append(r.Pairs, p)
	r.Count++
	return nil
}

func Lex(v string) (r Limit) {
	r.LexValue = v
	r.Inclusive = true
	if strings.HasPrefix(v, "[") {
		r.LexValue = r.LexValue[1:]
	} else if strings.HasPrefix(v, "(") {
		r.LexValue = r.LexValue[1:]
		r.Inclusive = false
	} else if v == "+" {
		r.LexValue = "\xff"
		r.LexPlus = true
	} else if v == "-" {
		r.LexValue = ""
	}
	return r
}

func Score(v string) (r Limit) {
	r.Inclusive = true
	if strings.HasPrefix(v, "[") {
		r.Float = s2pkg.MustParseFloat(v[1:])
	} else if strings.HasPrefix(v, "(") {
		r.Float = s2pkg.MustParseFloat(v[1:])
		r.Inclusive = false
	} else {
		r.Float = s2pkg.MustParseFloat(v)
	}
	return r
}

func (l Limit) ToScore() (buf []byte) {
	if !l.Inclusive {
		buf = append(buf, '(')
	}
	return strconv.AppendFloat(buf, l.Float, 'f', -1, 64)
}
