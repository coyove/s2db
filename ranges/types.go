package ranges

import (
	"fmt"
	"strings"

	"github.com/coyove/s2db/s2pkg"
)

type Limit struct {
	LexValue  string
	Float     float64
	Inclusive bool
	LexPlus   bool
}

type Options struct {
	OffsetStart int    // Z[REV]RANGE *start* ...
	OffsetEnd   int    // Z[REV]RANGE ... *end*
	Limit       int    // ... LIMIT 0 *limit*
	WithData    bool   // return attached data
	Rev         bool   // reversed range
	Match       string // BYLEX: match member name, BYSCORE: match member name and its data
	DeleteLog   []byte // if provided, returned pairs will be deleted first
	Append      func(*Result, s2pkg.Pair) error
}

var ErrAppendSafeExit = fmt.Errorf("exit")
var HardLimit = 65535

type Result struct {
	Pairs []s2pkg.Pair
	Count int
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
