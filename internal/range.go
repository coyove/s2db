package internal

import (
	"strings"

	"go.etcd.io/bbolt"
)

type RangeLimit struct {
	Value     string
	Float     float64
	Inclusive bool
	LexEnd    bool
}

type RangeOptions struct {
	Rev            bool
	OffsetStart    int
	OffsetEnd      int
	CountOnly      bool
	WithData       bool
	DeleteLog      []byte
	LexMatch       string
	ScoreMatchData string
	Limit          int
}

func NewRLFromString(v string) (r RangeLimit) {
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

func NewRLFromFloatString(v string) (r RangeLimit, err error) {
	r.Inclusive = true
	if strings.HasPrefix(v, "[") {
		r.Float, err = ParseFloat(v[1:])
	} else if strings.HasPrefix(v, "(") {
		r.Float, err = ParseFloat(v[1:])
		r.Inclusive = false
	} else {
		r.Float, err = ParseFloat(v)
	}
	return r, err
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
