package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"
)

const (
	MaxScore = 1 << 53
	MinScore = -MaxScore
)

func checkScore(s float64) error {
	if s > (MaxScore) || s < (MinScore) {
		return fmt.Errorf("score out of range: %d - %d", MinScore, MaxScore)
	}
	return nil
}

func intToBytes(v int64) []byte {
	tmp := [8]byte{}
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	return tmp[:]
}

func bytesToInt(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

func bytesToFloat(b []byte) float64 {
	f := math.Float64frombits(binary.BigEndian.Uint64(b[8:]))
	i := int64(binary.BigEndian.Uint64(b[:8])) - 1 - math.MaxInt64
	return float64(i) + f
}

func floatToBytes(v float64) []byte {
	i, f := math.Modf(v)
	x := math.Float64bits(f)
	tmp := [16]byte{}
	binary.BigEndian.PutUint64(tmp[:8], uint64(int64(i)+math.MaxInt64+1))
	binary.BigEndian.PutUint64(tmp[8:], x)
	return tmp[:]
}

func floatBytesStep(buf []byte, s int64) []byte {
	v := binary.BigEndian.Uint64(buf[8:])
	binary.BigEndian.PutUint64(buf[8:], uint64(int64(v)+s))
	return buf
}

func bytesStep(buf []byte, s int64) []byte {
	for i := len(buf) - 1; i >= 0; i-- {
		b := buf[i]
		if b == 255 && s == 1 {
			buf[i] = 0
		} else if b == 0 && s == -1 {
			buf[i] = 255
		} else {
			buf[i] = byte(int64(b) + s)
			return buf
		}
	}
	panic("no more step for buffer")
}

func intToBytesString(v int64) string {
	x := intToBytes(v)
	return *(*string)(unsafe.Pointer(&x))
}

func hashStr(s string) (h uint64) {
	h = 5381
	for i := 0; i < len(s); i++ {
		h = h*33 + uint64(s[i])
	}
	return h
}

func atof(a string) float64 {
	i, _ := strconv.ParseFloat(a, 64)
	return i
}

func reversePairs(in []Pair) []Pair {
	for i := 0; i < len(in)/2; i++ {
		j := len(in) - 1 - i
		in[i], in[j] = in[j], in[i]
	}
	return in
}

type RangeLimit struct {
	Value     string
	Float     float64
	Inclusive bool
}

type RangeOptions struct {
	OffsetStart int
	OffsetEnd   int
	Delete      bool
	CountOnly   bool
}

func (r RangeLimit) fromString(v string) RangeLimit {
	r.Value = v
	if strings.HasPrefix(v, "[") {
		r.Value = r.Value[1:]
		r.Inclusive = true
	} else if strings.HasPrefix(v, "(") {
		r.Value = r.Value[1:]
		r.Inclusive = false
	} else if v == "+" {
		r.Value = "\xff"
	} else if v == "-" {
		r.Value = ""
	}
	return r
}

func (r RangeLimit) fromFloatString(v string) RangeLimit {
	if strings.HasPrefix(v, "[") {
		r.Float = atof(v[1:])
		r.Inclusive = true
	} else if strings.HasPrefix(v, "(") {
		r.Float = atof(v[1:])
		r.Inclusive = false
	} else if v == "+inf" {
		r.Float = MaxScore
	} else if v == "-inf" {
		r.Float = MinScore
	} else {
		r.Float = atof(v)
	}
	return r
}

var (
	MinScoreRange = RangeLimit{Float: MinScore, Inclusive: true}
	MaxScoreRange = RangeLimit{Float: MaxScore, Inclusive: true}
)
