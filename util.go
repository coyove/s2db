package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/secmask/go-redisproto"
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
	h := binary.BigEndian.Uint64(buf[:8])
	if s == -1 && v == 0 {
		binary.BigEndian.PutUint64(buf[:8], uint64(int64(h)-1))
		binary.BigEndian.PutUint64(buf[8:], math.MaxUint64)
	} else if s == 1 && v == math.MaxUint64 {
		binary.BigEndian.PutUint64(buf[:8], uint64(int64(h)+1))
		binary.BigEndian.PutUint64(buf[8:], 0)
	} else {
		binary.BigEndian.PutUint64(buf[8:], uint64(int64(v)+s))
	}
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

func hashCommands(in *redisproto.Command) (h [2]uint64) {
	h = [2]uint64{0, 5381}
	for _, buf := range *(*[][]byte)(unsafe.Pointer(in)) {
		for _, b := range buf {
			old := h[1]
			h[1] = h[1]*33 + uint64(b)
			if h[1] < old {
				h[0]++
			}
		}
	}
	return h
}

func atof(a string) float64 {
	if a == "+inf" {
		return MaxScore
	}
	if a == "-inf" {
		return MinScore
	}
	i, _ := strconv.ParseFloat(a, 64)
	return i
}

func ftoa(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func atoi(a string) int {
	i, _ := strconv.Atoi(a)
	return i
}

func restCommandsToKeys(i int, command *redisproto.Command) []string {
	keys := []string{}
	for ; i < command.ArgCount(); i++ {
		keys = append(keys, string(command.Get(i)))
	}
	return keys
}

func reversePairs(in []Pair) []Pair {
	for i := 0; i < len(in)/2; i++ {
		j := len(in) - 1 - i
		in[i], in[j] = in[j], in[i]
	}
	return in
}

func writePairs(in []Pair, w *redisproto.Writer, command *redisproto.Command) error {
	withScores := bytes.Equal(bytes.ToUpper(command.Get(command.ArgCount()-1)), []byte("WITHSCORES"))
	data := make([]string, 0, len(in))
	for _, p := range in {
		data = append(data, p.Key)
		if withScores {
			data = append(data, strconv.FormatFloat(p.Score, 'f', -1, 64))
		}
	}
	return w.WriteBulkStrings(data)
}

func sizePairs(in []Pair) int {
	sz := 0
	for _, p := range in {
		sz += len(p.Key) + 8
	}
	return sz
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
	r.Inclusive = true
	if strings.HasPrefix(v, "[") {
		r.Value = r.Value[1:]
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
	r.Inclusive = true
	if strings.HasPrefix(v, "[") {
		r.Float = atof(v[1:])
	} else if v == "(+inf" {
		r.Float = MaxScore
		r.Inclusive = false
	} else if v == "(-inf" {
		r.Float = MinScore
		r.Inclusive = false
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
