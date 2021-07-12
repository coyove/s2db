package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"unsafe"
)

const (
	MaxScore    = 1 << 53
	MinScore    = -MaxScore
	MaxScoreStr = "9007199254740992"
	MinScoreStr = "-9007199254740992"
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

func makeZSetNameKey(name, key string) []byte {
	return []byte("zset.ns." + name + "." + key)
}

func parseZSetNameKey(in []byte) (name, key string) {
	idx := bytes.LastIndexByte(in, '.')
	key = string(in[idx+1:])
	in = in[:idx]
	idx = bytes.LastIndexByte(in, '.')
	name = string(in[idx+1:])
	return
}

func makeZSetScoreKey(name, key string, score float64) []byte {
	return makeZSetScoreKey2(name, key, floatToBytes(score))
}

func makeZSetScoreKey2(name, key string, score []byte) []byte {
	return []byte("zset.rev." + name + "." + *(*string)(unsafe.Pointer(&score)) + "." + key)
}

func parseZSetScoreKey(in []byte) (name, key string, score float64) {
	idx := bytes.LastIndexByte(in, '.')
	key = string(in[idx+1:])
	in = in[:idx]
	score = bytesToFloat(in[len(in)-16:])
	in = in[:len(in)-16-1]
	idx = bytes.LastIndexByte(in, '.')
	name = string(in[idx+1:])
	return
}

func atof(a string) float64 {
	i, _ := strconv.ParseFloat(a, 64)
	return i
}
