package main

import (
	"encoding/binary"
	"math"
	"unsafe"
)

func intToBytes(v int64) []byte {
	tmp := [8]byte{}
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	return tmp[:]
}

func bytesToInt(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

func bytesToFloat(b []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}

func floatToBytes(v float64) []byte {
	x := math.Float64bits(v)
	tmp := [8]byte{}
	binary.BigEndian.PutUint64(tmp[:], x)
	return tmp[:]
}

func floatToBytesString(v float64) string {
	x := floatToBytes(v)
	return *(*string)(unsafe.Pointer(&x))
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
