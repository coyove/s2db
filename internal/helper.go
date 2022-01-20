package internal

import (
	"crypto/rand"
	"encoding/hex"
	"runtime/debug"

	"github.com/sirupsen/logrus"
)

func UUID() string {
	buf := make([]byte, 16)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}

func HashStr2(s string) (h [2]uint64) {
	h = [2]uint64{0, 5381}
	for i := 0; i < len(s); i++ {
		old := h[1]
		h[1] = h[1]*33 + uint64(s[i])
		if h[1] < old {
			h[0]++
		}
	}
	return h
}

func HashStr(s string) (h uint64) {
	h = 5381
	for i := 0; i < len(s); i++ {
		h = h*33 + uint64(s[i])
	}
	return h
}

func Recover() {
	if r := recover(); r != nil {
		logrus.Error("fatal: ", r, " ", string(debug.Stack()))
	}
}

func SizeBytes(in [][]byte) int {
	sz := 1
	for _, p := range in {
		sz += len(p)
	}
	return sz
}

func SizePairs(in []Pair) int {
	sz := 1
	for _, p := range in {
		sz += len(p.Member) + 8 + len(p.Data)
	}
	return sz
}
