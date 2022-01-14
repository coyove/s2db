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
