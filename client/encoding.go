package client

import (
	"fmt"

	"github.com/coyove/s2db/s2"
)

func DD(prefix, data []byte) []byte {
	if len(prefix) > 127 {
		panic(fmt.Sprintf("distinct prefix %q exceeds 127 bytes", prefix))
	}
	return append(append([]byte{0, byte(len(prefix))}, prefix...), data...)
}

func SplitDD(p s2.Pair) ([]byte, []byte) {
	v := p.Data
	if len(p.Data) >= 2 && p.Data[0] == 0 {
		if 2+int(v[1]) <= len(v) {
			return v[2 : 2+v[1]], v[2+v[1]:]
		}
	}
	return nil, v
}
