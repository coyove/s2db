package wire

import (
	"strconv"
)

var (
	newLine = []byte{'\r', '\n'}
)

func itob(val int64) []byte {
	return strconv.AppendInt(nil, val, 10)
}
