package wire

import (
	"strconv"
)

var (
	newLine  = []byte{'\r', '\n'}
	nilBulk  = []byte{'$', '-', '1', '\r', '\n'}
	nilArray = []byte{'*', '-', '1', '\r', '\n'}
)

func itob(val int64) []byte {
	return strconv.AppendInt(nil, val, 10)
}
