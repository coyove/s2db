package wire

import (
	"strconv"
)

var (
	newLine  = []byte{'\r', '\n'}
	nilBulk  = []byte{'$', '-', '1', '\r', '\n'}
	nilArray = []byte{'*', '-', '1', '\r', '\n'}
)

func intToString(val int64) string {
	return strconv.FormatInt(val, 10)
}
