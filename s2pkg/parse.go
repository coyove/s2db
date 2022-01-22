package s2pkg

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/mmcloughlin/geohash"
)

func ParseFloat(a string) (float64, error) {
	if idx := strings.Index(a, ","); idx > 0 {
		a, b := a[:idx], a[idx+1:]
		long, err := strconv.ParseFloat(a, 64)
		if err != nil {
			return 0, err
		}
		lat, err := strconv.ParseFloat(b, 64)
		if err != nil {
			return 0, err
		}
		h := geohash.EncodeIntWithPrecision(lat, long, 52)
		return float64(h), nil
	}
	return strconv.ParseFloat(a, 64)
}

func ParseFloatBytes(a []byte) (float64, error) {
	return ParseFloat(*(*string)(unsafe.Pointer(&a)))
}

func MustParseFloatBytes(a []byte) float64 {
	f, err := ParseFloatBytes(a)
	PanicErr(err)
	return f
}

func FormatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func FormatFloatShort(f float64) string {
	if f < 0.01 {
		return strconv.FormatFloat(f, 'f', 3, 64)
	}
	return strconv.FormatFloat(f, 'f', 2, 64)
}

func FormatFloatBulk(f float64) []byte {
	if math.IsNaN(f) {
		return nil
	}
	return []byte(strconv.FormatFloat(f, 'f', -1, 64))
}

func ParseInt(a string) int {
	i, _ := strconv.Atoi(a)
	return i
}

func MustParseInt(a string) int {
	i, err := strconv.Atoi(a)
	if err != nil {
		panic("invalid integer: " + strconv.Quote(a))
	}
	return i
}

func MustParseInt64(a string) int64 {
	i, err := strconv.ParseInt(a, 10, 64)
	if err != nil {
		panic("invalid integer: " + strconv.Quote(a))
	}
	return i
}

func ParseUint64(a string) uint64 {
	i, _ := strconv.ParseUint(a, 10, 64)
	return i
}

func Uint64ToBytes(i uint64) []byte {
	v := [8]byte{}
	binary.BigEndian.PutUint64(v[:], i)
	return v[:]
}

func BytesToFloatZero(b []byte) float64 {
	if len(b) != 8 {
		return 0
	}
	return BytesToFloat(b)
}

func BytesToFloat(b []byte) float64 {
	x := binary.BigEndian.Uint64(b)
	if x>>63 == 1 {
		x = x << 1 >> 1
	} else {
		x = ^x
	}
	return math.Float64frombits(x)
}

func FloatToOrderedUint64(v float64) uint64 {
	x := math.Float64bits(v)
	if v >= 0 {
		x |= 1 << 63
	} else {
		x = ^x
	}
	return x
}

func FloatToBytes(v float64) []byte {
	tmp := [8]byte{}
	binary.BigEndian.PutUint64(tmp[:8], FloatToOrderedUint64(v))
	return tmp[:]
}
