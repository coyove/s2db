package s2pkg

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/mmcloughlin/geohash"
	"github.com/sirupsen/logrus"
)

func MustParseFloat(a string) float64 {
	if idx := strings.Index(a, ","); idx > 0 {
		a, b := a[:idx], a[idx+1:]
		long, err := strconv.ParseFloat(a, 64)
		PanicErr(err)
		lat, err := strconv.ParseFloat(b, 64)
		PanicErr(err)
		h := geohash.EncodeIntWithPrecision(lat, long, 52)
		return float64(h)
	}
	v, err := strconv.ParseFloat(a, 64)
	PanicErr(err)
	return v
}

func MustParseFloatBytes(a []byte) float64 {
	return MustParseFloat(*(*string)(unsafe.Pointer(&a)))
}

func FormatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func FormatFloatShort(f float64) string {
	if f != f {
		return "-.--"
	}
	if f < 0.01 && f > 0 {
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

func BytesToUint64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
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

func RemoveFile(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	return os.Remove(path)
}

type Locker struct {
	mu sync.Mutex
}

func (l *Locker) Unlock() {
	l.mu.Unlock()
}

func (l *Locker) Lock(waiting func()) {
	if *(*int32)(unsafe.Pointer(l)) != 0 && waiting != nil {
		waiting()
	}
	l.mu.Lock()
}

type LockBox struct {
	mu sync.Mutex
	v  interface{}
}

func (b *LockBox) Lock(v interface{}) (interface{}, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.v != nil {
		return b.v, false
	}
	b.v = v
	return v, true
}

func (b *LockBox) Unlock() {
	b.mu.Lock()
	b.v = nil
	b.mu.Unlock()
}

func PanicErr(err error) {
	if err != nil {
		panic(err)
	}
}

func Match(pattern string, text string) bool {
	if strings.HasPrefix(pattern, "^[") {
		for i, d := 2, 1; i < len(pattern); i++ {
			c, pc := pattern[i], pattern[i-1]
			if c == ']' && pc != '\\' {
				d--
				if d == 0 {
					if m, err := filepath.Match(pattern[2:i], text); err != nil {
						logrus.Error("Match: invalid pattern:", err)
					} else if m {
						return false
					}
					return Match(pattern[i+1:], text)
				}
			}
			if c == '[' && pc != '\\' {
				d++
			}
		}
	}
	if strings.HasPrefix(pattern, "\\^") {
		pattern = pattern[1:]
	}
	m, err := filepath.Match(pattern, text)
	if err != nil {
		logrus.Error("Match: invalid pattern:", err)
	}
	return m
}
