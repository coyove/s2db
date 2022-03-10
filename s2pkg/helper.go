package s2pkg

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"io"
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
		if s[i] == '\t' {
			h, _ = strconv.ParseUint(s[i+1:], 10, 64)
			return h
		}
		h = h*33 + uint64(s[i])
	}
	return h
}

func Recover(f func()) {
	if r := recover(); r != nil {
		logrus.Error("fatal: ", r, " ", string(debug.Stack()))
		if f != nil {
			f()
		}
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

func MatchBinary(pattern string, buf []byte) bool {
	return Match(pattern, *(*string)(unsafe.Pointer(&buf)))
}

func Match(pattern string, text string) bool {
	rp, rest := ExtractHeadCirc(pattern)
	if rp != "" {
		if m, err := filepath.Match(rp, text); err != nil {
			logrus.Errorf("Match: invalid pattern: `%s` %v", rp, err)
		} else if m {
			return false
		}
		return Match(rest, text)
	}
	m, err := filepath.Match(rest, text)
	if err != nil {
		logrus.Errorf("Match: invalid pattern: `%s` %v", pattern, err)
	}
	return m
}

func ExtractAllHeadCirc(text string) ([]string, string) {
	var res []string
	for {
		rp, rest := ExtractHeadCirc(text)
		if rp != "" {
			res = append(res, rp)
			text = rest
		} else {
			return res, rest
		}
	}
}

func ExtractHeadCirc(text string) (string, string) {
	if strings.HasPrefix(text, "^[") {
		eol := strings.Index(text, "\n")
		if eol > 2 {
			line := strings.TrimSpace(text[2:eol])
			if strings.HasPrefix(line, "\"") && strings.HasSuffix(line, "\"") {
				rp, err := strconv.Unquote(line)
				if err != nil {
					logrus.Errorf("ExtractHeadCirc: invalid quoted string: `%s` %v", line, err)
				}
				line = rp
			}
			return line, text[eol+1:]
		}
	}
	if strings.HasPrefix(text, "\\^") {
		text = text[1:]
	}
	return "", text
}

const (
	IndexedBufferCap        = 10000
	IndexedBufferSafeMargin = 1000000
)

type IndexedBuffer struct {
	mu    sync.Mutex
	lower int64
	m     map[int64][]byte
}

func (b *IndexedBuffer) Add(id int64, buf []byte) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if id == 0 {
		return false
	}
	if _, exist := b.m[id]; exist {
		return false
	}
	if b.m == nil {
		b.m = map[int64][]byte{}
	}
	if b.lower == 0 {
		b.lower = id
	}
	if id < b.lower {
		if id < b.lower-IndexedBufferSafeMargin {
			return false
		}
		b.lower = id
	}
	b.m[id] = buf
	for len(b.m) > IndexedBufferCap {
		delete(b.m, b.lower)
		b.lower++
	}
	return true
}

func (b *IndexedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.m)
}

func (b *IndexedBuffer) Head() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lower
}

func (b *IndexedBuffer) DeleteUntil(upper int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.lower == 0 {
		return
	}
	for i := b.lower; i <= upper; i++ {
		delete(b.m, i)
		b.lower = i + 1
	}
}

func (b *IndexedBuffer) GetRange(start int64, max int) (data [][]byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := start; len(data) < max; i++ {
		buf, ok := b.m[i]
		if !ok {
			break
		}
		data = append(data, buf)
	}
	return data
}

func CopyCrc32(w io.Writer, r io.Reader, f func(int)) (total int, ok bool, err error) {
	var last []byte
	h, p := crc32.NewIEEE(), make([]byte, 32*1024)
	for {
		n, err := r.Read(p)
		if n > 0 {
			last = append(last, p[:n]...)
			if len(last) >= 4 {
				x := len(last) - 4
				h.Write(last[:x])

				ew, err := w.Write(last[:x])
				if ew != x {
					return total + ew, false, io.ErrShortWrite
				}
				if err != nil {
					return total + ew, false, err
				}

				copy(last, last[x:])
				last = last[:4]
			}
			total += n
			if f != nil {
				f(total)
			}
		}
		if err != nil {
			if len(last) < 4 {
				return total, false, io.ErrShortBuffer
			}
			if len(last) > 4 {
				panic("CopyCrc32: shouldn't happen")
			}
			ok = bytes.Equal(h.Sum(nil), last)
			if err == io.EOF {
				err = nil
			}
			return total, ok, err
		}
	}
}
