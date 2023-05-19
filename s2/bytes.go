package s2

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"io"
	"math"
	"net/http"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"unsafe"

	"github.com/pierrec/lz4/v4"
	"github.com/sirupsen/logrus"
)

func MustParseFloat(a string) float64 {
	if strings.HasPrefix(a, "0x") {
		v, err := strconv.ParseUint(a[2:], 16, 64)
		PanicErr(err)
		return math.Float64frombits(v)
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

func ParseFloat(a string) float64 {
	i, err := strconv.ParseFloat(a, 64)
	if err != nil {
		return math.NaN()
	}
	return i
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

func OrderedUint64ToFloat(x uint64) float64 {
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

func HashBytes(s []byte) (h uint64) {
	return HashStr(*(*string)(unsafe.Pointer(&s)))
}

func HashStr(s string) (h uint64) {
	h = 14695981039346656037 // fnv64
	for i := 0; i < len(s); i++ {
		h = h * 1099511628211
		h = h ^ uint64(s[i])
	}
	return h
}

func HashBytes128(b []byte) (buf [16]byte) {
	h := sha1.Sum(b)
	copy(buf[:], h[:])
	return
}

func HashStr128(s string) (buf [16]byte) {
	var b []byte
	*(*string)(unsafe.Pointer(&b)) = s
	(*reflect.SliceHeader)(unsafe.Pointer(&b)).Cap = len(s)
	buf = HashBytes128(b)
	runtime.KeepAlive(s)
	return
}

func Recover(f func()) {
	if r := recover(); r != nil {
		logrus.Error("fatal: ", r, " ", string(debug.Stack()))
		if f != nil {
			f()
		}
	}
}

func HTTPRecover(w http.ResponseWriter, rr *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			w.WriteHeader(500)
			w.Write(debug.Stack())
			logrus.Errorf("fatal HTTP error of %q: %v", rr.RequestURI, r)
		}
	}()
}

func PanicErr(err error) {
	if err != nil {
		panic(err)
	}
}

func Bytes(b []byte) []byte {
	return append([]byte{}, b...)
}

func IncBytes(b []byte) []byte {
	return IncBytesInplace(Bytes(b))
}

func IncBytesInplace(b []byte) []byte {
	for i := len(b) - 1; i >= 0; i-- {
		b[i]++
		if b[i] != 0 {
			break
		}
	}
	return b
}

func AddBytesInplace(a, b []byte) []byte {
	_, _ = b[len(a)-1], a[len(b)-1]
	var carry byte
	for i := len(a) - 1; i >= 0; i-- {
		v := int(a[i]) + int(b[i]) + int(carry)
		if v > 255 {
			carry = 1
		} else {
			carry = 0
		}
		a[i] = byte(v)
	}
	return a
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

func BitsMask(hi, lo int64) int64 {
	return int64(1<<hi - 1<<lo + 1<<hi)
}

func CompressBulks(m [][]byte) []byte {
	p := &bytes.Buffer{}
	w := lz4.NewWriter(p)
	var tmp []byte
	for _, v := range m {
		tmp = binary.AppendUvarint(tmp[:0], uint64(len(v)))
		tmp = append(tmp, v...)
		w.Write(tmp)
	}
	w.Flush()
	return p.Bytes()
}

func DecompressBulks(r io.Reader) (bulks [][]byte, err error) {
	rd := bufio.NewReader(lz4.NewReader(r))
	for {
		n, err := binary.ReadUvarint(rd)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		b := make([]byte, n)
		if _, err := io.ReadFull(rd, b); err != nil {
			return nil, err
		}
		bulks = append(bulks, b)
	}
	return
}

func SizeOfBulksExceeds(bulks [][]byte, max int) bool {
	for _, b := range bulks {
		max -= len(b)
		if max < 0 {
			return true
		}
	}
	return false
}
