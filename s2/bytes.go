package s2

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"unsafe"

	"github.com/pierrec/lz4/v4"
)

func MustParseFloat(a string) float64 {
	if strings.HasPrefix(a, "0x") {
		v, err := strconv.ParseUint(a[2:], 16, 64)
		if err != nil {
			panic(err)
		}
		return math.Float64frombits(v)
	}
	v, err := strconv.ParseFloat(a, 64)
	if err != nil {
		panic(err)
	}
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

func CompressBulks(m [][]byte) []byte {
	p := &bytes.Buffer{}
	w := lz4.NewWriter(p)
	var tmp []byte
	for _, v := range m {
		tmp = binary.AppendUvarint(tmp[:0], uint64(len(v)))
		w.Write(tmp)
		w.Write(v)
	}
	w.Flush()
	return p.Bytes()
}

type stringReader string

func (r *stringReader) Read(b []byte) (n int, err error) {
	n = copy(b, *r)
	*r = (*r)[n:]
	if n == 0 {
		err = io.EOF
	}
	return
}

func DecompressBulksString(in string) (bulks [][]byte, err error) {
	return DecompressBulks((*stringReader)(&in))
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

func NZEncode(out, in []byte) []byte {
	for i := 0; i < len(in); i += 8 {
		var x uint64
		for j := i; j < i+7; j++ {
			x = x << 9
			if j < len(in) {
				x = x + 256 + uint64(in[j])
			}
		}
		x = x << 1
		if j := i + 8; j < len(in) {
			y := 256 + uint64(in[j])
			x += y >> 8
			out = binary.BigEndian.AppendUint64(out, x)
			out = append(out, byte(y))
		} else {
			out = binary.BigEndian.AppendUint64(out, x)
		}
	}
	out = bytes.TrimRight(out, "\x00")
	return out
}

func NZDecode(out, in []byte) []byte {
	for i := 0; i < len(in); i += 9 {
		end := i + 9
		if end > len(in) {
			w := (len(in) - i) * 8 / 9
			if len(in) == 1 {
				w = 1
			}
			var x uint64
			for j := i; j < i+8; j++ {
				x = x << 8
				if j < len(in) {
					x += uint64(in[j])
				}
			}
			for j := 55; j >= 1 && w > 0; j -= 9 {
				v := (x >> j) & 0x1ff
				if v == 0 {
					break
				}
				out = append(out, byte(v-256))
				w--
			}
		} else {
			x := binary.BigEndian.Uint64(in[i:])
			for j := 55; j >= 1; j -= 9 {
				v := (x >> j) & 0x1ff
				if v == 0 {
					break
				}
				out = append(out, byte(v-256))
			}
			v := (uint64(in[i+8]) + uint64(x&1)<<8)
			if v > 0 {
				out = append(out, byte(v-256))
			}
		}
	}
	return out
}

func ToBytes(v any) []byte {
	switch v := v.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	case int, int8, int16, int32, int64:
		return strconv.AppendInt(nil, reflect.ValueOf(v).Int(), 10)
	case uint, uint8, uint16, uint32, uint64, uintptr:
		return strconv.AppendUint(nil, reflect.ValueOf(v).Uint(), 10)
	case float32, float64:
		return strconv.AppendFloat(nil, reflect.ValueOf(v).Float(), 'f', -1, 64)
	default:
		return fmt.Append(nil, v)
	}
}

func HexEncode(k []byte) []byte {
	_ = k[15]
	k0 := make([]byte, 32)
	hex.Encode(k0, k)
	return k0
}

func HexDecode(k []byte) []byte {
	_ = k[31]
	k0 := make([]byte, 16)
	if len(k) == 33 && k[16] == '_' {
		hex.Decode(k0[:8], k[:16])
		hex.Decode(k0[8:], k[17:])
	} else {
		hex.Decode(k0, k)
	}
	return k0
}

func HexEncodeBulks(ids [][]byte) [][]byte {
	hexIds := make([][]byte, len(ids))
	for i := range hexIds {
		hexIds[i] = HexEncode(ids[i])
	}
	return hexIds
}

func HexDecodeBulks(ids [][]byte) [][]byte {
	hexIds := make([][]byte, len(ids))
	for i := range hexIds {
		hexIds[i] = HexDecode(ids[i])
	}
	return hexIds
}
