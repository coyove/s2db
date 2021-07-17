package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"unsafe"

	"github.com/secmask/go-redisproto"
)

var (
	MinScoreRange = RangeLimit{Float: math.Inf(-1), Inclusive: true}
	MaxScoreRange = RangeLimit{Float: math.Inf(1), Inclusive: true}
)

func checkScore(s float64) error {
	if math.IsNaN(s) {
		return fmt.Errorf("score is NaN")
	}
	return nil
}

func intToBytes(i uint64) []byte {
	v := [8]byte{}
	binary.BigEndian.PutUint64(v[:], i)
	return v[:]
}

func bytesToFloat(b []byte) float64 {
	x := binary.BigEndian.Uint64(b)
	if x>>63 == 1 {
		x = x << 1 >> 1
	} else {
		x = ^x
	}
	return math.Float64frombits(x)
}

func floatToBytes(v float64) []byte {
	x := math.Float64bits(v)
	if v >= 0 {
		x |= 1 << 63
	} else {
		x = ^x
	}
	tmp := [8]byte{}
	binary.BigEndian.PutUint64(tmp[:8], x)
	return tmp[:]
}

func floatBytesStep(buf []byte, s int64) []byte {
	v := binary.BigEndian.Uint64(buf)
	binary.BigEndian.PutUint64(buf, uint64(int64(v)+s))
	return buf
}

func hashStr(s string) (h uint64) {
	h = 5381
	for i := 0; i < len(s); i++ {
		h = h*33 + uint64(s[i])
	}
	return h
}

func hashCommands(in *redisproto.Command) (h [2]uint64) {
	h = [2]uint64{0, 5381}
	for _, buf := range *(*[][]byte)(unsafe.Pointer(in)) {
		for _, b := range buf {
			old := h[1]
			h[1] = h[1]*33 + uint64(b)
			if h[1] < old {
				h[0]++
			}
		}
	}
	return h
}

func atof(a string) float64 {
	if a == "+inf" {
		return math.Inf(1)
	}
	if a == "-inf" {
		return math.Inf(-1)
	}
	i, _ := strconv.ParseFloat(a, 64)
	return i
}

func ftoa(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func atoi(a string) int {
	i, _ := strconv.Atoi(a)
	return i
}

func restCommandsToKeys(i int, command *redisproto.Command) []string {
	keys := []string{}
	for ; i < command.ArgCount(); i++ {
		keys = append(keys, string(command.Get(i)))
	}
	return keys
}

func reversePairs(in []Pair) []Pair {
	for i := 0; i < len(in)/2; i++ {
		j := len(in) - 1 - i
		in[i], in[j] = in[j], in[i]
	}
	return in
}

func writePairs(in []Pair, w *redisproto.Writer, command *redisproto.Command) error {
	withScores := bytes.Equal(bytes.ToUpper(command.Get(command.ArgCount()-1)), []byte("WITHSCORES"))
	data := make([]string, 0, len(in))
	for _, p := range in {
		data = append(data, p.Key)
		if withScores {
			data = append(data, strconv.FormatFloat(p.Score, 'f', -1, 64))
		}
	}
	return w.WriteBulkStrings(data)
}

func sizePairs(in []Pair) int {
	sz := 1
	for _, p := range in {
		sz += len(p.Key) + 8 + len(p.Data)
	}
	return sz
}

func dumpCommand(cmd *redisproto.Command) []byte {
	x := *(*[][]byte)(unsafe.Pointer(cmd))
	return joinCommand(x...)
}

func splitCommand(in string) (*redisproto.Command, error) {
	var command struct {
		data [][]byte
		a    bool
	}
	command.a = true
	buf, _ := base64.URLEncoding.DecodeString(in)
	err := gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&command.data)
	return (*redisproto.Command)(unsafe.Pointer(&command)), err
}

func joinCommand(cmd ...[]byte) []byte {
	buf := &bytes.Buffer{}
	enc := base64.NewEncoder(base64.URLEncoding, buf)
	gob.NewEncoder(enc).Encode(cmd)
	enc.Close()
	return buf.Bytes()
}

func joinCommandString(cmd ...string) []byte {
	tmp := make([][]byte, len(cmd))
	for i := range cmd {
		tmp[i] = []byte(cmd[i])
	}
	return joinCommand(tmp...)
}

type RangeLimit struct {
	Value     string
	Float     float64
	Inclusive bool
}

type RangeOptions struct {
	OffsetStart int
	OffsetEnd   int
	CountOnly   bool
	DeleteLog   []byte
}

func (r RangeLimit) fromString(v string) RangeLimit {
	r.Value = v
	r.Inclusive = true
	if strings.HasPrefix(v, "[") {
		r.Value = r.Value[1:]
	} else if strings.HasPrefix(v, "(") {
		r.Value = r.Value[1:]
		r.Inclusive = false
	} else if v == "+" {
		r.Value = "\xff"
	} else if v == "-" {
		r.Value = ""
	}
	return r
}

func (r RangeLimit) fromFloatString(v string) RangeLimit {
	r.Inclusive = true
	if strings.HasPrefix(v, "[") {
		r.Float = atof(v[1:])
	} else if v == "(+inf" {
		r.Float = math.Inf(1)
		r.Inclusive = false
	} else if v == "(-inf" {
		r.Float = math.Inf(-1)
		r.Inclusive = false
	} else if strings.HasPrefix(v, "(") {
		r.Float = atof(v[1:])
		r.Inclusive = false
	} else if v == "+inf" {
		r.Float = math.Inf(1)
	} else if v == "-inf" {
		r.Float = math.Inf(-1)
	} else {
		r.Float = atof(v)
	}
	return r
}

// Copy the src file to dst. Any existing file will be overwritten and will not
// copy file attributes.
func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}
