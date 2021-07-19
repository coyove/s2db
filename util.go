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
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/coyove/common/lru"
	"github.com/secmask/go-redisproto"
	"gitlab.litatom.com/zhangzezhong/zset/calc"
	"go.etcd.io/bbolt"
)

func init() {
	redisproto.MaxBulkSize = 1 << 20
	redisproto.MaxNumArg = 10000
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
}

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

func bytesToFloatZero(b []byte) float64 {
	if len(b) != 8 {
		return 0
	}
	return bytesToFloat(b)
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
	for _, buf := range in.Argv {
		for _, b := range buf {
			old := h[1]
			h[1] = h[1]*33 + uint64(b)
			if h[1] < old {
				h[0]++
			}
		}
		h[1]++
	}
	return h
}

func atof(a string) (float64, error) { return calc.Eval(a) }

func atof2(a []byte) (float64, error) { return atof(*(*string)(unsafe.Pointer(&a))) }

func ftoa(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

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
	return joinCommand(cmd.Argv...)
}

func splitCommand(in string) (*redisproto.Command, error) {
	command := &redisproto.Command{}
	buf, _ := base64.URLEncoding.DecodeString(in)
	err := gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&command.Argv)
	return command, err
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
	Limit       int
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

func (r RangeLimit) fromFloatString(v string) (RangeLimit, error) {
	var err error
	r.Inclusive = true
	if strings.HasPrefix(v, "[") {
		r.Float, err = atof(v[1:])
	} else if strings.HasPrefix(v, "(") {
		r.Float, err = atof(v[1:])
		r.Inclusive = false
	} else {
		r.Float, err = atof(v)
	}
	return r, err
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

type ServerConfig struct {
	HardLimit     int
	CacheSize     int
	WeakCacheSize int
	WeakTTL       time.Duration
	SlowLimit     time.Duration
}

func (s *Server) validateConfig() {
	if s.HardLimit <= 0 {
		s.HardLimit = 10000
	}
	if s.WeakTTL <= 0 {
		s.WeakTTL = time.Minute * 5
	}
	if s.CacheSize <= 0 {
		s.CacheSize = 1024
	}
	s.cache = NewCache(int64(s.CacheSize) * 1024 * 1024)
	if s.WeakCacheSize <= 0 {
		s.WeakCacheSize = 1024
	}
	s.weakCache = lru.NewCache(int64(s.WeakCacheSize) * 1024 * 1024)
	if s.SlowLimit <= 0 {
		s.SlowLimit = time.Second / 2
	}
}

func (s *Server) loadConfig() error {
	err := s.db[0].Update(func(tx *bbolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte("_config"))
		if err != nil {
			return err
		}

		rv := reflect.ValueOf(&s.ServerConfig)
		rt := reflect.TypeOf(s.ServerConfig)
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			fv := rv.Elem().Field(i)
			n := strings.ToLower(f.Name)
			buf := bk.Get([]byte(n))
			switch f.Type {
			case reflect.TypeOf(0):
				fv.SetInt(int64(bytesToFloatZero(buf)))
			case reflect.TypeOf(time.Second):
				fv.SetInt(int64(time.Duration(bytesToFloatZero(buf)) * time.Millisecond))
			case reflect.TypeOf(""):
				fv.SetString(string(buf))
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.validateConfig()
	return s.saveConfig()
}

func (s *Server) saveConfig() error {
	return s.db[0].Update(func(tx *bbolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte("_config"))
		if err != nil {
			return err
		}

		rv := reflect.ValueOf(&s.ServerConfig)
		rt := reflect.TypeOf(s.ServerConfig)
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			fv := rv.Elem().Field(i)
			n := strings.ToLower(f.Name)

			var buf []byte
			switch f.Type {
			case reflect.TypeOf(0):
				buf = floatToBytes(float64(fv.Int()))
			case reflect.TypeOf(time.Second):
				buf = floatToBytes(float64(fv.Int() / int64(time.Millisecond)))
			case reflect.TypeOf(""):
				buf = []byte(fv.String())
			}
			if err := bk.Put([]byte(n), buf); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Server) updateConfig(key, value string) error {
	rv := reflect.ValueOf(&s.ServerConfig)
	rt := reflect.TypeOf(s.ServerConfig)
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		fv := rv.Elem().Field(i)
		if strings.ToLower(f.Name) != key {
			continue
		}
		I := int64(atoi(string(value)))
		switch f.Type {
		case reflect.TypeOf(0):
			fv.SetInt(I)
		case reflect.TypeOf(time.Second):
			fv.SetInt(int64(time.Duration(I) * time.Millisecond))
		case reflect.TypeOf(""):
			fv.SetString(string(value))
		}
		break
	}
	s.validateConfig()
	return s.saveConfig()
}

func (s *Server) getConfig(key string) (string, bool) {
	rv := reflect.ValueOf(&s.ServerConfig)
	rt := reflect.TypeOf(s.ServerConfig)
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		fv := rv.Elem().Field(i)
		if strings.ToLower(f.Name) != key {
			continue
		}
		return fmt.Sprint(fv.Interface()), true
	}
	return "", false
}
