package wire

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj/bas"
	"github.com/coyove/nj/typ"
	"github.com/coyove/s2db/s2"
	"github.com/sirupsen/logrus"
)

var (
	star   = []byte{'*'}
	colon  = []byte{':'}
	dollar = []byte{'$'}
	plus   = []byte{'+'}
	subs   = []byte{'-'}
	// newLine  = []byte{'\r', '\n'}
	// nilBulk  = []byte{'$', '-', '1', '\r', '\n'}
	// nilArray = []byte{'*', '-', '1', '\r', '\n'}
)

type Writer struct {
	Conn   io.Writer
	Logger *logrus.Logger
	pMode  bool
	tmp    *bytes.Buffer
}

func NewWriter(sink io.Writer, logger *logrus.Logger) *Writer {
	return &Writer{
		Conn:   sink,
		Logger: logger,
	}
}

func (w *Writer) EnablePipelineMode() {
	w.pMode = true
}

func (w *Writer) Write(data []byte) (int, error) {
	if !w.pMode {
		return w.Conn.Write(data)
	}
	if w.tmp == nil {
		w.tmp = &bytes.Buffer{}
	}
	return w.tmp.Write(data)
}

func (w *Writer) Flush() error {
	if w.pMode {
		if w.tmp == nil {
			return nil
		}
		_, err := w.Conn.Write(w.tmp.Bytes())
		w.tmp.Reset()
		return err
	}
	if f, ok := w.Conn.(*bufio.Writer); ok {
		return f.Flush()
	}
	return nil
}

func (w *Writer) WriteInt64(val int64) error {
	w.Write(colon)
	w.Write(itob(val))
	_, err := w.Write(newLine)
	return err
}

func (w *Writer) WriteBulk(val []byte) error {
	if val == nil {
		_, err := w.Write(nilBulk)
		return err
	}
	return w.WriteBulkNoNil(val)
}

func (w *Writer) WriteBulkNoNil(val []byte) error {
	if _, err := w.Write(dollar); err != nil {
		return err
	}
	if _, err := w.Write(itob(int64(len(val)))); err != nil {
		return err
	}
	if _, err := w.Write(newLine); err != nil {
		return err
	}
	if _, err := w.Write(val); err != nil {
		return err
	}
	_, err := w.Write(newLine)
	return err
}

func (w *Writer) WriteBulkString(s string) error {
	return w.WriteBulk([]byte(s))
}

func (w *Writer) WriteSimpleString(s string) error {
	w.Write(plus)
	w.Write([]byte(s))
	_, err := w.Write(newLine)
	return err
}

func (w *Writer) ReturnError(err error) func() error {
	return func() error { return w.WriteError(err.Error()) }
}

func (w *Writer) WriteError(s string) error {
	if !strings.Contains(s, "NOAUTH") &&
		!strings.Contains(s, ErrBlacklistedIP.Error()) &&
		s != pebble.ErrClosed.Error() &&
		s != ErrUnknownCommand.Error() &&
		s != ErrServerReadonly.Error() {
		logrus.Error("redis wire error: ", s)
	}
	s = strings.Replace(s, "\n", " ", -1)
	s = strings.Replace(s, "\r", " ", -1)
	w.Write(subs)
	w.Write([]byte(s))
	_, err := w.Write(newLine)
	return err
}

func (w *Writer) WriteIntOrError(v interface{}, err error) error {
	if err != nil {
		return w.WriteError(err.Error())
	}
	if u, ok := v.(uint64); ok {
		return w.WriteInt64(int64(u))
	}
	return w.WriteInt64(reflect.ValueOf(v).Int())
}

func (w *Writer) WriteObject(v interface{}) error {
	if v == nil {
		return w.WriteBulk(nil)
	}
	if objs, ok := v.([]interface{}); ok {
		return w.WriteObjectsSlice(objs)
	}
	switch rv := reflect.ValueOf(v); rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return w.WriteInt64(rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return w.WriteInt64(int64(rv.Uint()))
	default:
		return w.WriteBulkString(rv.String())
	}
}

func (w *Writer) WriteValue(v bas.Value) error {
	if v.IsNumber() {
		if v.IsInt64() {
			return w.WriteInt64(v.Int64())
		}
		return w.WriteBulk(s2.FormatFloatBulk(v.Float64()))
	}
	if v.IsArray() {
		var args []interface{}
		for i := 0; i < v.Native().Len(); i++ {
			args = append(args, v.Native().Get(i).Interface())
		}
		return w.WriteObjects(args)
	}
	if v.Type() == typ.Bool {
		return w.WriteSimpleString(v.String())
	}
	return w.WriteBulkString(v.String())
}

func (w *Writer) WriteObjects(objs ...interface{}) error {
	if objs == nil {
		_, err := w.Write(nilArray)
		return err
	}

	w.Write(star)
	w.Write(itob(int64(len(objs))))
	w.Write(newLine)

	numArg := len(objs)
	for i := 0; i < numArg; i++ {
		v := objs[i]
		if v == nil {
			if err := w.WriteBulk(nil); err != nil {
				return err
			}
			continue
		}
		switch v := v.(type) {
		case []string:
			if err := w.WriteBulkStrings(v); err != nil {
				return err
			}
		case [][]byte:
			if err := w.WriteBulks(v); err != nil {
				return err
			}
		case []byte:
			if err := w.WriteBulk(v); err != nil {
				return err
			}
		case string:
			if err := w.WriteBulkString(v); err != nil {
				return err
			}
		case int:
			if err := w.WriteInt64(int64(v)); err != nil {
				return err
			}
		case int32:
			if err := w.WriteInt64(int64(v)); err != nil {
				return err
			}
		case int64:
			if err := w.WriteInt64(int64(v)); err != nil {
				return err
			}
		case uint64:
			if err := w.WriteInt64(int64(v)); err != nil {
				return err
			}
		case float64:
			if err := w.WriteBulk(s2.FormatFloatBulk(v)); err != nil {
				return err
			}
		case []interface{}:
			if err := w.WriteObjectsSlice(v); err != nil {
				return err
			}
		case error:
			if err := w.WriteError(fmt.Sprint(v)); err != nil {
				return err
			}
		case bas.Value:
			if err := w.WriteValue(v); err != nil {
				return err
			}
		default:
			return fmt.Errorf("value not suppport %v", v)
		}
	}
	return nil
}

func (w *Writer) WriteBulks(bulks [][]byte) error {
	if bulks == nil {
		_, err := w.Write(nilArray)
		return err
	}

	if _, err := w.Write(star); err != nil {
		return err
	}
	numElement := len(bulks)
	if _, err := w.Write(itob(int64(numElement))); err != nil {
		return err
	}
	if _, err := w.Write(newLine); err != nil {
		return err
	}

	for i := 0; i < numElement; i++ {
		if err := w.WriteBulk(bulks[i]); err != nil {
			return err
		}
	}
	return nil
}

// WriteObjectsSlice works like WriteObjects, it useful when args is a slice that can be nil,
// in that case WriteObjects(nil) will understand as response 1 element array (nil element)
func (w *Writer) WriteObjectsSlice(args []interface{}) error {
	return w.WriteObjects(args...)
}

func (w *Writer) WriteBulkStrings(bulks []string) error {
	if bulks == nil {
		_, err := w.Write(nilArray)
		return err
	}

	w.Write(star)
	numElement := len(bulks)
	w.Write(itob(int64(numElement)))
	w.Write(newLine)

	for i := 0; i < numElement; i++ {
		if err := w.WriteBulkString(bulks[i]); err != nil {
			return err
		}
	}
	return nil
}
