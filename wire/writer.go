package wire

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"runtime"
	"strconv"
	"strings"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/sirupsen/logrus"
)

type Writer struct {
	conn   io.Writer
	tmp    []byte
	ppMode bool
	ppBuf  *bytes.Buffer
}

type WriterImpl interface {
	WriteSimpleString(s string) error
	WriteError(s string) error
	WriteInt64(val int64) error
	WriteBulk(val any) error
	WriteBulks(value any) error
	WriteBulkBulks(a any, b any) error
	GetWriter() io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		conn: w,
	}
}

func (w *Writer) EnablePipelineMode() {
	w.ppMode = true
}

func (w *Writer) Write(data []byte) (int, error) {
	if !w.ppMode {
		return w.conn.Write(data)
	}
	if w.ppBuf == nil {
		w.ppBuf = &bytes.Buffer{}
	}
	return w.ppBuf.Write(data)
}

func (w *Writer) Flush() error {
	if w.ppMode {
		if w.ppBuf == nil {
			return nil
		}
		_, err := w.conn.Write(w.ppBuf.Bytes())
		w.ppBuf.Reset()
		return err
	}
	if f, ok := w.conn.(*bufio.Writer); ok {
		return f.Flush()
	}
	return nil
}

func (w *Writer) _writeString(s string) error {
	var b []byte
	*(*struct {
		a   string
		cap int
	})(unsafe.Pointer(&b)) = struct {
		a   string
		cap int
	}{s, len(s)}
	_, err := w.Write(b)
	runtime.KeepAlive(s)
	return err
}

func (w *Writer) _writeInt(v int64) error {
	w.tmp = strconv.AppendInt(w.tmp[:0], v, 10)
	_, err := w.Write(w.tmp)
	return err
}

func (w *Writer) WriteInt64(val int64) error {
	w._writeString(":")
	w._writeInt(val)
	_, err := w.Write(newLine)
	return err
}

func (w *Writer) WriteBulk(val any) error {
	w._writeString("$")
	if b, ok := val.([]byte); ok {
		w._writeInt(int64(len(b)))
		w._writeString("\r\n")
		w.Write(b)
	} else {
		s := val.(string)
		w._writeInt(int64(len(s)))
		w._writeString("\r\n")
		w._writeString(s)
	}
	return w._writeString("\r\n")
}

func (w *Writer) WriteSimpleString(s string) error {
	w._writeString("+")
	w._writeString(s)
	return w._writeString("\r\n")
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
	w._writeString("-")
	w._writeString(s)
	return w._writeString("\r\n")
}

func (w *Writer) WriteBulkBulks(a any, b any) error {
	w._writeString("*")
	w._writeInt(2)
	w._writeString("\r\n")
	if err := w.WriteBulk(a); err != nil {
		return err
	}
	if err := w.WriteBulks(b); err != nil {
		return err
	}
	return nil
}

func (w *Writer) WriteBulks(value any) error {
	bulks, ok := value.([][]byte)
	if !ok {
		bulks = make([][]byte, len(value.([]string)))
		for i, v := range value.([]string) {
			x := (*struct {
				a string
				b int
			})(unsafe.Pointer(&bulks[i]))
			x.a, x.b = v, len(v)
		}
		defer func() { runtime.KeepAlive(value) }()
	}

	w._writeString("*")
	w._writeInt(int64(len(bulks)))
	w._writeString("\r\n")

	for _, b := range bulks {
		if err := w.WriteBulk(b); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) GetWriter() io.Writer {
	return w.conn
}

type DummySink struct {
	val any
}

func (w *DummySink) WriteSimpleString(s string) error  { w.val = s; return nil }
func (w *DummySink) WriteError(s string) error         { w.val = errors.New(s); return nil }
func (w *DummySink) WriteInt64(val int64) error        { w.val = val; return nil }
func (w *DummySink) WriteBulk(val any) error           { w.val = val; return nil }
func (w *DummySink) WriteBulks(val any) error          { w.val = val; return nil }
func (w *DummySink) WriteBulkBulks(a any, b any) error { w.val = []any{a, b}; return nil }
func (w *DummySink) GetWriter() io.Writer              { panic("DummySink has no writer") }

func (w *DummySink) Err() error {
	if err, ok := w.val.(error); ok {
		return err
	}
	return nil
}

func (w *DummySink) Val() any {
	return w.val
}

func (w *DummySink) Bytes() []byte {
	if b, ok := w.val.([]byte); ok {
		return b
	}
	return []byte(w.val.(string))
}
