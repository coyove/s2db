package wire

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"
	"time"
)

func TestWriter_Write(t *testing.T) {
	buff := bytes.NewBuffer(nil)
	w := NewWriter(buff)
	w.WriteBulk("hello")
	if buff.String() != "$5\r\nhello\r\n" {
		t.Errorf("Unexpected WriteBulkString")
	}
}

func TestConnString(t *testing.T) {
	a, _ := ParseConnString("a?Name=name&ReadTimeout=1e9")
	if a.ReadTimeout != time.Second {
		t.Fail()
	}

	if !(reflect.DeepEqual([]interface{}{"a", "b", "c d", int64(10), 0.5}, SplitCmdLine("a b \"c d\" 10 0.5"))) {
		t.Fail()
	}
}

func BenchmarkWriteString(b *testing.B) {
	w := NewWriter(ioutil.Discard)
	for i := 0; i < b.N; i++ {
		w._writeString(":")
	}
}

func BenchmarkWriteStringBytes(b *testing.B) {
	w := NewWriter(ioutil.Discard)
	for i := 0; i < b.N; i++ {
		w.Write([]byte(":"))
	}
}
