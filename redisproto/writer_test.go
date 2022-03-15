package redisproto

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestWriter_Write(t *testing.T) {
	buff := bytes.NewBuffer(nil)
	w := NewWriter(buff, logrus.StandardLogger())
	w.WriteBulkString("hello")
	if buff.String() != "$5\r\nhello\r\n" {
		t.Errorf("Unexpected WriteBulkString")
	}
}

func TestWriter_WriteSlice(t *testing.T) {
	buff := bytes.NewBuffer(nil)
	w := NewWriter(buff, logrus.StandardLogger())
	w.WriteObjectsSlice(nil)
	if buff.String() != "*-1\r\n" {
		t.Errorf("Unexpected WriteObjectsSlice")
	}
}

func TestWriter_WriteSlice2(t *testing.T) {
	buff := bytes.NewBuffer(nil)
	w := NewWriter(buff, logrus.StandardLogger())
	w.WriteObjectsSlice([]interface{}{1})
	if buff.String() != "*1\r\n:1\r\n" {
		t.Errorf("Unexpected WriteObjectsSlice, got %s", buff.String())
	}
}

func BenchmarkFlags(b *testing.B) {
	cmd := Command{Argv: [][]byte{
		[]byte("ZADD"),
		[]byte("zzz"),
		[]byte("1.2345"),
		[]byte("key"),
		[]byte("withscores"),
	}}
	for i := 0; i < b.N; i++ {
		cmd.Flags(2)
	}
}

func TestConnString(t *testing.T) {
	a, _ := ParseConnString("a?Name=name&ReadTimeout=1e9")
	if a.ReadTimeout != time.Second || a.Name != "name" {
		t.Fail()
	}

	if !(reflect.DeepEqual([]interface{}{"a", "b", "c d", int64(10), 0.5}, SplitCmdLine("a b \"c d\" 10 0.5"))) {
		t.Fail()
	}
}
