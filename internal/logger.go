package internal

import (
	"bytes"
	"path/filepath"
	"strconv"

	"github.com/sirupsen/logrus"
)

type LogFormatter struct{}

func (f *LogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	buf := bytes.Buffer{}
	if entry.Level <= logrus.ErrorLevel {
		buf.WriteString("@err")
	} else {
		buf.WriteString("@info")
	}
	if v, ok := entry.Data["shard"]; ok {
		buf.WriteString("`shard")
		buf.WriteString(v.(string))
	}
	buf.WriteString("\t")
	buf.WriteString(entry.Time.UTC().Format("2006-01-02T15:04:05.000\t"))
	buf.WriteString(filepath.Base(entry.Caller.File))
	buf.WriteString(":")
	buf.WriteString(strconv.Itoa(entry.Caller.Line))
	buf.WriteString("\t")
	buf.WriteString(entry.Message)
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}
