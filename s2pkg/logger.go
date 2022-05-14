package s2pkg

import (
	"bytes"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

type LogFormatter struct {
	SlowLog bool
	last    string
}

func (f *LogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	if strings.HasPrefix(entry.Message, "[M]") && f.last == entry.Message {
		return nil, nil
	}

	buf := bytes.Buffer{}
	if f.SlowLog {
		ts := entry.Time.UTC()
		buf.WriteString(strconv.FormatInt(ts.UnixNano()/1e6, 10))
		buf.WriteString("\t")
		buf.WriteString(ts.Format("01-02T15:04:05"))
	} else {
		if entry.Level <= logrus.ErrorLevel {
			buf.WriteString("ERR")
		} else {
			buf.WriteString("INFO")
		}
		if v, ok := entry.Data["shard"]; ok {
			buf.WriteString("\t#")
			buf.WriteString(v.(string))
		} else {
			buf.WriteString("\t-")
		}
		buf.WriteString("\t")
		buf.WriteString(entry.Time.UTC().Format("2006-01-02T15:04:05.000\t"))
		if entry.Caller == nil {
			buf.WriteString("internal")
		} else {
			buf.WriteString(filepath.Base(entry.Caller.File))
			buf.WriteString(":")
			buf.WriteString(strconv.Itoa(entry.Caller.Line))
		}
	}
	buf.WriteString("\t")
	buf.WriteString(entry.Message)
	buf.WriteByte('\n')
	if strings.HasPrefix(entry.Message, "[M]") {
		f.last = entry.Message
	}
	return buf.Bytes(), nil
}
