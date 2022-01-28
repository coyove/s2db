package s2pkg

import (
	"bytes"
	"path/filepath"
	"strconv"

	"github.com/sirupsen/logrus"
)

type LogFormatter struct {
	SlowLog bool
}

func (f *LogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
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
		buf.WriteString(filepath.Base(entry.Caller.File))
		buf.WriteString(":")
		buf.WriteString(strconv.Itoa(entry.Caller.Line))
	}
	buf.WriteString("\t")
	buf.WriteString(entry.Message)
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

// type SlowLogs struct {
// 	Start time.Time
// 	End   time.Time
// }
//
// func AnalyzeSlowLogs(path string) (sl SlowLogs, err error) {
// 	f, err := os.Open(path)
// 	if err != nil {
// 		return sl, err
// 	}
// 	defer f.Close()
// 	rd := bufio.NewReader(f)
//
// 	for {
// 		line, _ := rd.ReadBytes('\n')
// 		if len(line) == 0 {
// 			break
// 		}
// 		line = bytes.TrimSpace(line)
// 		parts := bytes.Split(line, []byte("\t"))
// 		startTS := MustParseInt64(string(parts[0]))
// 	}
// }
//
