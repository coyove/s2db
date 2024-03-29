package s2pkg

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

type LogFormatter struct {
	simple   bool
	last     string
	listener struct {
		sync.RWMutex
		idx int
		m   map[int]chan []byte
	}
	in chan []byte
}

func SetLogger(log *logrus.Logger, output string, simple bool) {
	lf := &LogFormatter{
		simple: simple,
		in:     make(chan []byte, 16),
	}
	lf.listener.m = map[int]chan []byte{}
	go func() {
		for data := range lf.in {
			lf.listener.RLock()
			for _, c := range lf.listener.m {
				select {
				case c <- data:
				default:
				}
			}
			lf.listener.RUnlock()
		}
	}()

	log.SetFormatter(lf)

	rd := strings.NewReader(output)
	maxSize, maxBackups, maxAge := 100, 8, 30
	fmt.Fscanf(rd, "%d,%d,%d,", &maxSize, &maxBackups, &maxAge)
	fn, _ := ioutil.ReadAll(rd)

	if output == "-" {
		log.SetOutput(ioutil.Discard)
	} else if output != "" {
		log.SetOutput(io.MultiWriter(os.Stdout, &lumberjack.Logger{
			Filename:   string(fn),
			MaxSize:    maxSize,
			MaxBackups: maxBackups,
			MaxAge:     maxAge,
			Compress:   true,
		}))
	} else {
		log.SetOutput(os.Stdout)
	}
	fmt.Printf("logger created: %q, max size: %d, max backups: %d, max age: %d\n", string(fn), maxSize, maxBackups, maxAge)
}

func (f *LogFormatter) LogFork(w io.WriteCloser) (err error) {
	c := make(chan []byte)
	f.listener.Lock()
	f.listener.idx++
	idx := f.listener.idx
	f.listener.m[idx] = c
	sz := len(f.listener.m)
	f.listener.Unlock()

	w.Write([]byte(fmt.Sprintf("log listener #%d of %d\n", idx, sz)))
	for data := range c {
		if _, err = w.Write(data); err != nil {
			break
		}
	}

	f.listener.Lock()
	delete(f.listener.m, idx)
	f.listener.Unlock()
	w.Close()
	return
}

func (f *LogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	if strings.HasPrefix(entry.Message, "[M]") && f.last == entry.Message {
		return nil, nil
	}

	buf := bytes.Buffer{}
	if f.simple {
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
			buf.WriteString("\t")
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
	select {
	case f.in <- buf.Bytes():
	default:
	}
	return buf.Bytes(), nil
}
