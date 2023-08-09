package server

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/coyove/sdss/future"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var slowLogger *log.Logger
var dbLogger *log.Logger
var workerLogger *log.Logger

func InitLogger(runtime, slow, db, worker string) {
	log.SetReportCaller(true)
	setLogger(log.StandardLogger(), runtime, false)

	slowLogger = log.New()
	setLogger(slowLogger, slow, true)

	dbLogger = log.New()
	setLogger(dbLogger, db, true)

	workerLogger = log.New()
	workerLogger.SetReportCaller(true)
	setLogger(workerLogger, worker, false)

	go future.StartWatcher(func(err error) {
		log.Errorf("future NTP watcher: %v", err)
	})
}

type logf struct {
	simple bool
}

func setLogger(log *logrus.Logger, output string, simple bool) {
	lf := &logf{
		simple: simple,
	}
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

func (f *logf) Format(entry *logrus.Entry) ([]byte, error) {
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
	return buf.Bytes(), nil
}
