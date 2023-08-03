package config

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/coyove/sdss/future"
)

func Save(path string, data any) error {
	out := path + "." + strconv.FormatInt(future.UnixNano(), 10)
	f, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return err
	}

	buf := &bytes.Buffer{}
	w := json.NewEncoder(buf)
	w.SetIndent("", "  ")
	if err := w.Encode(data); err != nil {
		f.Close()
		return err
	}
	if buf.Len() < 2 || buf.Bytes()[0] != '{' {
		f.Close()
		return fmt.Errorf("save: invalid config: %q", buf.Bytes())
	}

	h := sha1.Sum(buf.Bytes())
	buf.WriteString(hex.EncodeToString(h[:]))
	buf.WriteString("\n") // tail: 40 + 23 + 1
	buf.WriteString(time.Now().UTC().Format("2006-01-02T15:04:05.000"))

	f.Write(buf.Bytes())
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(out, path)
}

func Load(path string, data any) error {
	dir := filepath.Dir(path)
	fn := filepath.Base(path)
	rx := regexp.MustCompile(regexp.QuoteMeta(fn) + `\.(\d+)`)

	fi, _ := ioutil.ReadDir(dir)
	for _, f := range fi {
		if rx.MatchString(f.Name()) {
			os.Remove(filepath.Join(dir, f.Name()))
		}
	}

	b, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return fmt.Errorf("buffer corrupted: empty")
	}

	if b[len(b)-1] == '}' {
		// Ignoring checksum
		return json.Unmarshal(b, data)
	}

	if len(b) < 64 {
		return fmt.Errorf("buffer corrupted: length")
	}

	buf := b[:len(b)-64]
	h := sha1.Sum(buf)
	if hex.EncodeToString(h[:]) != string(b[len(b)-64:len(b)-64+40]) {
		return fmt.Errorf("buffer corrupted: hash")
	}

	return json.Unmarshal(buf, data)
}

func Update(path string, data any, f func(any)) error {
	if err := Load(path, data); err != nil {
		return err
	}
	f(data)
	return Save(path, data)
}
