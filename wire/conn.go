package wire

import (
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"text/scanner"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisConfig struct {
	Raw  string
	Name string
	*redis.Options
}

func (rc RedisConfig) GetClient() *redis.Client {
	return redis.NewClient(rc.Options)
}

func ParseConnString(addr string) (cfg RedisConfig, err error) {
	if !strings.HasPrefix(addr, "redis://") {
		addr = "redis://" + addr
	}
	cfg.Raw = addr

	q := addr[strings.Index(addr, "?")+1:]
	if q == addr {
		q = ""
	}

	cfg.Options, err = redis.ParseURL(addr[:len(addr)-len(q)])
	if err != nil {
		return
	}

	uq, _ := url.ParseQuery(q)
	rv := reflect.ValueOf(cfg.Options).Elem()
	for k, vs := range uq {
		if len(vs) == 0 {
			continue
		}
		if k == "Name" {
			cfg.Name = vs[0]
		} else if f := rv.FieldByName(k); f.Kind() >= reflect.Int && f.Kind() <= reflect.Int64 {
			v, _ := strconv.ParseFloat(vs[0], 64)
			f.SetInt(int64(v))
		} else {
			err = fmt.Errorf("invalid option field: %q", k)
			return
		}
	}
	if cfg.Options.Username != "" {
		cfg.Options.Username, cfg.Options.Password = "", cfg.Options.Username
	}
	if cfg.Options.DialTimeout == 0 {
		cfg.Options.DialTimeout = time.Second
	}
	return
}

func SplitCmdLine(line string) (args []interface{}) {
	var s scanner.Scanner
	s.Init(strings.NewReader(line))
	s.Mode = scanner.ScanStrings | scanner.ScanFloats | scanner.ScanInts | scanner.ScanIdents

	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		txt := s.TokenText()
		switch tok {
		case scanner.Int:
			v, _ := strconv.ParseInt(txt, 10, 64)
			args = append(args, v)
		case scanner.Float:
			v, _ := strconv.ParseFloat(txt, 64)
			args = append(args, v)
		case scanner.String, scanner.Ident:
			if len(txt) >= 2 && txt[0] == '"' && txt[len(txt)-1] == '"' {
				txt, _ = strconv.Unquote(txt)
			}
			args = append(args, txt)
		}
	}

	return
}
