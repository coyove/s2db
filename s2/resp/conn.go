package resp

import (
	"net/url"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"text/scanner"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

type RedisConfig struct {
	URI string
	redis.Options
}

func (rc RedisConfig) GetClient() *redis.Client {
	return redis.NewClient(&rc.Options)
}

func ParseConnString(addr string) (cfg RedisConfig, err error) {
	if !strings.HasPrefix(addr, "redis://") {
		addr = "redis://" + addr
	}
	cfg.URI = addr
	u, err := url.Parse(addr)
	if err != nil {
		return cfg, err
	}

	cfg.Addr = u.Host
	if !strings.Contains(cfg.Addr, ":") {
		cfg.Addr += ":6379"
	}
	if u.User != nil {
		cfg.Password = u.User.Username()
	}

	rv := reflect.ValueOf(&cfg.Options).Elem()
	for k, vs := range u.Query() {
		if len(vs) == 0 {
			continue
		}
		if f := rv.FieldByName(k); f.Type() == reflect.TypeOf(time.Duration(0)) {
			v, _ := strconv.ParseInt(vs[0], 10, 64)
			f.SetInt(v * 1e6)
		} else if f.Kind() >= reflect.Int && f.Kind() <= reflect.Int64 {
			v, _ := strconv.ParseInt(vs[0], 10, 64)
			f.SetInt(v)
		} else {
			logrus.Infof("invalid option field %q in %s", k, addr)
		}
	}
	if cfg.Options.DialTimeout == 0 {
		cfg.Options.DialTimeout = time.Second
	}
	if m := 20 * runtime.NumCPU(); cfg.Options.PoolSize < m {
		cfg.Options.PoolSize = m
	}
	cfg.Options.MaxRetries = -1
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