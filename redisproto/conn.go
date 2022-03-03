package redisproto

import (
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
)

type RedisConfig struct {
	Raw  string
	Name string
	Role string
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
		} else if k == "Role" {
			cfg.Role = vs[0]
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
	return
}
