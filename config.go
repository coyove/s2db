package main

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/extdb"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type ServerConfig struct {
	ServerName         string
	Slave              string
	Password           string
	MarkMaster         int // 0|1
	Passthrough        string
	CacheSize          int
	CacheObjMaxSize    int // kb
	WeakCacheSize      int
	SlowLimit          int // ms
	PingTimeout        int // ms
	ResponseLogSize    int // kb
	BatchMaxRun        int
	BatchFirstRunSleep int // ms
	CompactLogsTTL     int // sec
	CompactLogsDice    int
	PushLogsDice       int
	MetricsEndpoint    string
	InspectorSource    string
}

func (s *Server) loadConfig() error {
	if err := s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		buf, err := extdb.GetKey(s.DB, []byte("config__"+strings.ToLower(f.Name)))
		if err != nil {
			return err
		}
		switch f.Type.Kind() {
		case reflect.Int:
			fv.SetInt(int64(s2pkg.BytesToFloatZero(buf)))
		case reflect.String:
			fv.SetString(string(buf))
		}
		return nil
	}); err != nil {
		return err
	}
	return s.saveConfig()
}

func (s *Server) saveConfig() error {
	ifZero(&s.CacheSize, 1024)
	ifZero(&s.CacheObjMaxSize, 1024)
	ifZero(&s.WeakCacheSize, 1024)
	ifZero(&s.SlowLimit, 500)
	ifZero(&s.ResponseLogSize, 16)
	ifZero(&s.BatchMaxRun, 50)
	ifZero(&s.CompactLogsTTL, 86400)
	ifZero(&s.CompactLogsDice, 10000)
	ifZero(&s.PingTimeout, 5000)
	if s.ServerName == "" {
		s.ServerName = fmt.Sprintf("UNNAMED_%x", clock.UnixNano())
	}
	if s.Cache == nil {
		s.Cache = s2pkg.NewMasterLRU(int64(s.CacheSize), nil)
	} else {
		s.Cache.SetNewCap(int64(s.CacheSize))
	}
	if s.WeakCache == nil {
		s.WeakCache = s2pkg.NewMasterLRU(int64(s.WeakCacheSize), nil)
	} else {
		s.WeakCache.SetNewCap(int64(s.WeakCacheSize))
	}

	p, err := nj.LoadString(strings.Replace(s.InspectorSource, "\r", "", -1), s.getScriptEnviron())
	if err != nil {
		return err
	} else if _, err = p.Run(); err != nil {
		return err
	} else {
		s.SelfManager = p
	}

	if changed, err := s.Slave.CreateRedis(s.ServerConfig.Slave); err != nil {
		return err
	} else if changed {
		if s.ServerConfig.Slave != "" {
			log.Info("slave redis created with: ", s.ServerConfig.Slave)
		} else {
			log.Info("slave redis removed")
		}
	}

	return s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		var buf []byte
		switch f.Type {
		case reflect.TypeOf(0):
			buf = s2pkg.FloatToBytes(float64(fv.Int()))
		case reflect.TypeOf(""):
			buf = []byte(fv.String())
		}
		return s.DB.Set([]byte("config__"+strings.ToLower(f.Name)), buf, pebble.Sync)
	})
}

func (s *Server) UpdateConfig(key, value string, force bool) (bool, error) {
	key = strings.ToLower(key)
	if key == "servername" && !regexp.MustCompile(`[a-zA-Z0-9_]+`).MatchString(value) {
		return false, fmt.Errorf("invalid char in server name")
	}
	if key == "slave" && !strings.HasPrefix(value, "redis://") && value != "" {
		value = "redis://" + value
	}
	found := false
	old := s.ServerConfig
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		if strings.ToLower(f.Name) != key {
			return nil
		}
		old := fmt.Sprint(fv.Interface())
		if old == value {
			found = true
			return nil
		}
		switch f.Type {
		case reflect.TypeOf(0):
			fv.SetInt(int64(s2pkg.ParseInt(value)))
		case reflect.TypeOf(""):
			fv.SetString(value)
		}
		found = true
		return nil
	})
	if found {
		if err := s.saveConfig(); err != nil {
			s.ServerConfig = old
			return false, err
		}
	}
	return found, nil
}

func (s *Server) GetConfig(key string) (v string, ok bool) {
	fast := reflect.ValueOf(&s.ServerConfig).Elem().FieldByName(key)
	if fast.IsValid() {
		return fmt.Sprint(fast.Interface()), true
	}
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		if strings.ToLower(f.Name) == key {
			v, ok = fmt.Sprint(fv.Interface()), true
		}
		return nil
	})
	return
}

func (s *Server) listConfigCommand() (list []string) {
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		list = append(list, strings.ToLower(f.Name), fmt.Sprint(fv.Interface()))
		return nil
	})
	return list
}

func (s *Server) configForEachField(cb func(reflect.StructField, reflect.Value) error) error {
	rv := reflect.ValueOf(&s.ServerConfig)
	rt := reflect.TypeOf(s.ServerConfig)
	for i := 0; i < rt.NumField(); i++ {
		if err := cb(rt.Field(i), rv.Elem().Field(i)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) getRedis(addr string) (cli *redis.Client) {
	switch addr {
	case "", "local", "LOCAL":
		return s.LocalRedis
	case "slave", "SLAVE":
		return s.Slave.Redis()
	}
	cfg, err := wire.ParseConnString(addr)
	s2pkg.PanicErr(err)
	if cli, ok := s.rdbCache.Get(cfg.Raw); ok {
		return cli.(*redis.Client)
	}
	cli = redis.NewClient(cfg.Options)
	s.rdbCache.Delete(cfg.Addr)
	s.rdbCache.Add(cfg.Addr, cfg.Raw, cli, 0)
	return
}

func (s *Server) CopyConfig(remoteAddr, key string) (finalErr error) {
	rdb := s.getRedis(remoteAddr)

	s.configForEachField(func(rf reflect.StructField, rv reflect.Value) error {
		switch rf.Name {
		case "ServerName", "MarkMaster", "Slave", "Passthrough":
			return nil
		}
		if key != "" && !strings.EqualFold(rf.Name, key) {
			return nil
		}
		cmd := redis.NewStringSliceCmd(context.TODO(), "CONFIG", "GET", rf.Name)
		rdb.Process(context.TODO(), cmd)
		if cmd.Err() != nil {
			finalErr = errors.CombineErrors(finalErr, fmt.Errorf("get(%q): %v", rf.Name, cmd.Err()))
			return nil
		}
		v := cmd.Val()
		if len(v) != 2 {
			finalErr = errors.CombineErrors(finalErr, fmt.Errorf("get(%q): %v", rf.Name, v))
			return nil
		}
		_, err := s.UpdateConfig(rf.Name, v[1], false)
		if err != nil {
			finalErr = errors.CombineErrors(finalErr, fmt.Errorf("update(%q): %v", rf.Name, err))
			return nil
		}
		return nil
	})
	return finalErr
}

func (s *Server) runScriptFunc(name string, args ...interface{}) (bas.Value, error) {
	if s.SelfManager == nil {
		return bas.Nil, nil
	}
	defer s2pkg.Recover(nil)
	f, _ := s.SelfManager.Get(name)
	if !bas.IsCallable(f) {
		return f, nil
	}
	in := make([]bas.Value, len(args))
	for i := range in {
		in[i] = bas.ValueOf(args[i])
	}
	res, err := bas.Call2(f.Object(), in...)
	if err != nil {
		log.Errorf("runScript(%s): %v", name, err)
	}
	return res, err
}

func (s *Server) getScriptEnviron(args ...[]byte) *bas.Environment {
	var a []bas.Value
	for _, arg := range args {
		a = append(a, bas.Str(string(arg)))
	}
	return &bas.Environment{
		Globals: bas.NewObject(0).
			SetProp("server", bas.ValueOf(s)).
			SetProp("ctx", bas.ValueOf(context.TODO())).
			SetProp("args", bas.Array(a...)).
			SetMethod("flags", func(env *bas.Env) {
				cmd := wire.Command{}
				for _, v := range env.Stack() {
					cmd.Argv = append(cmd.Argv, bas.ToReadonlyBytes(v))
				}
				env.A = bas.ValueOf(cmd.Flags(0))
			}).
			SetMethod("log", func(env *bas.Env) {
				x := bytes.Buffer{}
				for _, a := range env.Stack() {
					x.WriteString(a.String() + " ")
				}
				log.Info("[logIO] ", x.String())
			}).
			SetMethod("shardOf", func(e *bas.Env) {
				e.A = bas.Int(shardIndex(e.Str(0)))
			}).
			SetMethod("atof", func(e *bas.Env) {
				v := s2pkg.MustParseFloat(e.Str(0))
				e.A = bas.Float64(v)
			}).
			SetMethod("hashmb", func(e *bas.Env) {
				v := make([][]byte, 0, e.Size())
				for i := range v {
					v = append(v, bas.ToReadonlyBytes(e.Get(i)))
				}
				e.A = bas.Str(s2pkg.HashMultiBytes(v))
			}).
			SetMethod("cmd", func(e *bas.Env) { //  func(addr string, args ...interface{}) interface{} {
				var args []interface{}
				for i := 1; i < e.Size(); i++ {
					args = append(args, e.Interface(i))
				}
				v, err := s.getRedis(e.Str(0)).Do(context.TODO(), args...).Result()
				if err != nil {
					if err == redis.Nil {
						e.A = bas.Nil
						return
					}
					panic(err)
				}
				e.A = bas.ValueOf(v)
			}),
	}
}

type LocalStorage struct{ db *pebble.DB }

func (s *Server) LocalStorage() *LocalStorage {
	return &LocalStorage{db: s.DB}
}

func (s *LocalStorage) Get(k string) (string, error) {
	v, err := extdb.GetKey(s.db, []byte("local___"+k))
	return string(v), err
}

func (s *LocalStorage) GetInt64(k string) (v int64, err error) {
	vs, err := s.Get(k)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(vs, 10, 64)
}

func (s *LocalStorage) Set(k string, v interface{}) (err error) {
	return s.db.Set([]byte("local___"+k), []byte(fmt.Sprint(v)), pebble.Sync)
}

func (s *LocalStorage) Delete(k string) (err error) {
	return s.db.Delete([]byte("local___"+k), pebble.Sync)
}

func ifZero(v *int, v2 int) {
	if *v <= 0 {
		*v = v2
	}
}

func ifInt(v bool, a, b int64) int64 {
	if v {
		return a
	}
	return b
}
