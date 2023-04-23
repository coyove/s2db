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
	"github.com/coyove/s2db/bitmap"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/extdb"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type ServerConfig struct {
	ServerName                 string
	Password                   string
	Peer0, Peer1, Peer2, Peer3 string
	Peer4, Peer5, Peer6, Peer7 string
	CacheSize                  int
	CacheObjMaxSize            int // kb
	SlowLimit                  int // ms
	PingTimeout                int // ms
	ResponseLogSize            int // kb
	BatchMaxRun                int
	BatchFirstRunSleep         int // ms
	TCPWriteTimeout            int // ms
	CompactLogsTTL             int // sec
	CompactLogsDice            int
	PushLogsDice               int
	MetricsEndpoint            string
	InspectorSource            string
}

func init() {
	bas.AddTopValue("ctx", bas.ValueOf(context.TODO()))
	bas.AddTopFunc("flags", func(env *bas.Env) {
		cmd := wire.Command{}
		for _, v := range env.Stack() {
			cmd.Argv = append(cmd.Argv, toReadonlyBytes(v))
		}
		env.A = bas.ValueOf(cmd.Flags(0))
	})
	bas.AddTopFunc("log", func(env *bas.Env) {
		x := bytes.Buffer{}
		for _, a := range env.Stack() {
			x.WriteString(a.String() + " ")
		}
		log.Info("[logIO] ", x.String())
	})
	bas.AddTopFunc("shardOf", func(e *bas.Env) {
		e.A = bas.Int(shardIndex(e.Str(0)))
	})
	bas.AddTopFunc("atof", func(e *bas.Env) {
		v := s2pkg.MustParseFloat(e.Str(0))
		e.A = bas.Float64(v)
	})
	bas.AddTopFunc("hashmb", func(e *bas.Env) {
		v := make([][]byte, 0, e.Size())
		for i := range v {
			v = append(v, toReadonlyBytes(e.Get(i)))
		}
		e.A = bas.Int64(int64(s2pkg.HashMultiBytes(v)))
	})
	bas.AddTopFunc("bfparse", func(e *bas.Env) {
		bf, err := bitmap.BloomFilterUnmarshalBinary(toReadonlyBytes(e.Get(0)))
		s2pkg.PanicErr(err)
		e.A = bas.ValueOf(bf)
	})
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
	if s.CacheObjMaxSize == 0 {
		s.CacheObjMaxSize = 1
	}
	ifZero(&s.SlowLimit, 500)
	ifZero(&s.ResponseLogSize, 16)
	ifZero(&s.BatchMaxRun, 50)
	ifZero(&s.CompactLogsTTL, 86400)
	ifZero(&s.CompactLogsDice, 10000)
	ifZero(&s.PingTimeout, 5000)
	if s.ServerName == "" {
		s.ServerName = fmt.Sprintf("UNNAMED_%x", clock.UnixNano())
	}

	p, err := nj.LoadString(strings.Replace(s.InspectorSource, "\r", "", -1), s.getScriptEnviron())
	if err != nil {
		return err
	} else if out := p.Run(); out.IsError() {
		return out.Error()
	} else {
		s.SelfManager = p
	}

	for i := range s.Peers {
		x := reflect.ValueOf(s.ServerConfig).FieldByName("Peer" + strconv.Itoa(i)).String()
		if changed, err := s.Peers[i].CreateRedis(x); err != nil {
			return err
		} else if changed {
			log.Infof("peer redis created/removed with: %q", x)
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
	}
	if !strings.HasPrefix(addr, "redis://") {
		addr = "redis://" + addr
	}
	if cli, ok := s.rdbCache.GetSimple(addr); ok {
		return cli.(*redis.Client)
	}
	cfg, err := wire.ParseConnString(addr)
	s2pkg.PanicErr(err)
	cli = redis.NewClient(cfg.Options)
	s.rdbCache.AddSimple(cfg.Raw, cli)
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
	if !f.IsObject() {
		return f, nil
	}
	in := make([]bas.Value, len(args))
	for i := range in {
		in[i] = bas.ValueOf(args[i])
	}
	res := f.Object().TryCall(nil, in...)
	if res.IsError() {
		log.Errorf("runScript(%s): %v", name, res)
		return bas.Nil, res.Error()
	}
	return res, nil
}

func (s *Server) getScriptEnviron(args ...[]byte) *nj.LoadOptions {
	a := bas.ValueOf(ssRef(args))
	return &nj.LoadOptions{
		Globals: bas.NewObject(8).
			SetProp("server", bas.ValueOf(s)).
			SetProp("args", a).SetProp("ARGV", a).
			SetProp("redis", bas.Func("redis", func(e *bas.Env) {
				var args []interface{}
				for i := 1; i < e.Size(); i++ {
					args = append(args, e.Interface(i))
				}
				v, err := s.getRedis(e.Str(0)).Do(context.TODO(), args...).Result()
				if err != nil {
					_ = err == redis.Nil && e.SetA(bas.Nil) || e.SetError(err)
					return
				}
				e.A = bas.ValueOf(v)
			}).Object().
				AddMethod("get", func(e *bas.Env) { e.A = bas.ValueOf(s.getRedis(e.Str(0))) }).
				AddMethod("call", func(e *bas.Env) {
					e.A = e.This().Object().Call(e, append([]bas.Value{bas.NullStr}, e.Stack()...)...)
				}).
				ToValue()).
			ToMap(),
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

func toReadonlyBytes(v bas.Value) []byte {
	if v.IsString() {
		return []byte(v.Str())
	}
	if v.IsBytes() {
		return v.Bytes()
	}
	panic("toReadonlyBytes: invalid data")
}
