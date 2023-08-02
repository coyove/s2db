package server

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/wire"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type ServerConfig struct {
	ServerName             string
	Password               string
	Peer0, Peer1, Peer2    string
	Peer3, Peer4, Peer5    string
	Peer6, Peer7, Peer8    string
	Peer9, Peer10, Peer11  string
	ListFillCacheSize      int
	ListWatermarkCacheSize int
	ListRetentionDays      int
	SlowLimit              int // ms
	TimeoutPeer            int // ms
	TimeoutRange           int // ms
	TimeoutCount           int // ms
	BatchLimit             int
	PipelineLimit          int
	L6WorkerMaxTx          string
	MetricsEndpoint        string
	InspectorSource        string
}

func init() {
	bas.AddTopValue("ctx", bas.ValueOf(context.TODO()))
	bas.AddTopFunc("log", func(env *bas.Env) {
		x := bytes.Buffer{}
		for _, a := range env.Stack() {
			x.WriteString(a.String() + " ")
		}
		log.Info("[logIO] ", x.String())
	})
	bas.AddTopFunc("atof", func(e *bas.Env) {
		v := s2.MustParseFloat(e.Str(0))
		e.A = bas.Float64(v)
	})
}

func (s *Server) loadConfig() error {
	if err := s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		buf, err := s.Get([]byte("config__" + strings.ToLower(f.Name)))
		if err != nil {
			return err
		}
		switch f.Type.Kind() {
		case reflect.Int:
			fv.SetInt(int64(s2.BytesToFloatZero(buf)))
		case reflect.String:
			fv.SetString(string(buf))
		}
		return nil
	}); err != nil {
		return err
	}
	return s.saveConfig("load")
}

func (s *Server) saveConfig(source string) error {
	ifZero(&s.Config.ListFillCacheSize, 100000)
	ifZero(&s.Config.ListWatermarkCacheSize, 1024*1024)
	ifZero(&s.Config.SlowLimit, 500)
	ifZero(&s.Config.TimeoutPeer, 50)
	ifZero(&s.Config.TimeoutRange, 500)
	ifZero(&s.Config.TimeoutCount, 1000)
	ifZero(&s.Config.BatchLimit, 100)
	ifZero(&s.Config.PipelineLimit, 1000)
	if s.Config.L6WorkerMaxTx == "" {
		s.Config.L6WorkerMaxTx = "5000,1000"
	}
	if s.Config.ServerName == "" {
		s.Config.ServerName = fmt.Sprintf("UNNAMED_%x", future.UnixNano())
	}

	s.fillCache = s2.NewLRUShardCache[struct{}](s.Config.ListFillCacheSize)
	s.wmCache = s2.NewLRUShardCache[[16]byte](s.Config.ListWatermarkCacheSize)

	p, err := nj.LoadString(strings.Replace(s.Config.InspectorSource, "\r", "", -1), s.getScriptEnviron())
	if err != nil {
		return err
	} else if out := p.Run(); out.IsError() {
		return out.Error()
	} else {
		s.SelfManager = p
	}

	for i := range s.Peers {
		x := reflect.ValueOf(s.Config).FieldByName("Peer" + strconv.Itoa(i)).String()
		if changed, err := s.Peers[i].Set(x); err != nil {
			return err
		} else if changed {
			log.Infof("[%s] peer #%d created/removed with %q", source, i, x)
		}
	}

	return s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		var buf []byte
		switch f.Type {
		case reflect.TypeOf(0):
			buf = s2.FloatToBytes(float64(fv.Int()))
		case reflect.TypeOf(""):
			buf = []byte(fv.String())
		}
		return s.DB.Set([]byte("config__"+strings.ToLower(f.Name)), buf, pebble.Sync)
	})
}

func (s *Server) UpdateConfig(key, value string, force bool) (bool, error) {
	if strings.EqualFold(key, "servername") && !regexp.MustCompile(`[a-zA-Z0-9_]+`).MatchString(value) {
		return false, fmt.Errorf("invalid char in server name")
	}
	found := false
	old := s.Config
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		if !strings.EqualFold(f.Name, key) {
			return nil
		}
		old := fmt.Sprint(fv.Interface())
		if old == value {
			found = true
			return nil
		}
		switch f.Type {
		case reflect.TypeOf(0):
			fv.SetInt(int64(s2.ParseInt(value)))
		case reflect.TypeOf(""):
			fv.SetString(value)
		}
		found = true
		return nil
	})
	if found {
		if err := s.saveConfig("update"); err != nil {
			s.Config = old
			return false, err
		}
	}
	return found, nil
}

func (s *Server) GetConfig(key string) (v string, ok bool) {
	if strings.EqualFold(key, "channel") {
		return strconv.Itoa(int(s.Channel)), true
	}
	fast := reflect.ValueOf(&s.Config).Elem().FieldByName(key)
	if fast.IsValid() {
		return fmt.Sprint(fast.Interface()), true
	}
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		if strings.EqualFold(f.Name, key) {
			v, ok = fmt.Sprint(fv.Interface()), true
		}
		return nil
	})
	return
}

func (s *Server) execListConfig() (list []string) {
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		if strings.HasPrefix(f.Name, "Peer") {
			if fv.String() != "" {
				list = append(list, strings.ToLower(f.Name), fv.String())
			}
		} else {
			list = append(list, strings.ToLower(f.Name), fmt.Sprint(fv.Interface()))
		}
		return nil
	})
	return list
}

func (s *Server) configForEachField(cb func(reflect.StructField, reflect.Value) error) error {
	rv := reflect.ValueOf(&s.Config)
	rt := reflect.TypeOf(s.Config)
	for i := 0; i < rt.NumField(); i++ {
		if err := cb(rt.Field(i), rv.Elem().Field(i)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) getRedis(addr string) (cli *redis.Client) {
	if !strings.HasPrefix(addr, "redis://") {
		addr = "redis://" + addr
	}
	if cli, ok := s.rdbCache.Get(addr); ok {
		return cli
	}
	cfg, err := wire.ParseConnString(addr)
	if err != nil {
		panic(err)
	}
	cli = cfg.GetClient()
	s.rdbCache.Add(cfg.URI, cli)
	return
}

func (s *Server) CopyConfig(remoteAddr, key string) (finalErr error) {
	rdb := s.getRedis(remoteAddr)

	s.configForEachField(func(rf reflect.StructField, rv reflect.Value) error {
		switch rf.Name {
		case "ServerName":
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
	// if s.SelfManager == nil {
	return bas.Nil, nil
	// }
	// defer s2.Recover(nil)
	// f, _ := s.SelfManager.Get(name)
	// if !f.IsObject() {
	// 	return f, nil
	// }
	// in := make([]bas.Value, len(args))
	// for i := range in {
	// 	in[i] = bas.ValueOf(args[i])
	// }
	// res := f.Object().TryCall(nil, in...)
	// if res.IsError() {
	// 	log.Errorf("runScript(%s): %v", name, res)
	// 	return bas.Nil, res.Error()
	// }
	// return res, nil
}

func (s *Server) mustRunCode(code string, args ...[]byte) bas.Value {
	return nj.MustRun(nj.LoadString(code, s.getScriptEnviron(args...)))
}

func (s *Server) getScriptEnviron(args ...[]byte) *nj.LoadOptions {
	ssRef := func(b [][]byte) (keys []string) {
		for i, b := range b {
			keys = append(keys, "")
			*(*[2]uintptr)(unsafe.Pointer(&keys[i])) = *(*[2]uintptr)(unsafe.Pointer(&b))
		}
		return keys
	}

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
