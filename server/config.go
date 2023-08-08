package server

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/s2/config"
	"github.com/coyove/s2db/s2/resp"
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
	ReadTimeoutRetry       int
	SyncBatchLimit         int
	PipelineLimit          int
	L6WorkerMaxTx          string
	L6PurgerSleepSecs      int
	L6DeduperSleepSecs     int
	MetricsEndpoint        string
	InfluxDB1Config        string
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
	if err := config.Load(s.DBPath+".conf", &s.Config); err != nil {
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
	ifZero(&s.Config.SyncBatchLimit, 100)
	ifZero(&s.Config.PipelineLimit, 1000)
	ifZero(&s.Config.L6PurgerSleepSecs, 3600)
	ifZero(&s.Config.L6DeduperSleepSecs, 75)
	if s.Config.L6WorkerMaxTx == "" {
		s.Config.L6WorkerMaxTx = "5000,1000"
	}
	if s.Config.ServerName == "" {
		s.Config.ServerName = fmt.Sprintf("UNNAMED_%x", future.UnixNano())
	}

	s.fillCache = s2.NewLRUShardCache[struct{}](s.Config.ListFillCacheSize)
	s.wmCache = s2.NewLRUShardCache[[16]byte](s.Config.ListWatermarkCacheSize)

	for i := range s.Peers {
		x := reflect.ValueOf(s.Config).FieldByName("Peer" + strconv.Itoa(i)).String()
		if changed, err := s.Peers[i].Set(x); err != nil {
			return err
		} else if changed {
			log.Infof("[%s] peer #%d created/removed with %q", source, i, x)
		}
	}

	if s.Config.InfluxDB1Config != "" {
		if err := s.initInfluxDB1Client(s.Config.InfluxDB1Config); err != nil {
			return err
		}
	}

	return config.Save(s.DBPath+".conf", &s.Config)
}

func (s *Server) UpdateConfig(key, value string) (bool, error) {
	old := s.Config
	_, found := s.Config.UpdateField(key, value)
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
	s.Config.ForEachField(func(f reflect.StructField, fv reflect.Value) error {
		if strings.EqualFold(f.Name, key) {
			v, ok = fmt.Sprint(fv.Interface()), true
		}
		return nil
	})
	return
}

func (s *Server) execListConfig() (list []string) {
	s.Config.ForEachField(func(f reflect.StructField, fv reflect.Value) error {
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

func (s *ServerConfig) ForEachField(cb func(reflect.StructField, reflect.Value) error) error {
	rv := reflect.ValueOf(s).Elem()
	rt := reflect.TypeOf(s).Elem()
	for i := 0; i < rt.NumField(); i++ {
		if err := cb(rt.Field(i), rv.Field(i)); err != nil {
			return err
		}
	}
	return nil
}

func (s *ServerConfig) UpdateField(key string, value string) (prev any, found bool) {
	s.ForEachField(func(f reflect.StructField, fv reflect.Value) error {
		if !strings.EqualFold(f.Name, key) {
			return nil
		}

		found, prev = true, fv.Interface()
		if fmt.Sprint(prev) == value {
			return nil
		}

		switch f.Type.Kind() {
		case reflect.Int:
			fv.SetInt(int64(s2.ParseInt(value)))
		case reflect.String:
			fv.SetString(value)
		}
		return nil
	})
	return
}

func (s *Server) getRedis(addr string) (cli *redis.Client) {
	if !strings.HasPrefix(addr, "redis://") {
		addr = "redis://" + addr
	}
	if cli, ok := s.rdbCache.Get(addr); ok {
		return cli
	}
	cfg, err := resp.ParseConnString(addr)
	if err != nil {
		panic(err)
	}
	cli = cfg.GetClient()
	s.rdbCache.Add(cfg.URI, cli)
	return
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
