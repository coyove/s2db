package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
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
	DisableMetrics     int // 0|1
	InspectorSource    string
}

func (s *Server) loadConfig() error {
	if err := s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		buf, err := s2pkg.GetKeyCopy(s.db, []byte("config__"+strings.ToLower(f.Name)))
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
		return s.db.Set([]byte("config__"+strings.ToLower(f.Name)), buf, pebble.Sync)
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
			return errSafeExit
		}
		switch f.Type {
		case reflect.TypeOf(0):
			fv.SetInt(int64(s2pkg.ParseInt(value)))
		case reflect.TypeOf(""):
			fv.SetString(value)
		}
		found = true
		return errSafeExit
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
	cfg, err := redisproto.ParseConnString(addr)
	s2pkg.PanicErr(err)
	if cli, ok := s.rdbCache.Get(cfg.Raw); ok {
		return cli.(*redis.Client)
	}
	cli = redis.NewClient(cfg.Options)
	s.rdbCache.Delete(cfg.Addr)
	s.rdbCache.Add(cfg.Addr, cfg.Raw, cli, 0)
	return
}

func (s *Server) CopyConfig(remoteAddr, key string) error {
	rdb := s.getRedis(remoteAddr)

	errBuf := bytes.Buffer{}
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
			errBuf.WriteString(fmt.Sprintf("get(%q): %v ", rf.Name, cmd.Err()))
			return nil
		}
		v := cmd.Val()
		if len(v) != 2 {
			errBuf.WriteString(fmt.Sprintf("get(%q): %v ", rf.Name, v))
			return nil
		}
		_, err := s.UpdateConfig(rf.Name, v[1], false)
		if err != nil {
			errBuf.WriteString(fmt.Sprintf("update(%q): %v ", rf.Name, err))
			return nil
		}
		return nil
	})
	if errBuf.Len() > 0 {
		return fmt.Errorf(errBuf.String())
	}
	return nil
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
				cmd := redisproto.Command{}
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
			SetMethod("hashArray", func(e *bas.Env) {
				v := make([][]byte, 0, e.Size())
				for i := range v {
					v = append(v, bas.ToReadonlyBytes(e.Get(i)))
				}
				e.A = bas.Str((redisproto.Command{Argv: v}).HashCode())
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
	return &LocalStorage{db: s.db}
}

func (s *LocalStorage) Get(k string) (string, error) {
	v, err := s2pkg.GetKeyCopy(s.db, []byte("local___"+k))
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

func (s *Server) appendMetricsPairs(ttl time.Duration) error {
	var pairs []s2pkg.Pair
	start := time.Now()
	now := start.UnixNano() - int64(60*time.Second)
	pairs = append(pairs, s2pkg.Pair{Member: "Connections", Score: float64(s.Survey.Connections)})
	rv, rt := reflect.ValueOf(s.Survey), reflect.TypeOf(s.Survey)
	for i := 0; i < rv.NumField(); i++ {
		if sv, ok := rv.Field(i).Interface().(s2pkg.Survey); ok {
			m, n := sv.Metrics(), rt.Field(i)
			if t := n.Tag.Get("metrics"); t == "mean" || t == "" {
				pairs = append(pairs, s2pkg.Pair{Member: n.Name + "_Mean", Score: m.Mean[0]})
			}
			if t := n.Tag.Get("metrics"); t == "qps" || t == "" {
				pairs = append(pairs, s2pkg.Pair{Member: n.Name + "_QPS", Score: m.QPS[0]})
			}
		}
	}
	s.Survey.Command.Range(func(k, v interface{}) bool {
		m, n := v.(*s2pkg.Survey).Metrics(), "Cmd"+k.(string)
		pairs = append(pairs, s2pkg.Pair{Member: n + "_Mean", Score: m.Mean[0]}, s2pkg.Pair{Member: n + "_QPS", Score: m.QPS[0]})
		return true
	})
	pairs = append(pairs, s2pkg.Pair{Member: "AddWatermarkConflict_QPS", Score: s.Cache.AddWatermarkConflict.Metrics().QPS[0]})

	lsmMetrics := s.db.Metrics()
	dbm := reflect.ValueOf(lsmMetrics).Elem()
	rt = dbm.Type()
	for i := 0; i < dbm.NumField(); i++ {
		switch f := dbm.Field(i); f.Kind() {
		case reflect.Struct:
			rft := f.Type()
			for ii := 0; ii < f.NumField(); ii++ {
				pairs = append(pairs, s2pkg.Pair{Member: "DB_" + rt.Field(i).Name + "_" + rft.Field(ii).Name, Score: rvToFloat64(f.Field(ii))})
			}
		case reflect.Array:
		default:
			pairs = append(pairs, s2pkg.Pair{Member: "DB_" + rt.Field(i).Name, Score: rvToFloat64(f)})
		}
	}
	for lv, lvm := range lsmMetrics.Levels {
		rv := reflect.ValueOf(lvm)
		for i := 0; i < rv.NumField(); i++ {
			pairs = append(pairs, s2pkg.Pair{Member: "DB_Level" + strconv.Itoa(lv) + "_" + rv.Type().Field(i).Name,
				Score: rvToFloat64(rv.Field(i))})
		}
	}

	b := s.db.NewBatch()
	for _, mp := range pairs {
		key := []byte("metrics_" + mp.Member + "\x00")
		if err := b.Set(bAppendUint64(key, uint64(now)), s2pkg.FloatToBytes(mp.Score), pebble.Sync); err != nil {
			return err
		}
		if clock.Rand() <= 0.01 {
			if err := b.DeleteRange(key, bAppendUint64(key, uint64(now)-uint64(ttl)), pebble.Sync); err != nil {
				return err
			}
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return err
	}
	if diff := time.Since(start); diff.Milliseconds() > int64(s.SlowLimit) {
		slowLogger.Infof("#%d\t% 4.3f\t%s\t%v", 0, diff.Seconds(), "127.0.0.1", "metrics")
	}
	return nil
}

func (s *Server) ListMetricsNames() (names []string) {
	key := []byte("metrics_")
	c := s.db.NewIter(&pebble.IterOptions{
		LowerBound: key,
		UpperBound: s2pkg.IncBytes(key),
	})
	defer c.Close()
	for c.First(); c.Valid() && bytes.HasPrefix(c.Key(), key); c.Next() {
		k := c.Key()[8:]
		k = k[:bytes.IndexByte(k, 0)]
		names = append(names, string(k))
		c.SeekLT(append(append(key, k...), 0xff))
	}
	return
}

func (s *Server) GetMetricsPairs(startNano, endNano int64, names ...string) (m []s2pkg.GroupedMetrics, err error) {
	if endNano == 0 && startNano == 0 {
		startNano, endNano = clock.UnixNano()-int64(time.Hour), clock.UnixNano()
	}
	res := map[string]s2pkg.GroupedMetrics{}
	getter := func(f string) {
		key := []byte("metrics_" + f + "\x00")
		c := s.db.NewIter(&pebble.IterOptions{
			LowerBound: key,
			UpperBound: s2pkg.IncBytes(key),
		})
		defer c.Close()

		for c.First(); c.Valid() && bytes.HasPrefix(c.Key(), key); c.Next() {
			ts := int64(binary.BigEndian.Uint64(c.Key()[len(key):]))
			if ts >= startNano && ts <= endNano {
				a := res[f]
				a.Name = f
				vf := s2pkg.BytesToFloat(c.Value())
				if math.IsNaN(vf) {
					vf = 0
				}
				tsMin := ts / 1e9 / 60 * 60
				if len(a.Timestamp) > 0 && a.Timestamp[len(a.Timestamp)-1] == tsMin {
					a.Value[len(a.Value)-1] = vf
				} else {
					a.Value = append(a.Value, vf)
					a.Timestamp = append(a.Timestamp, tsMin)
				}
				res[f] = a
			}
			if ts > endNano {
				break
			}
		}
	}
	for _, n := range names {
		getter(n)
	}
	return fillMetricsHoles(res, names, startNano, endNano), err
}

func fillMetricsHoles(res map[string]s2pkg.GroupedMetrics, names []string, startNano, endNano int64) (m []s2pkg.GroupedMetrics) {
	mints, maxts := startNano/1e9/60*60, endNano/1e9/60*60
	for _, name := range names {
		p := res[name]
		for c, ts := 0, mints; ts <= maxts; ts += 60 {
			if c >= len(p.Timestamp) {
				p.Timestamp = append(p.Timestamp, ts)
				p.Value = append(p.Value, 0)
			} else if p.Timestamp[c] != ts {
				p.Timestamp = append(p.Timestamp[:c], append([]int64{ts}, p.Timestamp[c:]...)...)
				p.Value = append(p.Value[:c], append([]float64{0}, p.Value[c:]...)...)
			}
			c++
		}
		m = append(m, p)
	}
	return m
}

func (s *Server) DeleteMetrics(name string) error {
	return s.db.DeleteRange([]byte("metrics_"+name+"\x00"), []byte("metrics_"+name+"\x01"), pebble.Sync)
}

func rvToFloat64(v reflect.Value) float64 {
	if v.Kind() >= reflect.Int && v.Kind() <= reflect.Int64 {
		return float64(v.Int())
	}
	if v.Kind() >= reflect.Uint && v.Kind() <= reflect.Uint64 {
		return float64(v.Uint())
	}
	return 0
}
