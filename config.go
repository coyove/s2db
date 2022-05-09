package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/s2pkg/fts"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
	"golang.org/x/time/rate"
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
	ResponseLogRun     int
	ResponseLogSize    int // kb
	DumpSafeMargin     int // mb
	BatchMaxRun        int
	BatchFirstRunSleep int // ms
	CompactJobType     int
	CompactLogHead     int
	CompactTxSize      int
	CompactTxWorkers   int
	CompactDumpTmpDir  string // use a temporal directory to store dumped shard
	DisableMetrics     int    // 0|1
	InspectorSource    string
	QAppendQPSLimiter  int
}

func (s *Server) loadConfig() error {
	if err := s.ConfigDB.Update(func(tx *bbolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte("_config"))
		if err != nil {
			return err
		}
		s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
			buf := bk.Get([]byte(strings.ToLower(f.Name)))
			switch f.Type {
			case reflect.TypeOf(0):
				fv.SetInt(int64(s2pkg.BytesToFloatZero(buf)))
			case reflect.TypeOf(""):
				fv.SetString(string(buf))
			}
			return nil
		})
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
	ifZero(&s.ResponseLogRun, 200)
	ifZero(&s.ResponseLogSize, 16)
	ifZero(&s.BatchMaxRun, 50)
	ifZero(&s.CompactLogHead, 1500)
	ifZero(&s.CompactTxSize, 20000)
	ifZero(&s.CompactTxWorkers, 1)
	ifZero(&s.DumpSafeMargin, 16)
	ifZero(&s.PingTimeout, 5000)
	if s.ServerName == "" {
		s.ServerName = fmt.Sprintf("UNNAMED_%x", time.Now().UnixNano())
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

	if ql := s.ServerConfig.QAppendQPSLimiter; ql > 0 {
		s.QAppendLimiter = rate.NewLimiter(rate.Limit(ql), ql*2)
	} else {
		s.QAppendLimiter = nil
	}

	return s.ConfigDB.Update(func(tx *bbolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte("_config"))
		if err != nil {
			return err
		}
		return s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
			var buf []byte
			switch f.Type {
			case reflect.TypeOf(0):
				buf = s2pkg.FloatToBytes(float64(fv.Int()))
			case reflect.TypeOf(""):
				buf = []byte(fv.String())
			}
			return bk.Put([]byte(strings.ToLower(f.Name)), buf)
		})
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
		s.ConfigDB.Update(func(tx *bbolt.Tx) error {
			bk, err := tx.CreateBucketIfNotExists([]byte("_configlog"))
			if err != nil {
				return err
			}
			buf, _ := json.Marshal(map[string]string{"key": f.Name, "old": old, "new": value, "ts": fmt.Sprint(time.Now().Unix())})
			return bk.Put(s2pkg.Uint64ToBytes(uint64(time.Now().UnixNano())), buf)
		})
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

func (s *Server) getConfig(key string) (v string, ok bool) {
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

func (s *Server) listConfig() (list []string) {
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		list = append(list, strings.ToLower(f.Name), fmt.Sprint(fv.Interface()))
		return nil
	})
	return list
}

func (s *Server) listConfigLogs(n int) (logs []string) {
	s.ConfigDB.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("_configlog"))
		if bk == nil {
			return nil
		}
		c := bk.Cursor()
		for k, v := c.Last(); len(k) > 0; k, v = c.Prev() {
			logs = append(logs, string(v))
			if len(logs) >= n {
				break
			}
		}
		return nil
	})
	return
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
	s.rdbCache.Add(cfg.Addr, cfg.Raw, cli)
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
			SetMethod("tokenize", func(e *bas.Env) {
				e.A = bas.ValueOf(fts.SplitSimple(e.Str(0)))
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

type LocalStorage struct{ db *bbolt.DB }

func (s *Server) LocalStorage() *LocalStorage {
	return &LocalStorage{db: s.ConfigDB}
}

func (s *LocalStorage) Get(k string) (v string, err error) {
	err = s.db.View(func(tx *bbolt.Tx) error {
		if bk := tx.Bucket([]byte("_local")); bk != nil {
			v = string(bk.Get([]byte(k)))
		}
		return nil
	})
	return
}

func (s *LocalStorage) GetInt64(k string) (v int64, err error) {
	vs, err := s.Get(k)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(vs, 10, 64)
}

func (s *LocalStorage) Set(k string, v interface{}) (err error) {
	err = s.db.Update(func(tx *bbolt.Tx) error {
		if bk, err := tx.CreateBucketIfNotExists([]byte("_local")); err != nil {
			return err
		} else {
			return bk.Put([]byte(k), []byte(fmt.Sprint(v)))
		}
	})
	return
}

func (s *LocalStorage) Delete(k string) (err error) {
	return s.db.Update(func(tx *bbolt.Tx) error {
		if bk := tx.Bucket([]byte("_local")); bk == nil {
			return nil
		} else {
			return bk.Delete([]byte(k))
		}
	})
}

func (s *Server) GetShardFilename(i int) (dir, fn string, err error) {
	err = s.ConfigDB.View(func(tx *bbolt.Tx) error {
		if bk := tx.Bucket([]byte("_shardsdir")); bk != nil {
			dir = string(bk.Get(s2pkg.Uint64ToBytes(uint64(i))))
		}
		if bk := tx.Bucket([]byte("_shards")); bk != nil {
			fn = string(bk.Get(s2pkg.Uint64ToBytes(uint64(i))))
		}
		return nil
	})
	if err == nil && fn == "" {
		fn = "shard" + strconv.Itoa(i)
	}
	if err == nil && dir == "" {
		dir = s.ConfigDir
	}
	return
}

func makeShardFilename(shard int) string {
	return "shard" + strconv.Itoa(shard) + "." + fmt.Sprintf("%013d", time.Now().UnixNano()/1e6)
}

func (s *Server) UpdateShardFilename(i int, dir, fn string) error {
	if dir == "" || fn == "" {
		return fmt.Errorf("UpdateShardFilename: empty parameters")
	}
	defer func(start time.Time) {
		log.Infof("update shard #%d filename to %q and %q in %v", i, (dir), (fn), time.Since(start))
	}(time.Now())
	return s.ConfigDB.Update(func(tx *bbolt.Tx) error {
		if bk, err := tx.CreateBucketIfNotExists([]byte("_shardsdir")); err != nil {
			return err
		} else if err := bk.Put(s2pkg.Uint64ToBytes(uint64(i)), []byte(dir)); err != nil {
			return err
		}
		if bk, err := tx.CreateBucketIfNotExists([]byte("_shards")); err != nil {
			return err
		} else if err := bk.Put(s2pkg.Uint64ToBytes(uint64(i)), []byte(fn)); err != nil {
			return err
		}
		return nil
	})
}

func (s *Server) appendMetricsPairs(ttl time.Duration) error {
	var pairs []s2pkg.Pair
	start := time.Now()
	now := start.UnixNano() - int64(60*time.Second)
	pairs = append(pairs, s2pkg.Pair{Member: "Connections", Score: float64(s.Survey.Connections)})
	rv, rt := reflect.ValueOf(s.Survey), reflect.TypeOf(s.Survey)
	for i := 0; i < rv.NumField(); i++ {
		if sv, ok := rv.Field(i).Interface().(s2pkg.Survey); ok {
			m, n := sv.Metrics(), rt.Field(i).Name
			pairs = append(pairs, s2pkg.Pair{Member: n + "_Mean", Score: m.Mean[0]}, s2pkg.Pair{Member: n + "_QPS", Score: m.QPS[0]})
		}
	}
	s.Survey.Command.Range(func(k, v interface{}) bool {
		m, n := v.(*s2pkg.Survey).Metrics(), "Cmd"+k.(string)
		pairs = append(pairs, s2pkg.Pair{Member: n + "_Mean", Score: m.Mean[0]}, s2pkg.Pair{Member: n + "_QPS", Score: m.QPS[0]})
		return true
	})
	err := s.ConfigDB.Update(func(tx *bbolt.Tx) error {
		for _, mp := range pairs {
			subbk, err := tx.CreateBucketIfNotExists([]byte("_metrics_" + mp.Member))
			if err != nil {
				return err
			}
			subbk.FillPercent = 0.9
			if err := subbk.Put(s2pkg.Uint64ToBytes(uint64(now)), s2pkg.FloatToBytes(mp.Score)); err != nil {
				return err
			}
			c := subbk.Cursor()
			for k, _ := c.First(); len(k) == 8; k, _ = c.Next() {
				if ts := int64(binary.BigEndian.Uint64(k)); time.Duration(now-ts) <= ttl {
					break
				}
				if err := subbk.Delete(k); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if diff := time.Since(start); diff.Milliseconds() > int64(s.SlowLimit) {
		slowLogger.Infof("#%d\t% 4.3f\t%s\t%v", 0, diff.Seconds(), "127.0.0.1", "metrics")
	}
	return err
}

func (s *Server) ListMetricsNames() (names []string) {
	s.ConfigDB.View(func(tx *bbolt.Tx) error {
		c := tx.Cursor()
		for bkNameBuf, _ := c.Seek([]byte("_metrics_")); bytes.HasPrefix(bkNameBuf, []byte("_metrics_")); bkNameBuf, _ = c.Next() {
			subbk := tx.Bucket(bkNameBuf)
			if subbk == nil {
				continue
			}
			names = append(names, string(bkNameBuf[9:]))
		}
		return nil
	})
	return
}

func (s *Server) GetMetricsPairs(startNano, endNano int64, names ...string) (m []s2pkg.GroupedMetrics, err error) {
	if endNano == 0 && startNano == 0 {
		startNano, endNano = time.Now().UnixNano()-int64(time.Hour), time.Now().UnixNano()
	}
	res := map[string]s2pkg.GroupedMetrics{}
	err = s.ConfigDB.View(func(tx *bbolt.Tx) error {
		c := tx.Cursor()
		getter := func(bkNameBuf []byte) string {
			subbk := tx.Bucket(bkNameBuf)
			if subbk == nil {
				return ""
			}
			bkName := string(bkNameBuf[9:])
			subc := subbk.Cursor() // TODO: fast lookup
			for k, v := subc.First(); len(k) == 8; k, v = subc.Next() {
				ts := int64(binary.BigEndian.Uint64(k))
				if ts >= startNano && ts <= endNano {
					a := res[bkName]
					a.Name = bkName
					vf := s2pkg.BytesToFloat(v)
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
					res[bkName] = a
				}
				if ts > endNano {
					break
				}
			}
			return bkName
		}
		if len(names) > 0 {
			for _, n := range names {
				getter([]byte("_metrics_" + n))
			}
		} else {
			for bkNameBuf, _ := c.Seek([]byte("_metrics_")); bytes.HasPrefix(bkNameBuf, []byte("_metrics_")); bkNameBuf, _ = c.Next() {
				if bkName := getter(bkNameBuf); bkName != "" {
					names = append(names, bkName)
				}
			}
		}
		return nil
	})
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
	return s.ConfigDB.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket([]byte("_metrics_" + name)) })
}
