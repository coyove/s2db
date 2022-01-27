package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

type ServerConfig struct {
	ServerName        string
	Password          string
	CacheSize         int
	CacheKeyMaxLen    int
	WeakCacheSize     int
	SlowLimit         int // ms
	ResponseLogRun    int
	ResponseLogSize   int // kb
	DumpSafeMargin    int // mb
	BatchMaxRun       int
	CompactJobType    int
	CompactLogHead    int
	CompactTxSize     int
	CompactTxWorkers  int
	CompactDumpTmpDir string
	CompactNoBackup   int // disable backup files when compacting, dangerous when you are master
	StopLogPull       int
	InspectorSource   string
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
	ifZero(&s.CacheKeyMaxLen, 100)
	ifZero(&s.WeakCacheSize, 1024)
	ifZero(&s.SlowLimit, 500)
	ifZero(&s.ResponseLogRun, 200)
	ifZero(&s.ResponseLogSize, 16)
	ifZero(&s.BatchMaxRun, 50)
	ifZero(&s.CompactLogHead, 1500)
	ifZero(&s.CompactTxSize, 20000)
	ifZero(&s.CompactTxWorkers, 1)
	ifZero(&s.DumpSafeMargin, 16)

	s.Cache = s2pkg.NewKeyedLRUCache(int64(s.CacheSize) * 1024 * 1024)
	s.WeakCache = s2pkg.NewLRUCache(int64(s.WeakCacheSize) * 1024 * 1024)

	p, err := nj.LoadString(strings.Replace(s.InspectorSource, "\r", "", -1), s.getScriptEnviron())
	if err != nil {
		log.Error("saveConfig inspector: ", err)
	} else if _, err = p.Run(); err != nil {
		log.Error("saveConfig inspector: ", err)
	} else {
		s.SelfManager = p
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

func (s *Server) updateConfig(key, value string, force bool) (bool, error) {
	key = strings.ToLower(key)
	if key == "servername" && !regexp.MustCompile(`[a-zA-Z0-9_]+`).MatchString(value) {
		return false, fmt.Errorf("invalid char in server name")
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

func (s *Server) listConfig() []string {
	list := []string{"readonly:" + strconv.FormatBool(s.ReadOnly)}
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		name := strings.ToLower(f.Name)
		value := fmt.Sprint(fv.Interface())
		if c := strings.Count(value, "\n"); c > 0 {
			value = strings.TrimSpace(value[:strings.Index(value, "\n")]) + " ...[" + strconv.Itoa(c) + " lines]..."
		}
		list = append(list, name+":"+value)
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
	if c, ok := s.rdbCache.Get(addr); ok {
		cli = c.(*redis.Client)
		return
	}
	defer func() { s.rdbCache.Add(addr, cli) }()

	config := &redis.Options{Addr: addr}
	if addr == "" || strings.EqualFold(addr, "local") {
		config.Network = "unix"
		config.Addr = s.lnLocal.Addr().String()
		return redis.NewClient(config)
	}
	si := s.Slaves.Get(addr)
	if si != nil {
		_, port, _ := net.SplitHostPort(si.ListenAddr)
		config.Addr = si.RemoteAddr + ":" + port
		return redis.NewClient(config)
	}
	if strings.Contains(addr, "@") {
		parts := strings.SplitN(addr, "@", 2)
		config.Password = parts[0]
		config.Addr = parts[1]
	}
	if strings.EqualFold(addr, "MASTER") {
		config.Addr = s.MasterAddr
		config.Password = s.MasterPassword
	}
	return redis.NewClient(config)
}

func (s *Server) CopyConfig(remoteAddr, key string) error {
	rdb := s.getRedis(remoteAddr)

	errBuf := bytes.Buffer{}
	s.configForEachField(func(rf reflect.StructField, rv reflect.Value) error {
		if rf.Name == "ServerName" {
			return nil
		}
		if key != "" && !strings.EqualFold(rf.Name, key) {
			return nil
		}
		cmd := redis.NewStringCmd(context.TODO(), "CONFIG", "GET", rf.Name)
		rdb.Process(context.TODO(), cmd)
		if cmd.Err() != nil {
			errBuf.WriteString(fmt.Sprintf("get(%q): %v ", rf.Name, cmd.Err()))
			return nil
		}
		v := cmd.Val()
		_, err := s.updateConfig(rf.Name, v, false)
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

func (s *Server) runInspectFunc(name string, args ...interface{}) (bas.Value, error) {
	if s.SelfManager == nil {
		return bas.Nil, nil
	}
	defer s2pkg.Recover()
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
			SetProp("args", bas.NewArray(a...).ToValue()).
			SetMethod("log", func(env *bas.Env) {
				x := bytes.Buffer{}
				for _, a := range env.Stack() {
					x.WriteString(a.String() + " ")
				}
				log.Info("[logIO] ", x.String())
			}, "").
			SetMethod("shardCalc", func(e *bas.Env) {
				e.A = bas.Int(shardIndex(e.Str(0)))
			}, "").
			SetMethod("atof", func(e *bas.Env) {
				v := s2pkg.MustParseFloat(e.Str(0))
				e.A = bas.Float64(v)
			}, "").
			SetMethod("hashCommands", func(e *bas.Env) { // ) [2]uint64 {
				v := make([][]byte, 0, e.Size())
				for i := range v {
					v = append(v, e.Get(i).Safe().Bytes())
				}
				e.A = bas.ValueOf((redisproto.Command{Argv: v}).HashCode())
			}, "").
			SetMethod("getPendingUnlinks", func(e *bas.Env) { // ) []string {
				v, err := getPendingUnlinks(s.db[e.Int(0)].DB)
				s2pkg.PanicErr(err)
				e.A = bas.ValueOf(v)
			}, "").
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
			}, ""),
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

func (s *Server) GetShardFilename(i int) (fn string, err error) {
	err = s.ConfigDB.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("_shards"))
		if bk == nil {
			return nil
		}
		fn = string(bk.Get(s2pkg.Uint64ToBytes(uint64(i))))
		return nil
	})
	if err == nil && fn == "" {
		fn = "shard" + strconv.Itoa(i)
	}
	return
}

func (s *Server) UpdateShardFilename(i int, fn string) error {
	return s.ConfigDB.Update(func(tx *bbolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte("_shards"))
		if err != nil {
			return err
		}
		return bk.Put(s2pkg.Uint64ToBytes(uint64(i)), []byte(fn))
	})
}
