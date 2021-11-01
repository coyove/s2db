package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/common/lru"
	"github.com/coyove/s2db/redisproto"
	"github.com/coyove/script"
	"github.com/coyove/script/typ"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

type ServerConfig struct {
	ServerName           string
	Password             string
	CacheSize            int
	CacheKeyMaxLen       int
	WeakCacheSize        int
	SlowLimit            int // ms
	ResponseLogRun       int
	ResponseLogSize      int // kb
	BatchMaxRun          int
	SchedCompactJob      string
	CompactLogHead       int
	CompactTxSize        int
	CompactDumpTmpDir    string
	CompactNoBackup      int // disable backup files when compacting, dangerous when you are master
	CompactRunWait       int // see runTask()
	CompactFreelistLimit int // compact when freelist is too large
	StopLogPull          int
	InspectorSource      string
	ShardPath0, ShardPath1, ShardPath2, ShardPath3,
	ShardPath4, ShardPath5, ShardPath6, ShardPath7,
	ShardPath8, ShardPath9, ShardPath10, ShardPath11,
	ShardPath12, ShardPath13, ShardPath14, ShardPath15,
	ShardPath16, ShardPath17, ShardPath18, ShardPath19,
	ShardPath20, ShardPath21, ShardPath22, ShardPath23,
	ShardPath24, ShardPath25, ShardPath26, ShardPath27,
	ShardPath28, ShardPath29, ShardPath30, ShardPath31 string
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
				fv.SetInt(int64(bytesToFloatZero(buf)))
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

	s.Cache = newKeyedCache(int64(s.CacheSize) * 1024 * 1024)
	s.WeakCache = lru.NewCache(int64(s.WeakCacheSize) * 1024 * 1024)

	script.AddGlobalValue("log", func(env *script.Env) {
		x := bytes.Buffer{}
		for _, a := range env.Stack() {
			x.WriteString(a.String() + " ")
		}
		log.Info("[logIO] ", x.String())
	})
	p, err := script.LoadString(strings.Replace(s.InspectorSource, "\r", "", -1), s.getCompileOptions())
	if err != nil {
		log.Error("saveConfig inspector: ", err)
	} else if _, err = p.Run(); err != nil {
		log.Error("saveConfig inspector: ", err)
	} else {
		s.Inspector = p
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
				buf = floatToBytes(float64(fv.Int()))
			case reflect.TypeOf(""):
				buf = []byte(fv.String())
			}
			return bk.Put([]byte(strings.ToLower(f.Name)), buf)
		})
	})
}

func (s *Server) updateConfig(key, value string, force bool) (bool, error) {
	key = strings.ToLower(key)
	if strings.EqualFold(key, "readonly") {
		s.ReadOnly, _ = strconv.ParseBool(value)
		return true, nil
	}
	if strings.HasPrefix(key, "shardpath") && !force {
		return false, fmt.Errorf("shard path cannot be changed directly")
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
			return fmt.Errorf("exit")
		}
		switch f.Type {
		case reflect.TypeOf(0):
			fv.SetInt(int64(atoi(value)))
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
			return bk.Put(intToBytes(uint64(time.Now().UnixNano())), buf)
		})
		return fmt.Errorf("exit")
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
		if strings.HasPrefix(name, "shardpath") && value == "" {
			return nil
		}
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

func (s *Server) getRedis(addr string) *redis.Client {
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
	defer rdb.Close()

	errBuf := bytes.Buffer{}
	s.configForEachField(func(rf reflect.StructField, rv reflect.Value) error {
		if strings.HasPrefix(rf.Name, "ShardPath") || rf.Name == "ServerName" {
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

func (s *Server) remapShard(shard int, newPath string) error {
	if newPath == "" {
		return fmt.Errorf("empty new path")
	}

	os.MkdirAll(newPath, 0777)
	of, err := os.Create(filepath.Join(newPath, "shard"+strconv.Itoa(shard)))
	if err != nil {
		return err
	}

	x := &s.db[shard]
	if v, ok := s.CompactLock.lock(int32(shard) + 1); !ok {
		return fmt.Errorf("previous compaction in the way #%d", v-1)
	}
	defer s.CompactLock.unlock()
	path := x.Path()

	x.DB.Close()
	roDB, err := bbolt.Open(path, 0666, bboltReadonlyOptions)
	if err != nil {
		of.Close()
		return err
	}
	x.DB = roDB

	if err := roDB.View(func(tx *bbolt.Tx) error {
		_, err := tx.WriteTo(of)
		return err
	}); err != nil {
		of.Close()
		return err
	}
	of.Close()
	// Finish dumping shard to the new location

	if _, err := s.updateConfig("ShardPath"+strconv.Itoa(shard), newPath, true); err != nil {
		return err
	}

	rwDB, err := bbolt.Open(of.Name(), 0666, bboltOptions)
	if err != nil {
		return err
	}
	x.DB = rwDB
	return nil
}

func (s *Server) runInspectFunc(name string, args ...interface{}) {
	if s.Inspector == nil {
		return
	}
	defer func() { recover() }()
	f := s.Inspector.GLoad(name)
	if f.Type() != typ.Func {
		return
	}
	v, err := f.Func().CallSimple(args...)
	if err != nil {
		log.Error("[inspector] run ", name, " err=", err)
	}
	if v != script.Nil {
		log.Info("[inspector] debug ", name, " result=", v)
	}
}

func (s *Server) runInspectFuncRet(name string, args ...interface{}) (script.Value, error) {
	if s.Inspector == nil {
		return script.Nil, nil
	}
	defer func() { recover() }()
	f := s.Inspector.GLoad(name)
	if f.Type() != typ.Func {
		return f, nil
	}
	in := make([]script.Value, len(args))
	for i := range in {
		in[i] = script.Val(args[i])
	}
	return f.Func().Call(in...)
}

func (s *Server) getCompileOptions(args ...[]byte) *script.CompileOptions {
	var a []script.Value
	for _, arg := range args {
		a = append(a, script.Bytes(arg))
	}
	return &script.CompileOptions{
		GlobalKeyValues: map[string]interface{}{
			"server": s,
			"args":   script.Array(a...),
			"shardCalc": func(in string) int {
				return shardIndex(in)
			},
			"hashCommands": func(in ...string) [2]uint64 {
				v := make([][]byte, len(in))
				for i := range v {
					v[i] = []byte(in[i])
				}
				return hashCommands(&redisproto.Command{Argv: v})
			},
			"getPendingUnlinks": func(shard int) []string {
				v, err := getPendingUnlinks(s.db[shard].DB)
				if err != nil {
					panic(err)
				}
				return v
			},
			"cmd": func(addr string, args ...interface{}) interface{} {
				rdb := s.getRedis(addr)
				defer rdb.Close()
				v, err := rdb.Do(context.TODO(), args...).Result()
				if err != nil {
					if err == redis.Nil {
						return nil
					}
					panic(err)
				}
				return v
			},
		},
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
