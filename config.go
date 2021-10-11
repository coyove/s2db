package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/common/lru"
	"github.com/go-redis/redis/v8"
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
	CompactTmpDir        string
	CompactNoBackup      int // disable backup files when compacting, dangerous when you are master
	CompactRunWait       int // see runTask()
	CompactFreelistLimit int // compact when freelist is too large
	FillPercent          int // 1~10 will be translated to 0.1~1.0 and 0 means bbolt default (0.5)
	StopLogPull          int
	QueueTTLSec          int
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
	if err := s.configDB.Update(func(tx *bbolt.Tx) error {
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

	s.cache = newKeyedCache(int64(s.CacheSize) * 1024 * 1024)
	s.weakCache = lru.NewCache(int64(s.WeakCacheSize) * 1024 * 1024)

	return s.configDB.Update(func(tx *bbolt.Tx) error {
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
	s.configForEachField(func(f reflect.StructField, fv reflect.Value) error {
		if strings.ToLower(f.Name) != key {
			return nil
		}
		old := fmt.Sprint(fv.Interface())
		switch f.Type {
		case reflect.TypeOf(0):
			fv.SetInt(int64(atoi(value)))
		case reflect.TypeOf(""):
			fv.SetString(value)
		}
		found = true
		s.configDB.Update(func(tx *bbolt.Tx) error {
			bk, err := tx.CreateBucketIfNotExists([]byte("_configlog"))
			if err != nil {
				return err
			}
			buf, _ := json.Marshal(map[string]string{"key": f.Name, "old": old, "new": value, "ts": fmt.Sprint(time.Now().Unix())})
			return bk.Put(intToBytes(uint64(time.Now().UnixNano())), buf)
		})
		return fmt.Errorf("exit")
	})
	return found, s.saveConfig()
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
		list = append(list, name+":"+value)
		return nil
	})
	return list
}

func (s *Server) listConfigLogs(n int) (logs []string) {
	s.configDB.View(func(tx *bbolt.Tx) error {
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

func (s *Server) duplicateConfig(remoteAddr string) error {
	config := &redis.Options{Addr: remoteAddr}
	if strings.Contains(remoteAddr, "@") {
		parts := strings.SplitN(remoteAddr, "@", 2)
		config.Password = parts[0]
		config.Addr = parts[1]
	}
	if strings.EqualFold(remoteAddr, "MASTER") {
		config.Addr = s.MasterAddr
		config.Password = s.MasterPassword
	}
	rdb := redis.NewClient(config)
	defer rdb.Close()

	errBuf := bytes.Buffer{}
	s.configForEachField(func(rf reflect.StructField, rv reflect.Value) error {
		if strings.HasPrefix(rf.Name, "ShardPath") || rf.Name == "ServerName" {
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
	x.compactLock.Lock()
	defer x.compactLock.Unlock()
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
