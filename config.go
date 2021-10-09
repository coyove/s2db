package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/common/lru"
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

func (s *Server) updateConfig(key, value string) (bool, error) {
	key = strings.ToLower(key)
	if strings.EqualFold(key, "readonly") {
		s.ReadOnly, _ = strconv.ParseBool(value)
		return true, nil
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
