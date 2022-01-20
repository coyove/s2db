package main

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/internal"
	"github.com/coyove/s2db/redisproto"
	"go.etcd.io/bbolt"
)

func (s *Server) pick(key string) *bbolt.DB {
	return s.db[shardIndex(key)].DB
}

func writeLog(tx *bbolt.Tx, dd []byte) error {
	bk, err := tx.CreateBucketIfNotExists([]byte("wal"))
	if err != nil {
		return err
	}
	bk.FillPercent = 0.9
	id, _ := bk.NextSequence()
	return bk.Put(internal.Uint64ToBytes(id), dd)
}

func parseZAdd(cmd, key string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	var xx, nx, ch, data bool
	var fillPercent float64
	var err error
	var idx = 2
	for ; ; idx++ {
		switch strings.ToUpper(command.Get(idx)) {
		case "XX":
			xx = true
			continue
		case "NX":
			nx = true
			continue
		case "CH":
			ch = true
			continue
		case "DATA":
			data = true
			continue
		case "FILL":
			fillPercent, err = internal.ParseFloat(command.Get(idx + 1))
			internal.PanicErr(err)
			idx++
			continue
		}
		break
	}

	var pairs []internal.Pair
	if !data {
		for i := idx; i < command.ArgCount(); i += 2 {
			s := internal.MustParseFloatBytes(command.Argv[i])
			pairs = append(pairs, internal.Pair{Member: command.Get(i + 1), Score: s})
		}
	} else {
		for i := idx; i < command.ArgCount(); i += 3 {
			s := internal.MustParseFloatBytes(command.Argv[i])
			pairs = append(pairs, internal.Pair{Member: command.Get(i + 1), Score: s, Data: append([]byte{}, command.At(i+2)...)})
		}
	}
	return prepareZAdd(key, pairs, nx, xx, ch, fillPercent, dumpCommand(command))
}

func parseDel(cmd, key string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	dd := dumpCommand(command)
	switch cmd {
	case "DEL":
		return prepareDel(key, dd)
	case "ZREM":
		return prepareZRem(key, restCommandsToKeys(2, command), dd)
	}
	start, end := command.Get(2), command.Get(3)
	switch cmd {
	case "ZREMRANGEBYLEX":
		return prepareZRemRangeByLex(key, start, end, dd)
	case "ZREMRANGEBYSCORE":
		return prepareZRemRangeByScore(key, start, end, dd)
	case "ZREMRANGEBYRANK":
		return prepareZRemRangeByRank(key, internal.MustParseInt(start), internal.MustParseInt(end), dd)
	default:
		panic(-1)
	}
}

func parseZIncrBy(cmd, key string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	by := internal.MustParseFloatBytes(command.Argv[2])
	return prepareZIncrBy(key, command.Get(3), by, dumpCommand(command))
}

func (s *Server) ZCard(key string, flags redisproto.Flags) (count int64) {
	s.pick(key).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset." + key))
		if bk == nil {
			return nil
		}
		if flags.MATCH != "" {
			c := bk.Cursor()
			for k, _ := c.First(); len(k) > 0; k, _ = c.Next() {
				if m, _ := filepath.Match(flags.MATCH, *(*string)(unsafe.Pointer(&k))); m {
					count++
				}
			}
		} else {
			count = int64(bk.KeyN())
		}
		return nil
	})
	return
}

func (s *Server) ZMScore(key string, keys []string, weak time.Duration) (scores []float64, err error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing keys")
	}
	for _, k := range keys {
		if score, ok := s.getWeakCache(internal.HashStr2(k), weak).(float64); ok {
			scores = append(scores, score)
		} else {
			scores = append(scores, math.NaN())
		}
	}
	err = s.pick(key).View(func(tx *bbolt.Tx) error {
		bkName := tx.Bucket([]byte("zset." + key))
		if bkName == nil {
			return nil
		}
		for i, key := range keys {
			if !math.IsNaN(scores[i]) {
				continue
			}
			if scoreBuf := bkName.Get([]byte(key)); len(scoreBuf) != 0 {
				scores[i] = internal.BytesToFloat(scoreBuf)
				h := internal.HashStr2(key)
				s.addWeakCache(h, scores[i], 1)
			}
		}
		return nil
	})
	return
}

func (s *Server) ZMData(key string, keys []string, flags redisproto.Flags) (data [][]byte, err error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing keys")
	}
	data = make([][]byte, len(keys))
	err = s.pick(key).View(func(tx *bbolt.Tx) error {
		func() {
			bkName := tx.Bucket([]byte("zset." + key))
			if bkName == nil {
				return
			}
			bkScore := tx.Bucket([]byte("zset.score." + key))
			if bkScore == nil {
				return
			}
			for i, key := range keys {
				scoreBuf := bkName.Get([]byte(key))
				if len(scoreBuf) != 0 {
					d := bkScore.Get([]byte(string(scoreBuf) + keys[i]))
					data[i] = append([]byte{}, d...)
				}
			}
		}()
		if flags.Command.ArgCount() > 0 {
			s.addCache(key, flags.HashCode(), data)
		}
		return nil
	})
	return
}

func deletePair(tx *bbolt.Tx, key string, pairs []internal.Pair, dd []byte) error {
	bkName := tx.Bucket([]byte("zset." + key))
	bkScore := tx.Bucket([]byte("zset.score." + key))
	if bkScore == nil || bkName == nil {
		return writeLog(tx, dd)
	}
	for _, p := range pairs {
		if err := bkName.Delete([]byte(p.Member)); err != nil {
			return err
		}
		if err := bkScore.Delete([]byte(string(internal.FloatToBytes(p.Score)) + p.Member)); err != nil {
			return err
		}
	}
	return writeLog(tx, dd)
}

func parseQAppend(cmd, key string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	value := append([]byte{}, command.At(2)...)
	flags := command.Flags(3)

	var m func(string) bool
	if flags.MATCH != "" {
		if f := nj.MustRun(nj.LoadString(flags.MATCH, nil)); bas.IsCallable(f) {
			m = func(a string) bool {
				v, _ := bas.Call2(f.Object(), bas.Str(a))
				return v.IsTrue()
			}
		}
	}
	return prepareQAppend(key, value, int64(flags.COUNT), m, dumpCommand(command))
}
