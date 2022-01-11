package main

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/coyove/s2db/internal"
	"github.com/coyove/s2db/redisproto"
	"go.etcd.io/bbolt"
)

func (s *Server) pick(name string) *bbolt.DB {
	return s.db[hashStr(name)%uint64(len(s.db))].DB
}

func writeLog(tx *bbolt.Tx, dd []byte) error {
	bkWal, err := tx.CreateBucketIfNotExists([]byte("wal"))
	if err != nil {
		return err
	}
	bkWal.FillPercent = 0.9
	id, _ := bkWal.NextSequence()
	return bkWal.Put(internal.Uint64ToBytes(id), dd)
}

func parseZAdd(cmd, name string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
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

	pairs := []Pair{}
	if !data {
		for i := idx; i < command.ArgCount(); i += 2 {
			s := internal.MustParseFloatBytes(command.Argv[i])
			pairs = append(pairs, Pair{Key: command.Get(i + 1), Score: s})
		}
	} else {
		for i := idx; i < command.ArgCount(); i += 3 {
			s := internal.MustParseFloatBytes(command.Argv[i])
			pairs = append(pairs, Pair{Key: command.Get(i + 1), Score: s, Data: append([]byte{}, command.At(i+2)...)})
		}
	}
	return prepareZAdd(name, pairs, nx, xx, ch, fillPercent, dumpCommand(command))
}

func parseDel(cmd, name string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	dd := dumpCommand(command)
	switch cmd {
	case "DEL":
		return prepareDel(name, dd)
	case "ZREM":
		return prepareZRem(name, restCommandsToKeys(2, command), dd)
	}
	start, end := command.Get(2), command.Get(3)
	switch cmd {
	case "ZREMRANGEBYLEX":
		return prepareZRemRangeByLex(name, start, end, dd)
	case "ZREMRANGEBYSCORE":
		return prepareZRemRangeByScore(name, start, end, dd)
	case "ZREMRANGEBYRANK":
		return prepareZRemRangeByRank(name, internal.MustParseInt(start), internal.MustParseInt(end), dd)
	default:
		panic(-1)
	}
}

func parseZIncrBy(cmd, name string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	by := internal.MustParseFloatBytes(command.Argv[2])
	return prepareZIncrBy(name, command.Get(3), by, dumpCommand(command))
}

func (s *Server) ZCard(name string, match bool) (count int64, err error) {
	if match {
		for i := range s.db {
			err = s.db[i].View(func(tx *bbolt.Tx) error {
				return tx.ForEach(func(k []byte, bk *bbolt.Bucket) error {
					x := *(*string)(unsafe.Pointer(&k))
					if strings.HasPrefix(x, "zset.score.") {
						if m, _ := filepath.Match(name, x[11:]); m {
							count += int64(bk.KeyN())
						}
					}
					return nil
				})
			})
		}
	} else {
		err = s.pick(name).View(func(tx *bbolt.Tx) error {
			bk := tx.Bucket([]byte("zset." + name))
			if bk == nil {
				return nil
			}
			count = int64(bk.KeyN())
			return nil
		})
	}
	return
}

func (s *Server) ZMScore(name string, keys ...string) (scores []float64, err error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing keys")
	}
	for range keys {
		scores = append(scores, math.NaN())
	}
	err = s.pick(name).View(func(tx *bbolt.Tx) error {
		bkName := tx.Bucket([]byte("zset." + name))
		if bkName == nil {
			return nil
		}
		for i, key := range keys {
			scoreBuf := bkName.Get([]byte(key))
			if len(scoreBuf) != 0 {
				scores[i] = internal.BytesToFloat(scoreBuf)
			}
		}
		return nil
	})
	return
}

func (s *Server) ZMData(name string, keys ...string) (data [][]byte, err error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing keys")
	}
	data = make([][]byte, len(keys))
	err = s.pick(name).View(func(tx *bbolt.Tx) error {
		bkName := tx.Bucket([]byte("zset." + name))
		if bkName == nil {
			return nil
		}
		bkScore := tx.Bucket([]byte("zset.score." + name))
		if bkScore == nil {
			return nil
		}
		for i, key := range keys {
			scoreBuf := bkName.Get([]byte(key))
			if len(scoreBuf) != 0 {
				d := bkScore.Get([]byte(string(scoreBuf) + keys[i]))
				data[i] = append([]byte{}, d...)
			}
		}
		return nil
	})
	return
}

func deletePair(tx *bbolt.Tx, name string, pairs []Pair, dd []byte) error {
	bkName := tx.Bucket([]byte("zset." + name))
	bkScore := tx.Bucket([]byte("zset.score." + name))
	if bkScore == nil || bkName == nil {
		return writeLog(tx, dd)
	}
	for _, p := range pairs {
		if err := bkName.Delete([]byte(p.Key)); err != nil {
			return err
		}
		if err := bkScore.Delete([]byte(string(internal.FloatToBytes(p.Score)) + p.Key)); err != nil {
			return err
		}
	}
	return writeLog(tx, dd)
}

func parseQAppend(cmd, name string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	value := append([]byte{}, command.At(2)...)
	max := int64(internal.ParseInt(command.Get(3)))

	var m func(string) bool
	switch appender := strings.ToUpper(command.Get(4)); appender {
	case "MATCH", "NOTMATCH":
		v := command.Get(5)
		m = func(a string) bool {
			ok, err := filepath.Match(v, a)
			internal.PanicErr(err)
			return ok != (appender == "NOTMATCH")
		}
	}

	return prepareQAppend(name, value, max, m, dumpCommand(command))
}
