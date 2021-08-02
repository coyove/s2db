package main

import (
	"fmt"
	"math"
	"strings"

	"gitlab.litatom.com/zhangzezhong/zset/redisproto"
	"go.etcd.io/bbolt"
)

func (s *Server) pick(name string) *bbolt.DB {
	return s.db[hashStr(name)%uint64(len(s.db))].DB
}

func (s *Server) writeLog(tx *bbolt.Tx, dd []byte) error {
	bkWal, err := tx.CreateBucketIfNotExists([]byte("wal"))
	if err != nil {
		return err
	}
	id, _ := bkWal.NextSequence()
	return bkWal.Put(intToBytes(id), dd)
}

func (s *Server) parseZAdd(cmd, name string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	var xx, nx, ch, data bool
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
		}
		break
	}

	pairs := []Pair{}
	if !data {
		for i := idx; i < command.ArgCount(); i += 2 {
			s := atof2p(command.At(i))
			pairs = append(pairs, Pair{Key: command.Get(i + 1), Score: s})
		}
	} else {
		for i := idx; i < command.ArgCount(); i += 3 {
			s := atof2p(command.At(i))
			pairs = append(pairs, Pair{Key: command.Get(i + 1), Score: s, Data: command.At(i + 2)})
		}
	}
	return s.prepareZAdd(name, pairs, nx, xx, ch, dumpCommand(command))
}

func (s *Server) parseDel(cmd, name string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	dd := dumpCommand(command)
	switch cmd {
	case "DEL":
		return s.prepareDel(name, dd)
	case "ZREM":
		return s.prepareZRem(name, restCommandsToKeys(2, command), dd)
	}
	start, end := command.Get(2), command.Get(3)
	switch cmd {
	case "ZREMRANGEBYLEX":
		return s.prepareZRemRangeByLex(name, start, end, dd)
	case "ZREMRANGEBYSCORE":
		return s.prepareZRemRangeByScore(name, start, end, dd)
	case "ZREMRANGEBYRANK":
		return s.prepareZRemRangeByRank(name, atoip(start), atoip(end), dd)
	default:
		panic(-1)
	}
}

func (s *Server) parseZIncrBy(cmd, name string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	by := atof2p(command.At(2))
	return s.prepareZIncrBy(name, command.Get(3), by, dumpCommand(command))
}

func (s *Server) ZCard(name string) (int64, error) {
	count := 0
	err := s.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset." + name))
		if bk == nil {
			return nil
		}
		count = bk.KeyN()
		return nil
	})
	return int64(count), err
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
				scores[i] = bytesToFloat(scoreBuf)
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

func (s *Server) deletePair(tx *bbolt.Tx, name string, pairs []Pair, dd []byte) error {
	bkName := tx.Bucket([]byte("zset." + name))
	if bkName == nil {
		return nil
	}
	bkScore := tx.Bucket([]byte("zset.score." + name))
	if bkScore == nil {
		return nil
	}
	for _, p := range pairs {
		if err := bkName.Delete([]byte(p.Key)); err != nil {
			return err
		}
		if err := bkScore.Delete([]byte(string(floatToBytes(p.Score)) + p.Key)); err != nil {
			return err
		}
	}
	return s.writeLog(tx, dd)
}
