package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
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
	return bk.Put(s2pkg.Uint64ToBytes(id), dd)
}

func parseZAdd(cmd, key string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	var xx, nx, ch, data bool
	var fillPercent float64
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
			fillPercent = command.Float64(idx + 1)
			idx++
			continue
		}
		break
	}

	var pairs []s2pkg.Pair
	if !data {
		for i := idx; i < command.ArgCount(); i += 2 {
			pairs = append(pairs, s2pkg.Pair{Member: command.Get(i + 1), Score: command.Float64(i)})
		}
	} else {
		for i := idx; i < command.ArgCount(); i += 3 {
			p := s2pkg.Pair{Score: command.Float64(i), Member: command.Get(i + 1), Data: command.Bytes(i + 2)}
			pairs = append(pairs, p)
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
		return prepareZRemRangeByRank(key, s2pkg.MustParseInt(start), s2pkg.MustParseInt(end), dd)
	default:
		panic(-1)
	}
}

func parseZIncrBy(cmd, key string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	return prepareZIncrBy(key, command.Get(3), command.Float64(2), dumpCommand(command))
}

func (s *Server) ZAdd(key string, deferred bool, members []s2pkg.Pair) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	cmd := &redisproto.Command{Argv: [][]byte{[]byte("ZADD"), []byte(key), []byte("DATA")}}
	for _, m := range members {
		cmd.Argv = append(cmd.Argv, s2pkg.FormatFloatBulk(m.Score), []byte(m.Member), m.Data)
	}
	v, err := s.runPreparedTx("ZADD", key, deferred, prepareZAdd(key, members, false, false, false, 0, dumpCommand(cmd)))
	if err != nil {
		return 0, err
	}
	if deferred {
		return 0, nil
	}
	return int64(v.(int)), nil
}

func (s *Server) ZRem(key string, deferred bool, members []string) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}
	cmd := &redisproto.Command{Argv: [][]byte{[]byte("ZREM"), []byte(key)}}
	for _, m := range members {
		cmd.Argv = append(cmd.Argv, []byte(m))
	}
	v, err := s.runPreparedTx("ZREM", key, deferred, prepareZRem(key, members, dumpCommand(cmd)))
	if err != nil {
		return 0, err
	}
	if deferred {
		return 0, nil
	}
	return v.(int), nil
}

func (s *Server) ZCard(key string) (count int64) {
	s.pick(key).View(func(tx *bbolt.Tx) error {
		if bk := tx.Bucket([]byte("zset.score." + key)); bk != nil {
			count = int64(bk.Sequence())
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
		if score, ok := s.getWeakCache(s2pkg.HashStr2(k), weak).(float64); ok {
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
				scores[i] = s2pkg.BytesToFloat(scoreBuf)
				h := s2pkg.HashStr2(key)
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
		// fillPairsData will call ZMData as well (with an empty Flags), but no cache should be stored
		if flags.Command.ArgCount() > 0 {
			s.addStaticCache(key, flags.HashCode(), data)
		}
		return nil
	})
	return
}

func deletePair(tx *bbolt.Tx, key string, pairs []s2pkg.Pair, dd []byte) error {
	bkName := tx.Bucket([]byte("zset." + key))
	bkScore := tx.Bucket([]byte("zset.score." + key))
	if bkScore == nil || bkName == nil {
		return writeLog(tx, dd)
	}
	for _, p := range pairs {
		if err := bkName.Delete([]byte(p.Member)); err != nil {
			return err
		}
		if err := bkScore.Delete([]byte(string(s2pkg.FloatToBytes(p.Score)) + p.Member)); err != nil {
			return err
		}
	}
	bkScore.SetSequence(bkScore.Sequence() - uint64(len(pairs)))
	return writeLog(tx, dd)
}

func parseQAppend(cmd, key string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	value := command.Bytes(2)
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

func queueLenImpl(bk *bbolt.Bucket) (int64, int64, int64) {
	c := bk.Cursor()
	firstKey, _ := c.First()
	lastKey, _ := c.Last()
	if len(lastKey) == 16 && len(firstKey) == 16 {
		first := binary.BigEndian.Uint64(firstKey)
		last := binary.BigEndian.Uint64(lastKey)
		if first <= last {
			return int64(first), int64(last), int64(last - first + 1)
		}
		panic(-2)
	}
	return 0, 0, 0
}

func (s *Server) QLength(key string) (count int64, err error) {
	err = s.pick(key).View(func(tx *bbolt.Tx) error {
		if bk := tx.Bucket([]byte("q." + key)); bk != nil {
			_, _, count = queueLenImpl(bk)
		}
		return nil
	})
	return count, err
}

func (s *Server) QScan(key string, start, n int64, flags redisproto.Flags) (data []s2pkg.Pair, err error) {
	desc := false
	if n < 0 {
		n = -n
		desc = true
	}
	if n > int64(s2pkg.RangeHardLimit) {
		n = int64(s2pkg.RangeHardLimit)
	}
	err = s.pick(key).View(func(tx *bbolt.Tx) error {
		err := func() error {
			bk := tx.Bucket([]byte("q." + key))
			if bk == nil {
				return nil
			}
			first, last, count := queueLenImpl(bk)
			if count == 0 {
				return nil
			}

			if start <= 0 {
				start = last + start
			} else {
				start = first + start - 1
			}

			if start < first || start > last {
				return nil
			}

			c := bk.Cursor()
			startBuf := s2pkg.Uint64ToBytes(uint64(start))
			k, v := c.Seek(startBuf)
			if !bytes.HasPrefix(k, startBuf) {
				return fmt.Errorf("fatal: missing key")
			}
			for len(data) < int(n) && len(k) == 16 {
				idx := binary.BigEndian.Uint64(k[:8])
				data = append(data, s2pkg.Pair{Member: string(v), Score: float64(idx)})
				if desc {
					k, v = c.Prev()
				} else {
					k, v = c.Next()
				}
			}
			return nil
		}()
		if err == nil {
			s.addStaticCache(key, flags.HashCode(), data)
		}
		return err
	})
	return data, err
}

func (s *Server) QGet(key string, idx int64) ([]byte, error) {
	var data []byte
	err := s.pick(key).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("q." + key))
		if bk == nil {
			return nil
		}
		first, last, count := queueLenImpl(bk)
		if count == 0 {
			return nil
		}

		if idx <= 0 {
			idx += last
		}
		if idx < first || idx > last {
			return nil
		}

		c := bk.Cursor()
		startBuf := s2pkg.Uint64ToBytes(uint64(idx))
		k, v := c.Seek(startBuf)
		if !bytes.HasPrefix(k, startBuf) {
			return fmt.Errorf("fatal: missing key")
		}
		data = v
		return nil
	})
	return data, err
}

func (s *Server) QHead(key string) (head int64, err error) {
	err = s.pick(key).View(func(tx *bbolt.Tx) error {
		if bk := tx.Bucket([]byte("q." + key)); bk != nil {
			head, _, _ = queueLenImpl(bk)
		}
		return nil
	})
	return head, err
}

// func (s *Server) indexSet(key string, id uint32, content string) {
// 	idx, _ := s.Index.LoadOrStore(key, fts.New())
// 	idx.(*fts.Index).Add(id, content)
// }
//
// func (s *Server) indexDel(key string, id uint32) bool {
// 	idx, _ := s.Index.Load(key)
// 	if i, _ := idx.(*fts.Index); i != nil {
// 		_, ok := i.Remove(id)
// 		return ok
// 	}
// 	return false
// }
//
// func (s *Server) indexClear(key string) {
// 	s.Index.Delete(key)
// }
//
// func (s *Server) indexCard(key string) (sz int) {
// 	idx, _ := s.Index.Load(key)
// 	if i, _ := idx.(*fts.Index); i != nil {
// 		sz = i.Cardinality()
// 	}
// 	return
// }
//
// func (s *Server) indexSize(key string) (sz int) {
// 	idx, _ := s.Index.Load(key)
// 	if i, _ := idx.(*fts.Index); i != nil {
// 		sz = i.SizeBytes()
// 	}
// 	return
// }
//
// func (s *Server) indexSearch(key string, q []string, flags redisproto.Flags) (p []s2pkg.Pair) {
// 	idx, _ := s.Index.Load(key)
// 	if i, _ := idx.(*fts.Index); i != nil {
// 		for _, r := range i.TopN(flags.BM25, flags.COUNT, q...) {
// 			p = append(p, s2pkg.Pair{Member: strconv.Itoa(int(r.ID)), Score: float64(r.Score)})
// 		}
// 	}
// 	return
// }
//
// func (s *Server) indexBuild(key string, flags redisproto.Flags) {
// 	panic("not implemented")
// 	// s.pick(key).View(func(tx *bbolt.Tx) error {
// 	// 	bk := tx.Bucket([]byte("zset.score." + key))
// 	// 	if bk == nil {
// 	// 		return nil
// 	// 	}
//
// 	// 	tmp, _ := s.Index.LoadOrStore(key, fts.New())
// 	// 	idx := tmp.(*fts.Indexer)
// 	// 	c := bk.Cursor()
// 	// 	if flags.DESC {
// 	// 		for k, v := c.Last(); len(k) >= 8; k, v = c.Prev() {
// 	// 		}
// 	// 	} else {
// 	// 		for k, v := c.First(); len(k) >= 8; k, v = c.Next() {
// 	// 		}
// 	// 	}
// 	// 	return nil
// 	// })
// }
