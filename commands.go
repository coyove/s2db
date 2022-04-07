package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/s2pkg/fts"
	"go.etcd.io/bbolt"
)

func (s *Server) pick(key string) *bbolt.DB {
	return s.db[shardIndex(key)].DB
}

func writeLog(tx s2pkg.LogTx, dd []byte) error {
	bk, err := tx.CreateBucketIfNotExists([]byte("wal"))
	if err != nil {
		return err
	}
	bk.FillPercent = 0.9
	id, _ := bk.NextSequence()
	if tx.Logtail != nil {
		*tx.Logtail = id
	}
	return bk.Put(s2pkg.Uint64ToBytes(id), dd)
}

func parseZAdd(cmd, key string, command *redisproto.Command, dd []byte) preparedTx {
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
	return prepareZAdd(key, pairs, nx, xx, ch, fillPercent, dd)
}

func parseDel(cmd, key string, command *redisproto.Command, dd []byte) preparedTx {
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
		panic("shouldn't happen")
	}
}

func parseZIncrBy(cmd, key string, command *redisproto.Command, dd []byte) preparedTx {
	// ZINCRBY key score member [datafunc]
	var dataFunc bas.Value
	if code := command.Get(4); code != "" {
		dataFunc = nj.MustRun(nj.LoadString(code, nil))
	}
	return prepareZIncrBy(key, command.Get(3), command.Float64(2), dataFunc, dd)
}

func (s *Server) ZAdd(key string, runType int, members []s2pkg.Pair) (int64, error) {
	if err := s.checkWritable(); err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, nil
	}
	cmd := &redisproto.Command{Argv: [][]byte{[]byte("ZADD"), []byte(key), []byte("DATA")}}
	for _, m := range members {
		cmd.Argv = append(cmd.Argv, s2pkg.FormatFloatBulk(m.Score), []byte(m.Member), m.Data)
	}
	v, err := s.runPreparedTx("ZADD", key, runType, prepareZAdd(key, members, false, false, false, 0, dumpCommand(cmd)))
	if err != nil {
		return 0, err
	}
	if runType == RunDefer {
		return 0, nil
	}
	return int64(v.(int)), nil
}

func (s *Server) ZRem(key string, runType int, members []string) (int, error) {
	if err := s.checkWritable(); err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, nil
	}
	cmd := &redisproto.Command{Argv: [][]byte{[]byte("ZREM"), []byte(key)}}
	for _, m := range members {
		cmd.Argv = append(cmd.Argv, []byte(m))
	}
	v, err := s.runPreparedTx("ZREM", key, runType, prepareZRem(key, members, dumpCommand(cmd)))
	if err != nil {
		return 0, err
	}
	if runType == RunDefer {
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

func (s *Server) ZMScore(key string, keys []string, flags redisproto.Flags) (scores []float64, err error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing keys")
	}
	for range keys {
		scores = append(scores, math.NaN())
	}
	err = s.pick(key).View(func(tx *bbolt.Tx) error {
		bkName := tx.Bucket([]byte("zset." + key))
		if bkName == nil {
			return nil
		}
		for i, k := range keys {
			if !math.IsNaN(scores[i]) {
				continue
			}
			if scoreBuf := bkName.Get([]byte(k)); len(scoreBuf) != 0 {
				scores[i] = s2pkg.BytesToFloat(scoreBuf)
			}
		}
		if flags.Command.ArgCount() > 0 {
			s.addCache(key, flags.HashCode(), scores)
		}
		return nil
	})
	return
}

func (s *Server) ZMData(key string, members []string, flags redisproto.Flags) (data [][]byte, err error) {
	if len(members) == 0 {
		return nil, fmt.Errorf("missing members")
	}
	data = make([][]byte, len(members))
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
			for i, m := range members {
				scoreBuf := bkName.Get([]byte(m))
				if len(scoreBuf) != 0 {
					d := bkScore.Get([]byte(string(scoreBuf) + members[i]))
					if key == FTSDocsStoreKey {
						var doc fts.Document
						doc.UnmarshalBinary(d)
						data[i] = []byte(doc.Text)
					} else {
						data[i] = append([]byte{}, d...)
					}
				}
			}
		}()
		// fillPairsData will call ZMData as well (with an empty Flags), but no cache should be stored
		if flags.Command.ArgCount() > 0 {
			s.addCache(key, flags.HashCode(), data)
		}
		return nil
	})
	return
}

func deletePair(tx s2pkg.LogTx, key string, pairs []s2pkg.Pair, dd []byte) error {
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

func parseQAppend(cmd, key string, command *redisproto.Command, dd []byte) preparedTx {
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
	if flags.NANOTS == nil {
		flags.NANOTS = new(int64)
		*flags.NANOTS = time.Now().UnixNano()
	}
	return prepareQAppend(key, value, int64(flags.COUNT), *flags.NANOTS, m, dd)
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
		panic("corrupted queue")
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

func (s *Server) QScan(key string, startString string, n int64, flags redisproto.Flags) (data []s2pkg.Pair, err error) {
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
			c := bk.Cursor()

			var start int64
			if strings.HasPrefix(startString, "@") {
				// Timestamp start
				ts := s2pkg.MustParseInt64(startString[1:])
				i, j := first, last+1
				for i < j {
					mid, midts, ok := qMidCursor(c, i, j)
					if !ok {
						return nil
					}
					if midts < ts {
						i = mid + 1
					} else {
						j = mid
					}
				}
				if i < first && i > last {
					return nil
				}
				start = i
			} else if strings.HasPrefix(startString, "!") {
				// Absolute start
				start = s2pkg.MustParseInt64(startString[1:])
			} else {
				// Relative start
				start = s2pkg.MustParseInt64(startString)
				if start <= 0 {
					start = last + start
				} else {
					start = first + start - 1
				}
			}

			if start < first || start > last {
				return nil
			}

			startBuf := s2pkg.Uint64ToBytes(uint64(start))
			k, v := c.Seek(startBuf)
			if !bytes.HasPrefix(k, startBuf) {
				return fmt.Errorf("fatal: missing key")
			}
			for len(data) < int(n) && len(k) == 16 {
				idx := binary.BigEndian.Uint64(k[:8])
				p := s2pkg.Pair{Member: string(v), Score: float64(idx)}
				if flags.WITHDATA {
					p.Data = []byte(strconv.FormatUint(binary.BigEndian.Uint64(k[8:]), 10))
				}
				data = append(data, p)
				if desc {
					k, v = c.Prev()
				} else {
					k, v = c.Next()
				}
			}
			return nil
		}()
		if err == nil {
			s.addCache(key, flags.HashCode(), data)
		}
		return err
	})
	return data, err
}

func qMidCursor(c *bbolt.Cursor, start, end int64) (int64, int64, bool) {
	end--
	if end < start {
		panic("qMidCursor: invalid end")
	}
	var startBuf, midBuf, endBuf [16]byte
	binary.BigEndian.PutUint64(startBuf[:8], uint64(start))
	binary.BigEndian.PutUint64(midBuf[:8], uint64((start+end)/2))
	binary.BigEndian.PutUint64(endBuf[:8], uint64(end))
	binary.BigEndian.PutUint64(endBuf[8:], math.MaxUint64)

	k, _ := c.Seek(midBuf[:])
	if len(k) != 16 {
		panic("qMidCursor: shouldn't happen")
	}
	if bytes.Compare(k, endBuf[:]) > 0 || bytes.Compare(k, startBuf[:]) < 0 {
		return 0, 0, false
	}
	idx := int64(binary.BigEndian.Uint64(k[:8]))
	ts := int64(binary.BigEndian.Uint64(k[8:]))
	return idx, ts, true
}
