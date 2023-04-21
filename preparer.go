package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/bitmap"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/ranges"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
	"github.com/coyove/sdss/future"
)

type zaddFlag struct {
	xx, nx, ch      bool
	withData        bool
	preserveOldData bool // if old data exists
	bm16Data        bool
	delScoreLt      float64
	mergeScore      struct {
		f      func(float64, float64) float64
		custom bool
	}
}

type zincrbyFlag struct {
	dataFunc    bas.Value
	incrToScore bool
	setData     bool
	data        []byte
	bm16Data    bool
	bm16        uint16
	add         struct {
		score2   float64
		retScale float64
		member   string
	}
}

func defaultMergeScore(old, new float64) float64 {
	return new
}

func parseMergeScoreBitRange(command *wire.Command, idx int, rangeOnly bool) (int64, int64) {
	hi, lo := command.Int64(idx+1), command.Int64(idx+2)
	if hi < lo || hi < 0 || lo < 0 {
		panic(fmt.Sprintf("invalid bits range: %d-%d", hi, lo))
	}
	if rangeOnly {
		return s2pkg.BitsMask(hi, lo), 0
	}
	at := command.Int64(idx + 3)
	if at > hi || at < lo {
		panic(fmt.Sprintf("invalid bit position: %d", at))
	}
	return s2pkg.BitsMask(hi, lo), 1 << at
}

func (s *Server) parseAppend(cmd, key string, command *wire.Command, id future.Future) preparedTx {
	var data [][]byte
	var maxCount int64
	var idx = 2
	switch command.StrRef(idx) {
	case "MAX", "max":
		maxCount = command.Int64(idx + 1)
		idx += 2
	}

	for i := idx; i < command.ArgCount(); i++ {
		data = append(data, command.Bytes(i))
	}
	return s.prepareAppend(key, data, maxCount, id)
}

func (s *Server) parseDel(cmd, key string, command *wire.Command, dd []byte) preparedTx {
	switch cmd {
	case "DEL":
		// DEL key
		// DEL start end
		return s.prepareDel(key, command.Str(2), dd)
	case "ZREM":
		tmp := command.Argv[2:]
		if len(tmp) > *zsetMemberLimit {
			panic("ZREM: too many members to remove")
		}
		var keys []string
		for _, b := range tmp {
			keys = append(keys, string(b))
		}
		return prepareZRem(key, keys, dd)
	}
	panic("shouldn't happen")
}

func (s *Server) parseZIncrBy(cmd, key string, command *wire.Command, dd []byte) preparedTx {
	// ZINCRBY key score member [DF datafunc] [BM16 bit] [DATA data] [ADD source member2] [INCRTO]
	var flags zincrbyFlag
	for i := 4; i < command.ArgCount(); i++ {
		switch strings.ToUpper(command.Str(i)) {
		case "DF":
			flags.dataFunc = nj.MustRun(nj.LoadString(command.Str(i+1), nil))
			i++
		case "BM16":
			flags.bm16Data, flags.bm16 = true, uint16(command.Int64(i+1))
			i++
		case "DATA":
			flags.setData, flags.data = true, command.Bytes(i+1)
			i++
		case "INCRTO":
			flags.incrToScore = true
		case "ADD":
			source := command.Str(i + 1)
			if v, err := strconv.ParseFloat(source, 64); err == nil {
				flags.add.score2, flags.add.retScale = v, math.NaN()
			} else if strings.EqualFold(source, "result") {
				flags.add.score2, flags.add.retScale = math.NaN(), 1
			} else if strings.EqualFold(source, "negresult") {
				flags.add.score2, flags.add.retScale = math.NaN(), -1
			} else {
				panic("ZINCRBY ADD invalid source: " + source)
			}
			flags.add.member = command.Str(i + 2)
			i += 2
		}
	}
	return prepareZIncrBy(key, command.Str(3), command.Float64(2), flags, dd)
}

func deletePair(tx extdb.LogTx, key string, pairs []s2pkg.Pair, dd []byte) error {
	bkName, bkScore, bkCounter := ranges.GetZSetRangeKey(key)
	for _, p := range pairs {
		if err := tx.Delete(append(bkName, p.Member...), pebble.Sync); err != nil {
			return err
		}
		if err := tx.Delete(append(append(bkScore, s2pkg.FloatToBytes(p.Score)...), p.Member...), pebble.Sync); err != nil {
			return err
		}
	}
	if _, err := extdb.IncrKey(tx, bkCounter, -int64(len(pairs))); err != nil {
		return err
	}
	return writeLog(tx, dd)
}

func (s *Server) deleteZSet(tx extdb.Storage, key string, bkName, bkScore, bkCounter []byte) error {
	if _, c := s.rangeDeleteWatcher.Incr(1); int(c) > *deleteKeyQPSLimit {
		return fmt.Errorf("delete zset key %q rate limit: %d", key, *deleteKeyQPSLimit)
	}
	if err := tx.DeleteRange(bkName, s2pkg.IncBytes(bkName), pebble.Sync); err != nil {
		return err
	}
	if err := tx.DeleteRange(bkScore, s2pkg.IncBytes(bkScore), pebble.Sync); err != nil {
		return err
	}
	return tx.Delete(bkCounter, pebble.Sync)
}

func (s *Server) deleteSet(tx extdb.Storage, key string, bkName, bkCounter []byte) error {
	if _, c := s.rangeDeleteWatcher.Incr(1); int(c) > *deleteKeyQPSLimit {
		return fmt.Errorf("delete set key %q rate limit: %d", key, *deleteKeyQPSLimit)
	}
	if err := tx.DeleteRange(bkName, s2pkg.IncBytes(bkName), pebble.Sync); err != nil {
		return err
	}
	return tx.Delete(bkCounter, pebble.Sync)
}

func (s *Server) prepareDel(startKey, endKey string, dd []byte) preparedTx {
	f := func(tx extdb.LogTx) (interface{}, error) {
		if endKey != "" {
			if _, c := s.rangeDeleteWatcher.Incr(1); int(c) > *deleteKeyQPSLimit {
				return nil, fmt.Errorf("delete range key %q-%q rate limit: %d", startKey, endKey, *deleteKeyQPSLimit)
			}
			bkStartName, bkStartScore, bkStartCounter := ranges.GetZSetRangeKey(startKey)
			bkEndName, bkEndScore, bkEndCounter := ranges.GetZSetRangeKey(endKey)
			if err := tx.DeleteRange(bkStartName, s2pkg.IncBytes(bkEndName), pebble.Sync); err != nil {
				return nil, err
			}
			if err := tx.DeleteRange(bkStartScore, s2pkg.IncBytes(bkEndScore), pebble.Sync); err != nil {
				return nil, err
			}
			if err := tx.DeleteRange(bkStartCounter, s2pkg.IncBytes(bkEndCounter), pebble.Sync); err != nil {
				return nil, err
			}
			bkStartName, bkStartCounter = ranges.GetSetRangeKey(startKey)
			bkEndName, bkEndCounter = ranges.GetSetRangeKey(endKey)
			if err := tx.DeleteRange(bkStartName, s2pkg.IncBytes(bkEndName), pebble.Sync); err != nil {
				return nil, err
			}
			if err := tx.DeleteRange(bkStartCounter, s2pkg.IncBytes(bkEndCounter), pebble.Sync); err != nil {
				return nil, err
			}
			bkStartName = ranges.GetKVKey(startKey)
			bkEndName = ranges.GetKVKey(endKey)
			if err := tx.DeleteRange(bkStartName, s2pkg.IncBytes(bkEndName), pebble.Sync); err != nil {
				return nil, err
			}
			return 1, writeLog(tx, dd)
		}
		bkName, bkScore, bkCounter := ranges.GetZSetRangeKey(startKey)
		if err := s.deleteZSet(tx, startKey, bkName, bkScore, bkCounter); err != nil {
			return nil, err
		}
		bkName, bkCounter = ranges.GetSetRangeKey(startKey)
		if err := s.deleteSet(tx, startKey, bkName, bkCounter); err != nil {
			return nil, err
		}
		bkName = ranges.GetKVKey(startKey)
		if err := tx.Delete(bkName, pebble.Sync); err != nil {
			return nil, err
		}
		return 1, writeLog(tx, dd)
	}
	return preparedTx{f: f}
}

func (s *Server) prepareAppend(key string, data [][]byte, max int64, id future.Future) preparedTx {
	f := func(tx extdb.LogTx) (interface{}, error) {
		bkPrefix, bkCounter, _ := ranges.GetKey(key)

		idx := make([]byte, 16)
		binary.BigEndian.PutUint64(idx[:], uint64(id))
		rand.Read(idx[8:12])

		for i, p := range data {
			binary.BigEndian.PutUint32(idx[12:], uint32(i))
			k := append(bkPrefix, idx...)
			if err := tx.Set(k, p, pebble.Sync); err != nil {
				return nil, err
			}
		}

		ctr, err := extdb.IncrKey(tx, bkCounter, int64(len(data)))
		if err != nil {
			return nil, err
		}

		if max > 0 && ctr > max {
			c := tx.NewIter(&pebble.IterOptions{
				LowerBound: bkPrefix,
				UpperBound: s2pkg.IncBytes(bkPrefix),
			})
			defer c.Close()

			for c.First(); c.Valid(); c.Next() {
				if ctr <= max {
					break
				}
				if err := tx.Delete(c.Key(), pebble.Sync); err != nil {
					return nil, err
				}
				ctr--
			}

			if err := tx.Set(bkCounter, s2pkg.Uint64ToBytes(uint64(ctr)), pebble.Sync); err != nil {
				return nil, err
			}
		}

		return int64(len(data)), nil
	}
	return preparedTx{f: f}
}

func prepareZRem(key string, members []string, dd []byte) preparedTx {
	f := func(tx extdb.LogTx) (count interface{}, err error) {
		bkName, _, _ := ranges.GetZSetRangeKey(key)
		var pairs []s2pkg.Pair
		for _, member := range members {
			scoreBuf, err := extdb.GetKey(tx, append(bkName, member...))
			if err != nil {
				return nil, err
			}
			if len(scoreBuf) == 0 {
				continue
			}
			pairs = append(pairs, s2pkg.Pair{Member: member, Score: s2pkg.BytesToFloat(scoreBuf)})
		}
		return len(pairs), deletePair(tx, key, pairs, dd)
	}
	return preparedTx{f: f}
}

func prepareZIncrBy(key string, member string, by float64, flags zincrbyFlag, dd []byte) preparedTx {
	f := func(tx extdb.LogTx) (newValue interface{}, err error) {
		bkName, bkScore, bkCounter := ranges.GetZSetRangeKey(key)
		score := 0.0
		added := false

		score, _, foundScore, err := extdb.GetKeyNumber(tx, append(bkName, member...))
		if err != nil {
			return nil, err
		}

		var dataBuf []byte
		if foundScore {
			oldKey := append(append(bkScore, s2pkg.FloatToBytes(score)...), member...)
			dataBuf, err = extdb.GetKey(tx, oldKey)
			if err != nil {
				return nil, err
			}
			if err := tx.Delete(oldKey, pebble.Sync); err != nil {
				return 0, err
			}
		} else {
			dataBuf = []byte("")
			added = true
		}

		if flags.dataFunc.IsObject() {
			res := flags.dataFunc.Object().TryCall(nil, bas.UnsafeStr(dataBuf), bas.Float64(score), bas.Float64(by))
			if res.IsError() {
				return 0, res.Error()
			}
			if res.IsString() {
				dataBuf = []byte(res.Str())
			} else if res.IsBytes() {
				dataBuf = res.Bytes()
			} else {
				return 0, fmt.Errorf("ZINCRBY datafunc: expects string or bytes")
			}
		} else if flags.bm16Data {
			dataBuf, _ = bitmap.Add(dataBuf, flags.bm16)
		} else if flags.setData {
			dataBuf = flags.data
		}

		newScore := score + by
		retScore := newScore
		if flags.incrToScore {
			retScore = by - score
			newScore = by
		}
		if err := checkScore(newScore); err != nil {
			return 0, err
		}

		scoreBuf := s2pkg.FloatToBytes(newScore)
		if err := tx.Set(append(bkName, member...), scoreBuf, pebble.Sync); err != nil {
			return 0, err
		}
		if err := tx.Set(append(append(bkScore, scoreBuf...), member...), dataBuf, pebble.Sync); err != nil {
			return 0, err
		}
		if added {
			if _, err := extdb.IncrKey(tx, bkCounter, 1); err != nil {
				return nil, err
			}
		}
		if dd == nil {
			// Special case: (ZINCRBY key score member) (ADD ...)
			// dd has been written in the first ZINCRBY, there is no need to
			// write again in the second ADD
			return retScore, nil
		}
		return retScore, writeLog(tx, dd)
	}
	if flags.add.member != "" {
		f2 := f
		f = func(tx extdb.LogTx) (interface{}, error) {
			flags2 := flags
			am := flags2.add.member
			flags2.add.member = ""
			flags2.incrToScore = false
			v, err := f2(tx)
			if err != nil {
				return nil, err
			}
			if math.Abs(flags2.add.retScale) == 1 {
				_, err = prepareZIncrBy(key, am, v.(float64)*flags.add.retScale, flags2, nil).f(tx)
			} else {
				_, err = prepareZIncrBy(key, am, flags2.add.score2, flags2, nil).f(tx)
			}
			if err != nil {
				return nil, err
			}
			return v, nil
		}
	}
	return preparedTx{f: f}
}

func checkScore(s float64) error {
	if math.IsNaN(s) {
		return fmt.Errorf("score is NaN")
	}
	return nil
}

func (s *Server) parseSAddRem(cmd, key string, command *wire.Command, dd []byte) preparedTx {
	var members [][]byte
	for i := 2; i < command.ArgCount(); i++ {
		members = append(members, command.Bytes(i))
	}
	rem := cmd == "SREM"
	add := cmd == "SADD"
	f := func(tx extdb.LogTx) (interface{}, error) {
		bkName, bkCounter := ranges.GetSetRangeKey(key)
		ctr := 0
		for _, p := range members {
			if len(p) == 0 {
				return nil, fmt.Errorf("empty member name")
			}

			nameBuf := append(bkName, p...)
			_, closer, err := tx.Get(nameBuf)
			if err == pebble.ErrNotFound {
				// Add new member
				if add {
					ctr++
					if err := tx.Set(nameBuf, nil, pebble.Sync); err != nil {
						return nil, err
					}
				}
			} else if err != nil {
				// Bad error
				return nil, err
			} else {
				// Member exists
				closer.Close()
				if rem {
					ctr--
					if err := tx.Delete(nameBuf, pebble.Sync); err != nil {
						return nil, err
					}
				}
			}
		}

		if ctr != 0 {
			if _, err := extdb.IncrKey(tx, bkCounter, int64(ctr)); err != nil {
				return nil, err
			}
		}
		return int(math.Abs(float64(ctr))), writeLog(tx, dd)
	}
	return preparedTx{f: f}
}

func (s *Server) parseSet(cmd, key string, command *wire.Command, dd []byte) preparedTx {
	value := command.Bytes(2)
	nx := cmd == "SETNX"
	f := func(tx extdb.LogTx) (interface{}, error) {
		bkName := ranges.GetKVKey(key)
		if nx {
			_, closer, err := tx.Get(bkName)
			if err == pebble.ErrNotFound {
			} else if err != nil {
				return nil, err
			} else {
				closer.Close()
				return 0, writeLog(tx, dd)
			}
		}
		if err := tx.Set(bkName, value, pebble.Sync); err != nil {
			return nil, err
		}
		if nx {
			return 1, writeLog(tx, dd)
		}
		return "OK", writeLog(tx, dd)
	}
	return preparedTx{f: f}
}
