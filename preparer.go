package main

import (
	"bytes"
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
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/coyove/s2db/wire"
	"github.com/sirupsen/logrus"
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

func (s *Server) parseZAdd(cmd, key string, command *wire.Command, dd []byte) preparedTx {
	flags := zaddFlag{delScoreLt: math.NaN()}
	flags.mergeScore.f = defaultMergeScore

	var idx = 2
	for ; ; idx++ {
		switch strings.ToUpper(command.Str(idx)) {
		case "XX":
			flags.xx = true
		case "NX":
			flags.nx = true
		case "CH":
			flags.ch = true
		case "PD":
			flags.preserveOldData = true
		case "DATA":
			flags.withData = true
		case "BM16":
			flags.bm16Data, flags.withData = true, true
		case "DSLT":
			idx++
			flags.delScoreLt = command.Float64(idx)
		case "MSSUM":
			flags.mergeScore.f = func(old, new float64) float64 { return old + new }
			flags.mergeScore.custom = true
		case "MSAVG":
			flags.mergeScore.f = func(old, new float64) float64 { return (old + new) / 2 }
			flags.mergeScore.custom = true
		case "MSBIT": // hi lo
			mask, _ := parseMergeScoreBitRange(command, idx, true)
			flags.mergeScore.f = func(o, n float64) float64 { return float64((int64(o) & mask) | int64(n)) }
			flags.mergeScore.custom = true
			idx += 2
		case "MSBITSET": // hi lo at
			mask, at := parseMergeScoreBitRange(command, idx, false)
			flags.mergeScore.f = func(o, n float64) float64 { return float64((int64(o) & mask) | at | int64(n)) }
			flags.mergeScore.custom = true
			idx += 3
		case "MSBITCLR": // hi lo at
			mask, at := parseMergeScoreBitRange(command, idx, false)
			flags.mergeScore.f = func(o, n float64) float64 { return float64((int64(o)&mask)&^at | int64(n)) }
			flags.mergeScore.custom = true
			idx += 3
		case "MSBITINV": // hi lo at
			mask, at := parseMergeScoreBitRange(command, idx, false)
			flags.mergeScore.f = func(o, n float64) float64 { return float64((int64(o) & mask) ^ at | int64(n)) }
			flags.mergeScore.custom = true
			idx += 3
		default:
			goto MEMEBRS
		}
	}

MEMEBRS:
	var pairs []s2pkg.Pair
	if !flags.withData {
		for i := idx; i < command.ArgCount(); i += 2 {
			pairs = append(pairs, s2pkg.Pair{Member: command.Str(i + 1), Score: command.Float64(i)})
		}
	} else {
		for i := idx; i < command.ArgCount(); i += 3 {
			p := s2pkg.Pair{Score: command.Float64(i), Member: command.Str(i + 1), Data: command.Bytes(i + 2)}
			if flags.bm16Data {
				s2pkg.MustParseFloatBytes(p.Data)
			}
			pairs = append(pairs, p)
		}
	}
	if len(pairs) > *zsetMemberLimit {
		panic("ZADD: too many members to add")
	}
	return s.prepareZAdd(key, pairs, flags, dd)
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

func (s *Server) prepareZAdd(key string, pairs []s2pkg.Pair, flags zaddFlag, dd []byte) preparedTx {
	f := func(tx extdb.LogTx) (interface{}, error) {
		if !math.IsNaN(flags.delScoreLt) && !flags.mergeScore.custom {
			// Filter out all DSLT in advance, if no custom mergeScore function is provided
			for i := len(pairs) - 1; i >= 0; i-- {
				if pairs[i].Score < flags.delScoreLt {
					pairs = append(pairs[:i], pairs[i+1:]...)
				}
			}
		}

		bkName, bkScore, bkCounter := ranges.GetZSetRangeKey(key)
		added, updated := 0, 0
		for _, p := range pairs {
			if err := checkScore(p.Score); err != nil {
				return nil, err
			}
			if p.Member == "" {
				return nil, fmt.Errorf("empty member name")
			}
			scoreBuf, scoreBufCloser, err := tx.Get(append(bkName, p.Member...))
			if err == pebble.ErrNotFound {
				// Add new key
				if flags.xx {
					continue
				}
				if flags.bm16Data {
					p.Data, _ = bitmap.Add(nil, uint16(s2pkg.MustParseFloatBytes(p.Data)))
				}
				p.Score = flags.mergeScore.f(0, p.Score)
				added++
			} else if err != nil {
				// Bad error
				return nil, err
			} else {
				// Old key exists
				scoreKey := append(append(bkScore, scoreBuf...), p.Member...)
				scoreBufCloser.Close()
				if flags.nx {
					continue
				}
				if flags.bm16Data {
					mb, err := extdb.GetKey(tx, scoreKey)
					if err != nil {
						return nil, err
					}
					p.Data, _ = bitmap.Add(mb, uint16(s2pkg.MustParseFloatBytes(p.Data)))
				}
				if flags.preserveOldData && len(p.Data) == 0 {
					p.Data, err = extdb.GetKey(tx, scoreKey)
					if err != nil {
						return nil, err
					}
				}
				if err := tx.Delete(scoreKey, pebble.Sync); err != nil {
					return nil, err
				}
				oldScore := s2pkg.BytesToFloat(scoreBuf)
				p.Score = flags.mergeScore.f(oldScore, p.Score)
				if p.Score != oldScore {
					updated++
				}
			}
			scoreBuf = s2pkg.FloatToBytes(p.Score)
			if err := tx.Set(append(bkName, p.Member...), scoreBuf, pebble.Sync); err != nil {
				return nil, err
			}
			if err := tx.Set(append(append(bkScore, scoreBuf...), p.Member...), p.Data, pebble.Sync); err != nil {
				return nil, err
			}
		}

		// Delete members whose scores are smaller than DSLT
		if !math.IsNaN(flags.delScoreLt) && *dsltMaxMembers > 0 {
			c := tx.NewIter(&pebble.IterOptions{
				LowerBound: bkScore,
				UpperBound: s2pkg.IncBytes(bkScore),
			})
			defer c.Close()

			if c.Last(); c.Valid() && bytes.HasPrefix(c.Key(), bkScore) {
				score := s2pkg.BytesToFloat(c.Key()[len(bkScore):])
				if score < flags.delScoreLt {
					// Special case: all members can be deleted
					if err := s.deleteZSet(tx, key, bkName, bkScore, bkCounter); err != nil {
						logrus.Errorf("[DSLT] delete key: %s, dslt: %f, error: %v", key, flags.delScoreLt, err)
					} else {
						s.Survey.DSLTFull.Incr(1)
					}
					return math.MinInt64, writeLog(tx, dd)
				}
			}

			x := 0
			dsltThreshold := int(math.Max(float64(len(pairs))*2, float64(*dsltMaxMembers)))
			for c.First(); c.Valid() && bytes.HasPrefix(c.Key(), bkScore); c.Next() {
				score := s2pkg.BytesToFloat(c.Key()[len(bkScore):])
				if score >= flags.delScoreLt {
					break
				}
				if err := tx.Delete(c.Key(), pebble.Sync); err != nil {
					return nil, err
				}
				if err := tx.Delete(append(s2pkg.Bytes(bkName), c.Key()[len(bkScore)+8:]...), pebble.Sync); err != nil {
					return nil, err
				}
				added--
				if x++; x > dsltThreshold {
					logrus.Infof("[DSLT] reach hard limit on key: %s, current score: %f, dslt: %f, batch limit: %d",
						key, score, flags.delScoreLt, dsltThreshold)
					break
				}
			}

			if x > 0 {
				s.Survey.DSLT.Incr(int64(x))
			}
		}

		if added != 0 {
			if _, err := extdb.IncrKey(tx, bkCounter, int64(added)); err != nil {
				return nil, err
			}
		}
		if flags.ch {
			return added + updated, writeLog(tx, dd)
		}
		return added, writeLog(tx, dd)
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
