package main

import (
	"bytes"
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/nj/typ"
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
	mergeScore      func(float64, float64) float64
}

type zincrbyFlag struct {
	dataFunc    bas.Value
	incrToValue bool
	setData     bool
	data        []byte
	bm16Data    bool
	bm16        uint16
	addToMember string
	subToMember string
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
	flags := zaddFlag{
		delScoreLt: math.NaN(),
		mergeScore: defaultMergeScore,
	}

	var idx = 2
	for ; ; idx++ {
		switch strings.ToUpper(command.Get(idx)) {
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
			flags.mergeScore = func(old, new float64) float64 { return old + new }
		case "MSAVG":
			flags.mergeScore = func(old, new float64) float64 { return (old + new) / 2 }
		case "MSBIT": // hi lo
			mask, _ := parseMergeScoreBitRange(command, idx, true)
			flags.mergeScore = func(o, n float64) float64 { return float64((int64(o) & mask) | int64(n)) }
			idx += 2
		case "MSBITSET": // hi lo at
			mask, at := parseMergeScoreBitRange(command, idx, false)
			flags.mergeScore = func(o, n float64) float64 { return float64((int64(o) & mask) | at | int64(n)) }
			idx += 3
		case "MSBITCLR": // hi lo at
			mask, at := parseMergeScoreBitRange(command, idx, false)
			flags.mergeScore = func(o, n float64) float64 { return float64((int64(o)&mask)&^at | int64(n)) }
			idx += 3
		case "MSBITINV": // hi lo at
			mask, at := parseMergeScoreBitRange(command, idx, false)
			flags.mergeScore = func(o, n float64) float64 { return float64((int64(o) & mask) ^ at | int64(n)) }
			idx += 3
		default:
			goto MEMEBRS
		}
	}

MEMEBRS:
	var pairs []s2pkg.Pair
	if !flags.withData {
		for i := idx; i < command.ArgCount(); i += 2 {
			pairs = append(pairs, s2pkg.Pair{Member: command.Get(i + 1), Score: command.Float64(i)})
		}
	} else {
		for i := idx; i < command.ArgCount(); i += 3 {
			p := s2pkg.Pair{Score: command.Float64(i), Member: command.Get(i + 1), Data: command.Bytes(i + 2)}
			if flags.bm16Data {
				s2pkg.MustParseFloatBytes(p.Data)
			}
			pairs = append(pairs, p)
		}
	}
	return s.prepareZAdd(key, pairs, flags, dd)
}

func (s *Server) parseDel(cmd, key string, command *wire.Command, dd []byte) preparedTx {
	switch cmd {
	case "DEL":
		// DEL key
		// DEL start end
		return s.prepareDel(key, command.Get(2), dd)
	case "ZREM":
		return prepareZRem(key, toStrings(command.Argv[2:]), dd)
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

func (s *Server) parseZIncrBy(cmd, key string, command *wire.Command, dd []byte) preparedTx {
	// ZINCRBY key score member [DF datafunc] [BM16 bit] [DATA data]
	var flags zincrbyFlag
	for i := 4; i < command.ArgCount(); i++ {
		switch strings.ToUpper(command.Get(i)) {
		case "DF":
			flags.dataFunc = nj.MustRun(nj.LoadString(command.Get(i+1), nil))
			i++
		case "BM16":
			flags.bm16Data, flags.bm16 = true, uint16(command.Int64(i+1))
			i++
		case "DATA":
			flags.setData, flags.data = true, command.Bytes(i+1)
			i++
		case "INCRTO":
			flags.incrToValue = true
		case "ADDTM":
			flags.addToMember = command.Get(i + 1)
			i++
		case "SUBTM":
			flags.subToMember = command.Get(i + 1)
			i++
		}
	}
	return prepareZIncrBy(key, command.Get(3), command.Float64(2), flags, dd)
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

func (s *Server) deleteKey(tx extdb.Storage, key string, bkName, bkScore, bkCounter []byte) error {
	if _, c := s.rangeDeleteWatcher.Incr(1); int(c) > *deleteKeyQPSLimit {
		return fmt.Errorf("delete key (%s) qps limit reached: %d", key, *deleteKeyQPSLimit)
	}
	if err := tx.DeleteRange(bkName, s2pkg.IncBytes(bkName), pebble.Sync); err != nil {
		return err
	}
	if err := tx.DeleteRange(bkScore, s2pkg.IncBytes(bkScore), pebble.Sync); err != nil {
		return err
	}
	return tx.Delete(bkCounter, pebble.Sync)
}

func (s *Server) prepareDel(startKey, endKey string, dd []byte) preparedTx {
	f := func(tx extdb.LogTx) (interface{}, error) {
		if endKey != "" {
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
			return 1, writeLog(tx, dd)
		}
		bkName, bkScore, bkCounter := ranges.GetZSetRangeKey(startKey)
		if err := s.deleteKey(tx, startKey, bkName, bkScore, bkCounter); err != nil {
			return nil, err
		}
		return 1, writeLog(tx, dd)
	}
	return preparedTx{f: f}
}

func (s *Server) prepareZAdd(key string, pairs []s2pkg.Pair, flags zaddFlag, dd []byte) preparedTx {
	f := func(tx extdb.LogTx) (interface{}, error) {
		if !math.IsNaN(flags.delScoreLt) {
			// Filter out all DSLT in advance
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
				p.Score = flags.mergeScore(0, p.Score)
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
				p.Score = flags.mergeScore(oldScore, p.Score)
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
		if !math.IsNaN(flags.delScoreLt) {
			c := tx.NewIter(&pebble.IterOptions{
				LowerBound: bkScore,
				UpperBound: s2pkg.IncBytes(bkScore),
			})
			defer c.Close()

			if c.Last(); c.Valid() && bytes.HasPrefix(c.Key(), bkScore) {
				score := s2pkg.BytesToFloat(c.Key()[len(bkScore):])
				if score < flags.delScoreLt {
					// Special case: all members can be deleted
					if err := s.deleteKey(tx, key, bkName, bkScore, bkCounter); err != nil {
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

		if bas.IsCallable(flags.dataFunc) {
			res, err := bas.Call2(flags.dataFunc.Object(), bas.UnsafeStr(dataBuf), bas.Float64(score), bas.Float64(by))
			if err != nil {
				return 0, err
			}
			if res.Type() != typ.String && !bas.IsBytes(res) {
				return 0, fmt.Errorf("ZINCRBY datafunc: expects string or bytes")
			}
			dataBuf = bas.ToReadonlyBytes(res)
		} else if flags.bm16Data {
			dataBuf, _ = bitmap.Add(dataBuf, flags.bm16)
		} else if flags.setData {
			dataBuf = flags.data
		}

		newScore := score + by
		retScore := newScore
		if flags.incrToValue {
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
		return retScore, writeLog(tx, dd)
	}
	if flags.addToMember != "" {
		flags.addToMember = ""
		f2 := func(tx extdb.LogTx) (interface{}, error) {
			v, err := f(tx)
			if err != nil {
				return v, err
			}
			prepareZIncrBy(key, flags.addToMember, v.(float64), flags)
		}
		f = f2
	}
	return preparedTx{f: f}
}

func prepareZRemRangeByRank(key string, start, end int, dd []byte) preparedTx {
	f := func(tx extdb.LogTx) (interface{}, error) {
		c, err := rangeScore(key, ranges.MinScoreRange, ranges.MaxScoreRange, ranges.Options{
			OffsetStart: start,
			OffsetEnd:   end,
			DeleteLog:   dd,
			Limit:       ranges.HardLimit,
			Append:      ranges.DefaultAppend,
		})(tx)
		return c.Count, err
	}
	return preparedTx{f: f}
}

func prepareZRemRangeByLex(key string, start, end string, dd []byte) preparedTx {
	f := func(tx extdb.LogTx) (interface{}, error) {
		c, err := rangeLex(key, ranges.Lex(start), ranges.Lex(end), ranges.Options{
			OffsetStart: 0,
			OffsetEnd:   math.MaxInt64,
			DeleteLog:   dd,
			Limit:       ranges.HardLimit,
			Append:      ranges.DefaultAppend,
		})(tx)
		return c.Count, err
	}
	return preparedTx{f: f}
}

func prepareZRemRangeByScore(key string, start, end string, dd []byte) preparedTx {
	f := func(tx extdb.LogTx) (interface{}, error) {
		c, err := rangeScore(key, ranges.Score(start), ranges.Score(end), ranges.Options{
			OffsetStart: 0,
			OffsetEnd:   math.MaxInt64,
			DeleteLog:   dd,
			Limit:       ranges.HardLimit,
			Append:      ranges.DefaultAppend,
		})(tx)
		return c.Count, err
	}
	return preparedTx{f: f}
}

func checkScore(s float64) error {
	if math.IsNaN(s) {
		return fmt.Errorf("score is NaN")
	}
	return nil
}
