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
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/sirupsen/logrus"
)

var zsetKeyScoreFullRange = &pebble.IterOptions{
	LowerBound: []byte("zsetks__"),
	UpperBound: []byte("zsetks_\xff"),
}

var zsetScoreKeyValueFullRange = &pebble.IterOptions{
	LowerBound: []byte("zsetskv_"),
	UpperBound: []byte("zsetskv\xff"),
}

func getZSetRangeKey(key string) ([]byte, []byte, []byte) {
	return []byte("zsetks__" + key + "\x00"), []byte("zsetskv_" + key + "\x00"), getZSetCounterKey(key)
}

func getZSetCounterKey(key string) []byte {
	return []byte("zsetctr_" + key)
}

func getShardLogKey(shard int16) []byte {
	return []byte(fmt.Sprintf("log%04x_", shard))
}

func (s *Server) parseZAdd(cmd, key string, command *redisproto.Command, dd []byte) preparedTx {
	var xx, nx, ch, pd, data bool
	var dslt = math.NaN()
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
		case "PD":
			pd = true
			continue
		case "DATA":
			data = true
			continue
		case "DSLT":
			idx++
			dslt = command.Float64(idx)
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
	return s.prepareZAdd(key, pairs, nx, xx, ch, pd, dslt, dd)
}

func (s *Server) parseDel(cmd, key string, command *redisproto.Command, dd []byte) preparedTx {
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

func parseZIncrBy(cmd, key string, command *redisproto.Command, dd []byte) preparedTx {
	// ZINCRBY key score member [datafunc]
	var dataFunc bas.Value
	if code := command.Get(4); code != "" {
		dataFunc = nj.MustRun(nj.LoadString(code, nil))
	}
	return prepareZIncrBy(key, command.Get(3), command.Float64(2), dataFunc, dd)
}

func deletePair(tx s2pkg.LogTx, key string, pairs []s2pkg.Pair, dd []byte) error {
	bkName, bkScore, bkCounter := getZSetRangeKey(key)
	for _, p := range pairs {
		if err := tx.Delete(append(bkName, p.Member...), pebble.Sync); err != nil {
			return err
		}
		if err := tx.Delete(append(append(bkScore, s2pkg.FloatToBytes(p.Score)...), p.Member...), pebble.Sync); err != nil {
			return err
		}
	}
	if err := s2pkg.IncrKey(tx, bkCounter, -int64(len(pairs))); err != nil {
		return err
	}
	return writeLog(tx, dd)
}

func (s *Server) deleteKey(tx s2pkg.Storage, key string, bkName, bkScore, bkCounter []byte) error {
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
	f := func(tx s2pkg.LogTx) (interface{}, error) {
		if endKey != "" {
			bkStartName, bkStartScore, bkStartCounter := getZSetRangeKey(startKey)
			bkEndName, bkEndScore, bkEndCounter := getZSetRangeKey(endKey)
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
		bkName, bkScore, bkCounter := getZSetRangeKey(startKey)
		if err := s.deleteKey(tx, startKey, bkName, bkScore, bkCounter); err != nil {
			return nil, err
		}
		return 1, writeLog(tx, dd)
	}
	return preparedTx{f: f}
}

func (s *Server) prepareZAdd(key string, pairs []s2pkg.Pair, nx, xx, ch, pd bool, dslt float64, dd []byte) preparedTx {
	f := func(tx s2pkg.LogTx) (interface{}, error) {
		if !math.IsNaN(dslt) {
			// Filter out all DSLT in advance
			for i := len(pairs) - 1; i >= 0; i-- {
				if pairs[i].Score < dslt {
					pairs = append(pairs[:i], pairs[i+1:]...)
				}
			}
		}

		bkName, bkScore, bkCounter := getZSetRangeKey(key)
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
				if xx {
					continue
				}
				added++
			} else if err != nil {
				return nil, err
			} else {
				// Old key exists
				scoreKey := append(append(bkScore, scoreBuf...), p.Member...)
				scoreBufCloser.Close()
				if nx {
					continue
				}
				if pd && len(p.Data) == 0 {
					p.Data, err = s2pkg.GetKey(tx, scoreKey)
					if err != nil {
						return nil, err
					}
				}
				if err := tx.Delete(scoreKey, pebble.Sync); err != nil {
					return nil, err
				}
				if p.Score != s2pkg.BytesToFloat(scoreBuf) {
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

		if !math.IsNaN(dslt) {
			c := tx.NewIter(&pebble.IterOptions{
				LowerBound: bkScore,
				UpperBound: s2pkg.IncBytes(bkScore),
			})
			defer c.Close()

			if c.Last(); c.Valid() && bytes.HasPrefix(c.Key(), bkScore) {
				score := s2pkg.BytesToFloat(c.Key()[len(bkScore):])
				if score < dslt {
					// Special case: all members can be deleted
					if err := s.deleteKey(tx, key, bkName, bkScore, bkCounter); err != nil {
						logrus.Errorf("[DSLT] delete key: %s, dslt: %f, error: %v", key, dslt, err)
					}
					s.Survey.DSLTFull.Incr(1)
					return math.MinInt64, writeLog(tx, dd)
				}
			}

			x := 0
			dsltThreshold := int(math.Max(float64(len(pairs))*2, float64(*dsltMaxMembers)))
			for c.First(); c.Valid() && bytes.HasPrefix(c.Key(), bkScore); c.Next() {
				score := s2pkg.BytesToFloat(c.Key()[len(bkScore):])
				if score >= dslt {
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
						key, score, dslt, dsltThreshold)
					break
				}
			}

			if x > 0 {
				s.Survey.DSLT.Incr(int64(x))
			}
		}

		if added != 0 {
			if err := s2pkg.IncrKey(tx, bkCounter, int64(added)); err != nil {
				return nil, err
			}
		}
		if ch {
			return added + updated, writeLog(tx, dd)
		}
		return added, writeLog(tx, dd)
	}
	return preparedTx{f: f}
}

func prepareZRem(key string, members []string, dd []byte) preparedTx {
	f := func(tx s2pkg.LogTx) (count interface{}, err error) {
		bkName, _, _ := getZSetRangeKey(key)
		var pairs []s2pkg.Pair
		for _, member := range members {
			scoreBuf, err := s2pkg.GetKey(tx, append(bkName, member...))
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

func prepareZIncrBy(key string, member string, by float64, dataUpdate bas.Value, dd []byte) preparedTx {
	f := func(tx s2pkg.LogTx) (newValue interface{}, err error) {
		bkName, bkScore, bkCounter := getZSetRangeKey(key)
		score := 0.0
		added := false

		score, _, foundScore, err := s2pkg.GetKeyNumber(tx, append(bkName, member...))
		if err != nil {
			return nil, err
		}

		var dataBuf []byte
		if foundScore {
			oldKey := append(append(bkScore, s2pkg.FloatToBytes(score)...), member...)
			dataBuf, err = s2pkg.GetKey(tx, oldKey)
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
		if bas.IsCallable(dataUpdate) {
			res, err := bas.Call2(dataUpdate.Object(), bas.UnsafeStr(dataBuf), bas.Float64(score), bas.Float64(by))
			if err != nil {
				return 0, err
			}
			if res.Type() != typ.String && !bas.IsBytes(res) {
				return 0, fmt.Errorf("ZINCRBY datafunc: expects string or bytes")
			}
			dataBuf = bas.ToReadonlyBytes(res)
		}

		if by == 0 {
			_ = "special case: zincrby name 0 non_existed_key"
		}
		if err := checkScore(score + by); err != nil {
			return 0, err
		}

		scoreBuf := s2pkg.FloatToBytes(score + by)
		if err := tx.Set(append(bkName, member...), scoreBuf, pebble.Sync); err != nil {
			return 0, err
		}
		if err := tx.Set(append(append(bkScore, scoreBuf...), member...), dataBuf, pebble.Sync); err != nil {
			return 0, err
		}
		if added {
			if err := s2pkg.IncrKey(tx, bkCounter, 1); err != nil {
				return nil, err
			}
		}
		return score + by, writeLog(tx, dd)
	}
	return preparedTx{f: f}
}

func prepareZRemRangeByRank(key string, start, end int, dd []byte) preparedTx {
	f := func(tx s2pkg.LogTx) (interface{}, error) {
		_, c, err := rangeScore(key, MinScoreRange, MaxScoreRange, s2pkg.RangeOptions{
			OffsetStart: start,
			OffsetEnd:   end,
			DeleteLog:   dd,
			Limit:       s2pkg.RangeHardLimit,
			Append:      s2pkg.DefaultRangeAppend,
		})(tx)
		return c, err
	}
	return preparedTx{f: f}
}

func prepareZRemRangeByLex(key string, start, end string, dd []byte) preparedTx {
	f := func(tx s2pkg.LogTx) (interface{}, error) {
		rangeStart := s2pkg.NewLexRL(start)
		rangeEnd := s2pkg.NewLexRL(end)
		_, c, err := rangeLex(key, rangeStart, rangeEnd, s2pkg.RangeOptions{
			OffsetStart: 0,
			OffsetEnd:   math.MaxInt64,
			DeleteLog:   dd,
			Limit:       s2pkg.RangeHardLimit,
			Append:      s2pkg.DefaultRangeAppend,
		})(tx)
		return c, err
	}
	return preparedTx{f: f}
}

func prepareZRemRangeByScore(key string, start, end string, dd []byte) preparedTx {
	f := func(tx s2pkg.LogTx) (interface{}, error) {
		_, c, err := rangeScore(key, s2pkg.NewScoreRL(start), s2pkg.NewScoreRL(end), s2pkg.RangeOptions{
			OffsetStart: 0,
			OffsetEnd:   math.MaxInt64,
			DeleteLog:   dd,
			Limit:       s2pkg.RangeHardLimit,
			Append:      s2pkg.DefaultRangeAppend,
		})(tx)
		return c, err
	}
	return preparedTx{f: f}
}
