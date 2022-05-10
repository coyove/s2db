package main

import (
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj/bas"
	"github.com/coyove/nj/typ"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/sirupsen/logrus"
)

var ErrBigDelete = fmt.Errorf("can't delete big keys directly, use 'UNLINK key' command")

func getZSetRangeKey(key string) ([]byte, []byte, []byte) {
	return []byte("zsetks__" + key + "\x00"), []byte("zsetskv_" + key + "\x00"), []byte("zsetctr_" + key)
}

func getShardLogKey(shard int16) []byte {
	return []byte(fmt.Sprintf("log%04x_", shard))
}

func prepareDel(key string, dd []byte) preparedTx {
	f := func(tx s2pkg.LogTx) (interface{}, error) {
		bkName, bkScore, bkCounter := getZSetRangeKey(key)
		if err := tx.DeleteRange(bkName, incrBytes(bkName), pebble.Sync); err != nil {
			return nil, err
		}
		if err := tx.DeleteRange(bkScore, incrBytes(bkScore), pebble.Sync); err != nil {
			return nil, err
		}
		if err := tx.Delete(bkCounter, pebble.Sync); err != nil {
			return nil, err
		}
		return 1, writeLog(tx, dd)
	}
	return preparedTx{f: f}
}

func prepareZAdd(key string, pairs []s2pkg.Pair, nx, xx, ch, pd bool, dslt float64, dd []byte) preparedTx {
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
					p.Data, err = GetKeyCopy(tx, scoreKey)
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
				UpperBound: incrBytes(bkScore),
			})
			defer c.Close()

			x := 0
			for c.First(); c.Valid() && bytes.HasPrefix(c.Key(), bkScore); c.Next() {
				score := s2pkg.BytesToFloat(c.Key()[len(bkScore):])
				if score >= dslt {
					break
				}
				if err := tx.Delete(c.Key(), pebble.Sync); err != nil {
					return nil, err
				}
				if err := tx.Delete(append(dupBytes(bkName), c.Key()[len(bkScore)+8:]...), pebble.Sync); err != nil {
					return nil, err
				}
				added--
				if x++; x > s2pkg.RangeHardLimit/2 {
					logrus.Infof("[DSLT] reach hard limit on key: %s, current score: %f, dslt: %f", key, score, dslt)
					break
				}
			}
		}

		if added != 0 {
			if err := IncrKey(tx, bkCounter, int64(added)); err != nil {
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
			scoreBuf, err := GetKeyCopy(tx, append(bkName, member...))
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

		score, _, foundScore, err := GetKeyNumber(tx, append(bkName, member...))
		if err != nil {
			return nil, err
		}

		var dataBuf []byte
		if foundScore {
			oldKey := append(append(bkScore, s2pkg.FloatToBytes(score)...), member...)
			dataBuf, err = GetKeyCopy(tx, oldKey)
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
			if err := IncrKey(tx, bkCounter, 1); err != nil {
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
