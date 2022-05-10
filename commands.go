package main

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
)

var counter func() uint64 = func() func() uint64 {
	start := time.Now().UnixNano()
	return func() uint64 { return uint64(atomic.AddInt64(&start, 1)) }
}()

func writeLog(tx s2pkg.LogTx, dd []byte) error {
	var id uint64
	if tx.InLogtail == nil {
		id = counter()
	} else {
		id = *tx.InLogtail
	}
	if tx.OutLogtail != nil {
		*tx.OutLogtail = id
	}
	return tx.Set(append(tx.LogPrefix, s2pkg.Uint64ToBytes(id)...), dd, pebble.Sync)
}

func parseZAdd(cmd, key string, command *redisproto.Command, dd []byte) preparedTx {
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
	return prepareZAdd(key, pairs, nx, xx, ch, pd, dslt, dd)
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
	v, err := s.runPreparedTx("ZADD", key, runType, prepareZAdd(key, members,
		false, false, false, false, math.NaN(), dumpCommand(cmd)))
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
	_, _, bkCounter := getZSetRangeKey(key)
	_, i, _, _ := GetKeyNumber(s.db, bkCounter)
	return int64(i)
}

func (s *Server) ZMScore(key string, memebrs []string, flags redisproto.Flags) (scores []float64, err error) {
	if len(memebrs) == 0 {
		return nil, fmt.Errorf("missing members")
	}
	for range memebrs {
		scores = append(scores, math.NaN())
	}
	bkName, _, _ := getZSetRangeKey(key)
	for i, m := range memebrs {
		score, _, found, _ := GetKeyNumber(s.db, append(bkName, m...))
		if found {
			scores[i] = score
		}
	}
	if flags.Command.ArgCount() > 0 {
		s.addCache(key, flags.HashCode(), scores)
	}
	return
}

func (s *Server) ZMData(key string, members []string, flags redisproto.Flags) (data [][]byte, err error) {
	if len(members) == 0 {
		return nil, fmt.Errorf("missing members")
	}
	data = make([][]byte, len(members))
	bkName, bkScore, _ := getZSetRangeKey(key)
	for i, m := range members {
		scoreBuf, _ := GetKeyCopy(s.db, append(bkName, m...))
		if len(scoreBuf) != 0 {
			d, err := GetKeyCopy(s.db, append(bkScore, append(scoreBuf, m...)...))
			if err != nil {
				return nil, err
			}
			data[i] = d
		}
	}
	// fillPairsData will call ZMData as well (with an empty Flags), but no cache should be stored
	if flags.Command.ArgCount() > 0 {
		s.addCache(key, flags.HashCode(), data)
	}
	return
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
	if err := IncrKey(tx, bkCounter, -int64(len(pairs))); err != nil {
		return err
	}
	return writeLog(tx, dd)
}
