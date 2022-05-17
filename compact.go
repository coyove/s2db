package main

import (
	"bytes"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	log "github.com/sirupsen/logrus"
)

func (s *Server) dsltWalker(tables [][]pebble.SSTableInfo, ttls []s2pkg.Pair) bool {
	defer s2pkg.Recover(nil)
	if len(ttls) == 0 {
		return false
	}

	// log := log.WithField("shard", strconv.Itoa(shard))
	var startKey, endKey []byte
	var probe = []byte("zsetskv_")

	for startKey == nil && len(tables) > 0 {
		tops := tables[0]
		tables = tables[1:]
		for len(tops) > 0 {
			i := rand.Intn(len(tops))
			table := tops[i]
			tops = append(tops[:i], tops[i+1:]...)

			if bytes.Compare(table.Smallest.UserKey, probe) <= 0 &&
				bytes.Compare(probe, table.Largest.UserKey) <= 0 {
				startKey = table.Smallest.UserKey
				endKey = table.Largest.UserKey
				break
			}
		}
	}

	if startKey == nil {
		log.Info("dsltWalker can't found valid keys")
		return false
	}

	log.Infof("dsltWalker started: %q-%q, set of ttls: %d", startKey, endKey, len(ttls))

	start := time.Now()
	keys := []s2pkg.Pair{}
	func() {
		c := s.DB.NewIter(&pebble.IterOptions{
			LowerBound: startKey,
			UpperBound: s2pkg.IncBytes(endKey),
		})
		defer c.Close()

		for c.SeekGE(probe); c.Valid() && time.Since(start).Seconds() < 5; {
			k := c.Key()[len(probe):]
			key := string(k[:bytes.IndexByte(k, 0)])
			if ttl := getTTLByName(ttls, key); ttl > 0 {
				lt := float64(int(clock.UnixNano()/1e9) - ttl)
				keys = append(keys, s2pkg.Pair{
					Member: key,
					Score:  lt,
				})
			}
			c.SeekGE(append(append(probe, key...), 0x01))
		}
	}()
	log.Infof("dsltWalker finished: %d keys collected in %v", len(keys), time.Since(start))

	for _, key := range keys {
		cmd := &redisproto.Command{
			Argv: [][]byte{[]byte("ZADD"), []byte(key.Member), []byte("DSLT"), s2pkg.FormatFloatBulk(key.Score)},
		}
		if err := s.checkWritable(); err != nil {
			return false
		}
		s.runPreparedTx("ZADD", key.Member, RunDefer,
			s.prepareZAdd(key.Member, nil, false, false, false, false, key.Score, dd(cmd)))
	}

	return true
}

func (s *Server) schedDSLTWalker() {
	var tablesCache struct {
		ts     time.Time
		tables [][]pebble.SSTableInfo
		ttls   []s2pkg.Pair
	}

	getTablesAndTTLs := func() ([][]pebble.SSTableInfo, []s2pkg.Pair) {
		if time.Since(tablesCache.ts).Seconds() < 60 {
			return tablesCache.tables, tablesCache.ttls
		}
		tables, err := s.DB.SSTables()
		if err != nil {
			log.Errorf("failed to list sstables: %v", err)
			return nil, nil
		}
		ttls, _ := s.ZRange(false, "dslt_clk", 0, -1, redisproto.Flags{LIMIT: s2pkg.RangeHardLimit})
		tablesCache.tables = tables
		tablesCache.ttls = ttls
		tablesCache.ts = time.Now()
		return tables, ttls
	}
	_ = getTablesAndTTLs

	for !s.Closed {
		if err := s.checkWritable(); err != nil {
			time.Sleep(time.Second)
			continue
		}

		// s.dsltWalker()
		time.Sleep(time.Minute * 10)
	}
}

func (s *Server) startCronjobs() {
	var run func(time.Duration, bool)
	run = func(d time.Duration, m bool) {
		if s.Closed {
			return
		}
		time.AfterFunc(d, func() { run(d, m) })
		if m {
			if s.DisableMetrics != 1 {
				if err := s.appendMetricsPairs(time.Hour * 24 * 30); err != nil {
					log.Error("AppendMetricsPairs: ", err)
				}
			}
		} else {
			s.runScriptFunc("cronjob" + strconv.Itoa(int(d.Seconds())))
		}
	}
	run(time.Second*30, false)
	run(time.Second*60, false)
	run(time.Second*60, true)
	run(time.Second*300, false)
}

func getTTLByName(ttls []s2pkg.Pair, name string) int {
	idx := sort.Search(len(ttls), func(i int) bool { return ttls[i].Member >= name })
	if idx < len(ttls) && name == ttls[idx].Member {
		return int(ttls[idx].Score)
	}
	if idx > 0 && idx <= len(ttls) && strings.HasPrefix(name, ttls[idx-1].Member) {
		return int(ttls[idx-1].Score)
	}
	return -1
}
