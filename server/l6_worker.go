package server

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/coyove/s2db/s2"
	"github.com/coyove/sdss/future"
	"github.com/sirupsen/logrus"
)

type hashmapData struct {
	ts   int64
	key  []byte
	data []byte
}

func (d hashmapData) clone() hashmapData {
	d.key = s2.Bytes(d.key)
	d.data = s2.Bytes(d.data)
	return d
}

func (d hashmapData) keystr() string {
	return *(*string)(unsafe.Pointer(&d.key))
}

type hashmapMerger struct {
	s *Server
	m map[string]hashmapData
}

func hashmapIterBytes(p []byte, f func(d hashmapData) bool) (err error) {
	if p[0] != 0x01 {
		return fmt.Errorf("hashmapIterBytes: invalid opcode %x", p)
	}

	// count := binary.BigEndian.Uint32(p[1:])
	rd := p[1+4:]
	for len(rd) > 0 {
		// ts (8b) + keylen + key + valuelen + value
		ts := int64(binary.BigEndian.Uint64(rd))
		rd = rd[8:]

		kl, n := binary.Uvarint(rd)
		rd = rd[n:]

		k := rd[:kl]
		rd = rd[kl:]

		vl, n := binary.Uvarint(rd)
		rd = rd[n:]

		v := rd[:vl]
		rd = rd[vl:]

		if !f(hashmapData{ts, k, v}) {
			break
		}
	}
	return
}

func (a *hashmapMerger) MergeNewer(value []byte) error {
	return a.MergeOlder(value)
}

func (s *hashmapMerger) MergeOlder(value []byte) error {
	return hashmapIterBytes(value, func(d hashmapData) bool {
		if d.ts > s.m[d.keystr()].ts {
			d = d.clone()
			s.m[d.keystr()] = d
		}
		return true
	})
}

func (s *hashmapMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	start := future.UnixNano()
	x := hashmapMergerBytes(s.m)
	s.s.Survey.HashMerger.Incr((future.UnixNano() - start) / 1e6)
	return x, nil, nil
}

func hashmapMergerBytes(m map[string]hashmapData) []byte {
	tmp := make([]byte, 0, len(m)*32)
	tmp = append(tmp, 0x01)
	tmp = binary.BigEndian.AppendUint32(tmp, 0)
	count := 0
	for k, v := range m {
		tmp = binary.BigEndian.AppendUint64(tmp, uint64(v.ts))
		tmp = binary.AppendUvarint(tmp, uint64(len(k)))
		tmp = append(tmp, k...)
		tmp = binary.AppendUvarint(tmp, uint64(len(v.data)))
		tmp = append(tmp, v.data...)
		if len(v.data) > 0 {
			count++
		}
	}
	binary.BigEndian.PutUint32(tmp[1:], uint32(count))
	return tmp
}

func (s *Server) createMerger() *pebble.Merger {
	return &pebble.Merger{
		Merge: func(key, value []byte) (pebble.ValueMerger, error) {
			if len(value) < 1 {
				return nil, fmt.Errorf("Merger: too short")
			}
			switch value[0] {
			case 1:
				res := &hashmapMerger{}
				res.s = s
				res.m = map[string]hashmapData{}
				err := hashmapIterBytes(value, func(d hashmapData) bool {
					d = d.clone()
					res.m[d.keystr()] = d
					return true
				})
				return res, err
			}
			return nil, fmt.Errorf("Merger: invalid opcode: %x", value[0])
		},

		Name: "pebble.concatenate", // keep the name same as the default one for data backward compatibilities
	}
}

func (s *Server) l6Purger() {
	if testFlag {
		return
	}

	finished := false

	defer func(start time.Time) {
		finished = true
		w := time.Duration(s.Config.L6PurgerSleepSecs) * time.Second
		logrus.Infof("finish L6 purger in %v, next scheduled at %v",
			time.Since(start), time.Now().UTC().Add(w).Format(time.Stamp))
		time.AfterFunc(w, func() { s.l6Purger() })
	}(time.Now())

	var i int
	level, err := s.collectSST("l6purger")
	if err != nil {
		logrus.Errorf("list sstables: %v", err)
		return
	}
	rand.Shuffle(len(level), func(i, j int) { level[i], level[j] = level[j], level[i] })

	go func() {
		for range time.Tick(time.Second) {
			if finished {
				return
			}
			s.Survey.L6PurgerProgress.Incr(int64(i + 1))
		}
	}()

	log := logrus.WithField("shard", "L6PG")
	for i = range level {
		t := level[i]

		for {
			c := s.DB.Metrics().Compact
			if c.NumInProgress == 0 {
				break
			}
			log.Infof("waiting for in-progress compactions %d/%d", c.NumInProgress, c.InProgressBytes)
			time.Sleep(time.Second * 10)
		}

		if err := s.purgeSSTable(log, t); err != nil {
			log.Errorf("[%d] failed to ttl purge: %v", t.FileNum, err)
		}
	}
}

func (s *Server) l6Deduper() {
	if testFlag {
		return
	}

	var finished bool
	var i int
	var marked int

	defer func(start time.Time) {
		finished = true
		w := time.Duration(s.Config.L6DeduperSleepSecs) * time.Second
		logrus.Infof("finish L6 deduper in %v, processed %d sst, next round scheduled at %v",
			time.Since(start), marked, time.Now().UTC().Add(w).Format(time.Stamp))
		time.AfterFunc(w, func() { s.l6Deduper() })
	}(time.Now())

	level, err := s.collectSST("l6deduper")
	if err != nil {
		logrus.Errorf("list sstables: %v", err)
		return
	}

	go func() {
		for range time.Tick(time.Second) {
			if finished {
				return
			}
			s.Survey.L6DeduperProgress.Incr(int64(i + 1))
		}
	}()

	log := logrus.WithField("shard", "L6DD")
	for i = range level {
		t := level[i]
		mark, err := s.dedupSSTable(log, t)
		if err != nil {
			log.Errorf("[%d] failed to dedup sst: %v", t.FileNum, err)
			return
		}
		if mark {
			marked++
		}
	}
}

func (s *Server) purgeSSTable(log *logrus.Entry, t pebble.SSTableInfo) error {
	ttl := int64(s.Config.ListRetentionDays) * 86400 * 1e9
	if ttl <= 0 {
		time.Sleep(time.Millisecond * 100)
		return nil
	}

	tsKey := strconv.AppendUint([]byte("t"), uint64(t.FileNum), 10)
	startTimestamp, err := s.GetInt64(tsKey)
	if err != nil {
		return fmt.Errorf("failed to get stored timestamp: %v", err)
	}

	var wait, maxTx int
	fmt.Sscanf(s.Config.L6WorkerMaxTx, "%d,%d", &maxTx, &wait)
	if wait == 0 || maxTx == 0 {
		return fmt.Errorf("invalid config")
	}

	var minTimestamp int64 = math.MaxInt64
	var deletes int
	var tmp []byte
	ddl := future.UnixNano() - ttl

	rd, err := s.readSST(uint64(t.FileNum))
	if err != nil {
		return fmt.Errorf("read sst: %v", err)
	}
	if rd == nil {
		return nil
	}
	defer rd.Close()

	iter, err := rd.NewIter(nil, s2.IncBytes(t.Largest.UserKey))
	if err != nil {
		return fmt.Errorf("sst new iter: %v", err)
	}
	defer iter.Close()

	tx := s.DB.NewBatch()
	defer func() {
		tx.Close()
	}()

	for ik, _ := iter.First(); ik != nil; {
		k := ik.UserKey
		if k[0] < 'l' {
			ik, _ = iter.Next()
			continue
		}
		if k[0] >= 'm' {
			break
		}

		id := k[bytes.IndexByte(k, 0)+1:]
		ts := int64(s2.Convert16BToFuture(id))
		if ts <= startTimestamp {
			ik, _ = iter.Next()
			continue
		}

		if ts <= ddl {
			if (s2.Pair{ID: id}).Cmd()&s2.PairCmdAppendNoExpire > 0 {
				goto NOEXP
			}
			deletes++
			tx.Delete(k, pebble.NoSync)
			if tx.Count() >= uint32(maxTx) {
				if err := tx.Commit(pebble.NoSync); err != nil {
					return err
				}
				tx.Close()
				time.Sleep(time.Duration(wait) * time.Millisecond)
				tx = s.DB.NewBatch()
			}
		NOEXP:
			ik, _ = iter.Next()
		} else {
			if ts < minTimestamp {
				minTimestamp = ts
			}

			tmp = append(tmp[:0], k[:bytes.IndexByte(k, 0)]...)
			ik, _ = iter.SeekGE(append(tmp, 1), true)
		}
	}

	tx.Set(tsKey, s2.Uint64ToBytes(uint64(minTimestamp)), pebble.NoSync)

	if startTimestamp == 0 {
		log.Infof("[%d] new sst: %v", t.FileNum, time.Unix(0, minTimestamp))
	} else if minTimestamp == math.MaxInt64 {
		log.Infof("[%d] sst all purged", t.FileNum)
	} else {
		log.Debugf("[%d] timestamp: %v -> %v", t.FileNum, time.Unix(0, startTimestamp), time.Unix(0, minTimestamp))
	}

	kkpRev := func(prefix []byte) (key []byte) {
		if bytes.HasPrefix(prefix, []byte("l")) {
			return prefix[1:bytes.IndexByte(prefix, 0)]
		}
		return prefix
	}

	log.Debugf("[%d] deletes %d within [%q, %q]", t.FileNum, deletes, kkpRev(t.Smallest.UserKey), kkpRev(t.Largest.UserKey))
	s.Survey.L6TTLDeletes.Incr(int64(deletes))

	return tx.Commit(pebble.NoSync)
}

func (s *Server) dedupSSTable(log *logrus.Entry, t pebble.SSTableInfo) (bool, error) {
	dedupMarkKey := strconv.AppendUint([]byte("td"), uint64(t.FileNum), 10)
	mark, err := s.GetInt64(dedupMarkKey)
	if err != nil {
		return false, err
	}
	if mark != 0 {
		return false, nil
	}

	var wait, maxTx int
	fmt.Sscanf(s.Config.L6WorkerMaxTx, "%d,%d", &maxTx, &wait)
	if wait == 0 || maxTx == 0 {
		return false, fmt.Errorf("invalid config")
	}

	for {
		c := s.DB.Metrics().Compact
		if c.NumInProgress == 0 {
			break
		}
		log.Infof("waiting for in-progress compactions %d/%d", c.NumInProgress, c.InProgressBytes)
		time.Sleep(time.Second * 10)
	}

	rd, err := s.readSST(uint64(t.FileNum))
	if err != nil {
		return false, err
	}
	if rd == nil {
		return false, nil
	}
	defer rd.Close()

	tx := s.DB.NewBatch()
	defer func() {
		tx.Close()
	}()

	iter, err := rd.NewIter(nil, nil)
	if err != nil {
		return false, fmt.Errorf("sst new iter: %v", err)
	}
	defer iter.Close()

	var lastKey string
	var lastCounter int
	var globalCounter int
	var globalDeletes int
	var globalCMDeletes int

	dedup := map[[sha1.Size]byte]struct{}{}
	var curCMKey []byte
	for ik, iv := iter.Last(); ik != nil; ik, iv = iter.Prev() {
		k := ik.UserKey
		if k[0] < 'l' {
			break
		}
		if k[0] >= 'm' {
			continue
		}

		id := k[bytes.IndexByte(k, 0)+1:]

		cookie, ok := s2.Convert16BToFuture(id).Cookie()
		if ok && cookie == consolidatedMark {
			if len(curCMKey) > 0 {
				// We are switching to the next block as 'curCMKey' changes.
				// Note that 'curCMKey' is currently not empty, indicating all keys in this block have been dedup-ed,
				// so 'curCMKey' itself can be deleted, which literally means this block is deleted.
				tx.Delete(curCMKey, pebble.NoSync)
				globalCMDeletes++
			}
			curCMKey = append(curCMKey[:0], k...)
			globalCounter++
			continue
		}
		if len(iv) == 0 || len(k) == 0 {
			continue
		}

		dp := s2.Pair{ID: id, Data: iv}.DistinctPrefix()
		if dp == nil {
			continue
		}
		dh := sha1.Sum(dp)

		key := k[1:bytes.IndexByte(k, 0)]
		if *(*string)(unsafe.Pointer(&key)) != lastKey {
			log.Debugf("purge %q, remains %d keys, before %d", key, len(dedup), lastCounter)
			for k := range dedup {
				delete(dedup, k)
			}
			lastCounter = 0
			lastKey = string(key)
		}

		lastCounter++
		globalCounter++

		if _, ok := dedup[dh]; !ok {
			dedup[dh] = struct{}{}

			// We are inside 'curCMKey' block and met a key that is not duplicated, which means this block
			// will not be empty after dedup, thus clear 'curCMKey' to avoid deleting this block.
			curCMKey = curCMKey[:0]
			continue
		}

		tx.Delete(k, pebble.NoSync)
		if tx.Count() >= uint32(maxTx) {
			if err := tx.Commit(pebble.NoSync); err != nil {
				return false, err
			}
			tx.Close()
			time.Sleep(time.Duration(wait) * time.Millisecond)
			tx = s.DB.NewBatch()
		}
		globalDeletes++
	}
	tx.Set(dedupMarkKey, s2.Uint64ToBytes(1), pebble.NoSync)

	if globalCounter > 0 {
		log.Debugf("dedup sst %d, purged %d + %d out of %d", t.FileNum, globalDeletes, globalCMDeletes, globalCounter)
		s.Survey.L6DedupBefore.Incr(int64(globalCounter))
		s.Survey.L6DedupDeletes.Incr(int64(globalDeletes))
		s.Survey.L6DedupCMDeletes.Incr(int64(globalCMDeletes))
	}
	return true, tx.Commit(pebble.NoSync)
}

func (s *Server) collectSST(source string) ([]pebble.SSTableInfo, error) {
	tables, err := s.DB.SSTables()
	if err != nil {
		return nil, err
	}

	var level []pebble.SSTableInfo
	var levelNum []int
	for i := 1; i < len(tables); i++ {
		levelNum = append(levelNum, 0)
		for _, t := range tables[i] {
			if bytes.Compare(t.Largest.UserKey, []byte("l")) < 0 {
				continue
			}
			if bytes.Compare(t.Smallest.UserKey, []byte("m")) >= 0 {
				continue
			}
			level = append(level, t)
			levelNum[len(levelNum)-1]++
		}
	}
	logrus.Infof("[%s] collected %d sst %v, L0=%d", source, len(level), levelNum, len(tables[0]))
	return level, nil
}

func (s *Server) readSST(idx uint64) (*sstable.Reader, error) {
	buf, err := ioutil.ReadFile(fmt.Sprintf("%s/%06d.sst", s.DBPath, idx))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read sst %d: %v", idx, err)
	}
	rd, err := sstable.NewMemReader(buf, sstable.ReaderOptions{})
	if err != nil {
		return nil, fmt.Errorf("parse sst %d: %v", idx, err)
	}
	return rd, nil
}
