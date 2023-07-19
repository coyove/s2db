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

func (s *Server) walkL6Tables() {
	if testFlag {
		return
	}

	ttl := int64(s.Config.ListRetentionDays) * 86400 * 1e9
	finished := false

	defer func(start time.Time) {
		finished = true
		// if ttl <= 0 {
		// 	time.AfterFunc(time.Minute, func() { s.walkL6Tables() })
		// 	return
		// }

		w := time.Hour
		logrus.Infof("finish L6 walking in %v, next scheduled at %v",
			time.Since(start), time.Now().UTC().Add(w).Format(time.Stamp))
		time.AfterFunc(w, func() { s.walkL6Tables() })
	}(time.Now())

	tables, err := s.DB.SSTables()
	if err != nil {
		logrus.Errorf("walkL6: list sstables: %v", err)
		return
	}

	var ii, i int
	go func() {
		for range time.Tick(time.Second) {
			if finished {
				return
			}
			s.Survey.L6WorkerProgress.Incr(int64(ii + 1))
		}
	}()

	level := tables[6]
	for i := 5; len(level) < 100 && i >= 0; i-- {
		level = append(level, tables[i]...)
		logrus.Infof("walkL6: add more sst from level %d: %d", i, len(tables[i]))
	}
	logrus.Infof("walkL6: start walking %d sst", len(level))

	for ii, i = range rand.Perm(len(level)) {
		t := level[i]
		log := logrus.WithField("shard", fmt.Sprintf("L6[%d/%d]", ii, len(level)))

		if bytes.Compare(t.Largest.UserKey, []byte("l")) < 0 {
			continue
		}
		if bytes.Compare(t.Smallest.UserKey, []byte("m")) >= 0 {
			continue
		}

		for {
			c := s.DB.Metrics().Compact
			if c.NumInProgress == 0 {
				break
			}
			log.Infof("waiting for in-progress compactions %d/%d", c.NumInProgress, c.InProgressBytes)
			time.Sleep(time.Second * 10)
		}

		buf, err := ioutil.ReadFile(fmt.Sprintf("%s/%d.sst", s.DBPath, t.FileNum))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			log.Errorf("open sst %d: %v", t.FileNum, err)
			continue
		}

		if err := s.dedupSSTable(log, buf, t); err != nil {
			log.Errorf("failed to dedup sst: %v", err)
			return
		}

		timestamp, err := s.GetInt64(makeSSTableWMKey(uint64(t.FileNum)))
		if err != nil {
			log.Errorf("failed to get stored timestamp: %v", err)
			return
		}

		if ttl > 0 && timestamp < future.UnixNano()-ttl {
			minTimestamp, deletes, err := s.purgeSSTable(log, buf, timestamp, t, ttl)
			if err != nil {
				log.Errorf("[%d] purge: %v", t.FileNum, err)
			} else {
				if timestamp == 0 {
					log.Debugf("[%d] new sst: %v", t.FileNum, minTimestamp)
				} else if minTimestamp == math.MaxInt64 {
					log.Debugf("[%d] sst all purged", t.FileNum)
				} else {
					log.Debugf("[%d] timestamp: %v -> %v (+%d)", t.FileNum, timestamp, minTimestamp, (minTimestamp-timestamp)/1e9)
				}

				kkpRev := func(prefix []byte) (key []byte) {
					if bytes.HasPrefix(prefix, []byte("l")) {
						return prefix[1:bytes.IndexByte(prefix, 0)]
					}
					return prefix
				}

				log.Debugf("[%d] deletes %d within [%q, %q]", t.FileNum, deletes, kkpRev(t.Smallest.UserKey), kkpRev(t.Largest.UserKey))
				s.Survey.L6TTLDeletes.Incr(int64(deletes))
			}
			time.Sleep(time.Second)
		} else {
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (s *Server) purgeSSTable(log *logrus.Entry, buf []byte, startTimestamp int64, t pebble.SSTableInfo, ttl int64) (int64, int, error) {
	var wait, maxTx int
	fmt.Sscanf(s.Config.L6WorkerMaxTx, "%d,%d", &maxTx, &wait)
	if wait == 0 || maxTx == 0 {
		return 0, 0, fmt.Errorf("invalid config")
	}

	var minTimestamp int64 = math.MaxInt64
	var deletes int
	var tmp []byte
	ddl := future.UnixNano() - ttl

	rd, err := sstable.NewMemReader(buf, sstable.ReaderOptions{})
	if err != nil {
		return 0, 0, fmt.Errorf("read sst: %v", err)
	}
	defer rd.Close()

	iter, err := rd.NewIter(nil, s2.IncBytes(t.Largest.UserKey))
	if err != nil {
		return 0, 0, fmt.Errorf("sst new iter: %v", err)
	}
	defer iter.Close()

	tx := s.DB.NewBatch()
	defer func() {
		tx.Close()
	}()

	for ik, _ := iter.First(); ik != nil; {
		k := ik.UserKey
		if bytes.Compare(k, []byte("l")) < 0 {
			ik, _ = iter.Next()
			continue
		}
		if bytes.Compare(k, []byte("m")) >= 0 {
			break
		}

		ts := int64(s2.Convert16BToFuture(k[bytes.IndexByte(k, 0)+1:]))
		if ts <= startTimestamp {
			ik, _ = iter.Next()
			continue
		}

		if ts <= ddl {
			deletes++
			tx.Delete(k, pebble.NoSync)
			if tx.Count() >= uint32(maxTx) {
				if err := tx.Commit(pebble.NoSync); err != nil {
					return 0, 0, err
				}
				tx.Close()
				time.Sleep(time.Duration(wait) * time.Millisecond)
				tx = s.DB.NewBatch()
			}
			ik, _ = iter.Next()
		} else {
			if ts < minTimestamp {
				minTimestamp = ts
			}

			tmp = append(tmp[:0], k[:bytes.IndexByte(k, 0)]...)
			ik, _ = iter.SeekGE(append(tmp, 1), true)
		}
	}

	tx.Set(makeSSTableWMKey(uint64(t.FileNum)), s2.Uint64ToBytes(uint64(minTimestamp)), pebble.NoSync)

	return minTimestamp, deletes, tx.Commit(pebble.NoSync)
}

func (s *Server) dedupSSTable(log *logrus.Entry, buf []byte, t pebble.SSTableInfo) error {
	mark, err := s.GetInt64(makeSSTableDedupKey(uint64(t.FileNum)))
	if err != nil {
		return err
	}
	if mark != 0 {
		return nil
	}

	var wait, maxTx int
	fmt.Sscanf(s.Config.L6WorkerMaxTx, "%d,%d", &maxTx, &wait)
	if wait == 0 || maxTx == 0 {
		return fmt.Errorf("invalid config")
	}

	rd, err := sstable.NewMemReader(buf, sstable.ReaderOptions{})
	if err != nil {
		return fmt.Errorf("read sst: %v", err)
	}
	defer rd.Close()

	tx := s.DB.NewBatch()
	defer func() {
		tx.Close()
	}()

	iter, err := rd.NewIter(nil, nil)
	if err != nil {
		return fmt.Errorf("sst new iter: %v", err)
	}
	defer iter.Close()

	var lastKey string
	var lastCounter int
	var globalCounter int
	var globalDeletes int

	dedup := map[[sha1.Size]byte]struct{}{}
	for ik, iv := iter.Last(); ik != nil; ik, iv = iter.Prev() {
		k := ik.UserKey
		if len(iv) == 0 || len(k) == 0 {
			continue
		}
		if k[0] < 'l' {
			break
		}
		if k[0] >= 'm' {
			continue
		}

		dh, ok := s2.Pair{Data: iv}.DataDistinctHash()
		if !ok {
			continue
		}

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
			continue
		}

		tx.Delete(k, pebble.NoSync)
		if tx.Count() >= uint32(maxTx) {
			if err := tx.Commit(pebble.NoSync); err != nil {
				return err
			}
			tx.Close()
			time.Sleep(time.Duration(wait) * time.Millisecond)
			tx = s.DB.NewBatch()
		}
		globalDeletes++
	}
	tx.Set(makeSSTableDedupKey(uint64(t.FileNum)), s2.Uint64ToBytes(1), pebble.NoSync)

	if globalCounter > 0 {
		log.Debugf("dedup sst %d, purged %d out of %d", t.FileNum, globalDeletes, globalCounter)
		s.Survey.L6DedupBefore.Incr(int64(globalCounter))
		s.Survey.L6DedupDeletes.Incr(int64(globalDeletes))
	}
	return tx.Commit(pebble.NoSync)
}
