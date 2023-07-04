package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
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

type hashmapMerger struct {
	s *Server
	m map[string]hashmapData
}

func hashmapMergerIter(p []byte, f func(d hashmapData) bool) (err error) {
	if p[0] != 0x01 {
		return fmt.Errorf("setMerger: invalid opcode %x", p)
	}

	// count := binary.BigEndian.Uint32(p[1:])
	rd := bufio.NewReader(bytes.NewReader(p[1+4:]))
	for {
		// ts (8b) + keylen + key + valuelen + value
		var ts int64
		if err := binary.Read(rd, binary.BigEndian, &ts); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		kl, err := binary.ReadUvarint(rd)
		if err != nil {
			return err
		}
		k := make([]byte, kl)
		if _, err := io.ReadFull(rd, k); err != nil {
			return err
		}

		vl, err := binary.ReadUvarint(rd)
		if err != nil {
			return err
		}

		v := make([]byte, vl)
		if _, err := io.ReadFull(rd, v); err != nil {
			return err
		}

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
	return hashmapMergerIter(value, func(d hashmapData) bool {
		k := *(*string)(unsafe.Pointer(&d.key))
		if d.ts > s.m[k].ts {
			s.m[k] = d
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
				err := hashmapMergerIter(value, func(d hashmapData) bool {
					res.m[*(*string)(unsafe.Pointer(&d.key))] = d
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
	ttl := int64(s.Config.ExpireHardTTL)
	if ttl <= 0 {
		return
	}

	logrus.Infof("start L6 walking")
	start := time.Now()

	tables, err := s.DB.SSTables()
	if err != nil {
		logrus.Errorf("walkL6: list sstables: %v", err)
		return
	}

	for ii, i := range rand.Perm(len(tables[6])) {
		t := tables[6][i]
		log := logrus.WithField("shard", fmt.Sprintf("L6[%d/%d]", ii, len(tables[6])))

		if bytes.Compare(t.Largest.UserKey, []byte("l")) < 0 {
			continue
		}
		if bytes.Compare(t.Smallest.UserKey, []byte("m")) >= 0 {
			continue
		}
		k := tkp(uint64(t.FileNum))
		timestamp, err := s.GetInt64(k)
		if err != nil {
			log.Errorf("failed to get stored timestamp: %v", err)
			return
		}

		kkpRev := func(prefix []byte) (key []byte) {
			if bytes.HasPrefix(prefix, []byte("l")) {
				return prefix[1:bytes.IndexByte(prefix, 0)]
			}
			return prefix
		}

		if timestamp == 0 {
			minTimestamp, deletes, err := s.purgeSSTable(log, t, ttl)
			if err != nil {
				log.Errorf("[%d] delete range: %v", t.FileNum, err)
			} else {
				err := s.SetInt64(k, minTimestamp)
				log.Infof("[%d] update min timestamp: %v (%v), deletes %d in [%q, %q]",
					t.FileNum, minTimestamp, err, deletes, kkpRev(t.Smallest.UserKey), kkpRev(t.Largest.UserKey))
			}
			time.Sleep(time.Second / 2)
		} else if timestamp < future.UnixNano()-ttl*1e9 {
			_, deletes, err := s.purgeSSTable(log, t, ttl)
			if err != nil {
				log.Errorf("[%d] delete range: %v", t.FileNum, err)
			} else {
				log.Infof("[%d] purge %d in [%q, %q]",
					t.FileNum, deletes, kkpRev(t.Smallest.UserKey), kkpRev(t.Largest.UserKey))
			}
			time.Sleep(time.Second)
		} else {
			time.Sleep(time.Millisecond * 100)
		}
	}

	logrus.Infof("finish L6 walking in %v", time.Since(start))
}

func (s *Server) purgeSSTable(log *logrus.Entry, t pebble.SSTableInfo, ttl int64) (int64, int, error) {
	var wait, maxTx int
	fmt.Sscanf(s.Config.ExpireTx, "%d,%d", &maxTx, &wait)
	if wait == 0 || maxTx == 0 {
		return 0, 0, fmt.Errorf("invalid config")
	}

	for {
		c := s.DB.Metrics().Compact
		if c.NumInProgress == 0 {
			break
		}
		log.Infof("waiting for in-progress compactions %d/%d", c.NumInProgress, c.InProgressBytes)
		time.Sleep(time.Second * 10)
	}

	var tmp []byte
	var minTimestamp int64 = math.MaxInt64
	var deletes []string
	ddl := future.UnixNano() - ttl*1e9
	idx := s2.ConvertFutureTo16B(future.Future(ddl))

	// iter := s.DB.NewIter(&pebble.IterOptions{
	// 	LowerBound: t.Smallest.UserKey,
	// 	UpperBound: s2.IncBytes(t.Largest.UserKey),
	// })

	buf, err := ioutil.ReadFile(fmt.Sprintf("%s/%d.sst", s.DBPath, t.FileNum))
	if err != nil {
		return 0, 0, fmt.Errorf("open sst: %v", err)
	}
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
		if ts < minTimestamp {
			minTimestamp = ts
		}

		if ts < ddl {
			key := string(k[1:bytes.IndexByte(k, 0)])
			deletes = append(deletes, key)
		}

		tmp = append(tmp[:0], k[:bytes.IndexByte(k, 0)]...)
		ik, _ = iter.SeekGE(append(tmp, 1), true)
	}

	for i := 0; i < len(deletes); i += maxTx {
		end := i + maxTx
		if end > len(deletes) {
			end = len(deletes)
		}
		tx := s.DB.NewBatch()
		defer tx.Close()
		for j := i; j < end; j++ {
			k := kkp(deletes[j])
			if err := tx.DeleteRange(k, append(k, idx[:]...), pebble.NoSync); err != nil {
				return 0, 0, err
			}
		}
		if err := tx.Commit(pebble.NoSync); err != nil {
			return 0, 0, err
		}
		time.Sleep(time.Duration(wait) * time.Millisecond)
	}

	return minTimestamp, len(deletes), nil
}
