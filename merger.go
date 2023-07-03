package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
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

	log := logrus.WithField("shard", "l6worker")
	log.Infof("start walking")

	tables, err := s.DB.SSTables()
	if err != nil {
		log.Errorf("list sstables: %v", err)
		return
	}

	for _, t := range tables[6] {
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

	log.Infof("finish walking")
}

func (s *Server) purgeSSTable(log *logrus.Entry, t pebble.SSTableInfo, ttl int64) (int64, int, error) {
	ss := s.DB.NewSnapshot()
	defer ss.Close()
	iter := ss.NewIter(&pebble.IterOptions{
		LowerBound: t.Smallest.UserKey,
		UpperBound: s2.IncBytes(t.Largest.UserKey),
	})
	defer iter.Close()
	var tmp []byte
	var minTimestamp int64 = math.MaxInt64
	var deletes int

	tx := s.DB.NewBatch()
	defer func() {
		tx.Close()
	}()

	// startKey, _ := s.Get(trkp(uint64(t.FileNum)))
	// if len(startKey) > 0 {
	// 	log.Infof("[%d] found start key: %q", t.FileNum, startKey)
	// 	iter.SeekGE(startKey)
	// } else {
	iter.First()
	// }

	for iter.Valid() {
		k := iter.Key()
		if bytes.Compare(k, []byte("l")) < 0 {
			iter.Next()
			continue
		}
		if bytes.Compare(k, []byte("m")) >= 0 {
			break
		}

		ts := int64(s2.Convert16BToFuture(k[bytes.IndexByte(k, 0)+1:]))
		if ts < minTimestamp {
			minTimestamp = ts
		}

		if ddl := future.UnixNano() - ttl*1e9; ts < ddl {
			deletes++
			key := string(k[1:bytes.IndexByte(k, 0)])
			idx := s2.ConvertFutureTo16B(future.Future(ddl))
			if err := tx.DeleteRange(k, append(kkp(key), idx[:]...), pebble.NoSync); err != nil {
				return 0, 0, err
			}
			if tx.Count() >= uint32(s.Config.ExpireTxLimit) {
				// log.Infof("[%d] switch tx: %d %q", t.FileNum, deletes, iter.Key())
				// tx.Set(trkp(uint64(t.FileNum)), iter.Key(), pebble.NoSync)
				if err := tx.Commit(pebble.NoSync); err != nil {
					return 0, 0, err
				}
				tx.Close()
				time.Sleep(time.Second)
				tx = s.DB.NewBatch()
			}
		}

		tmp = append(tmp[:0], k[:bytes.IndexByte(k, 0)]...)
		iter.SeekGE(append(tmp, 1))
	}
	return minTimestamp, deletes, tx.Commit(pebble.NoSync)
}
