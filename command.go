package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/s2"
	"github.com/coyove/sdss/future"
	"github.com/sirupsen/logrus"
)

const (
	consolidatedMark = 1
	eolMark          = 2
	maxCursor        = "\x7f\xff\xff\xff\xcd\x0d\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00"
	minCursor        = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
)

func (s *Server) updateWatermarkCache(ck [16]byte, new []byte) {
	_ = new[15]
	// fmt.Println(s.ln.Addr(), new)
	s.wmCache.Update16(ck, func(old [16]byte) [16]byte {
		if bytes.Compare(new, old[:]) > 0 {
			copy(old[:], new)
		}
		return old
	})
}

func (s *Server) getHLL(key string) (s2.HyperLogLog, s2.HyperLogLog, error) {
	v, err := s.Get(append([]byte("H"), key...))
	if err != nil {
		return nil, nil, err
	}
	switch len(v) {
	case s2.HLLSize * 2:
		return v[:s2.HLLSize], v[s2.HLLSize:], nil
	case s2.HLLSize:
		return v, nil, nil
	case 0:
		return nil, nil, nil
	}
	return nil, nil, fmt.Errorf("invalid HLL size %d of %q", len(v), key)
}

func (s *Server) setHLL(tx *pebble.Batch, key string, add, del s2.HyperLogLog) error {
	return tx.Set(append([]byte("H"), key...), append(add, del...), pebble.Sync)
}

func (s *Server) deleteElement(tx *pebble.Batch, bkPrefix, key []byte, hllDel s2.HyperLogLog) error {
	if err := tx.Delete(key, pebble.Sync); err != nil {
		return err
	}

	idx := bytes.TrimPrefix(key, bkPrefix)
	hllDel.Add(uint32(s2.HashBytes(idx)))

	return nil
}

func (s *Server) Append(key string, ids, data [][]byte, ttlSec int64, wait bool) ([][]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("append to null key")
	}
	kh := sha1.Sum([]byte(key))

	var kk [][]byte
	var p []s2.Pair
	var id future.Future

	for i := 0; i < len(data); i++ {
		if len(data[i]) == 0 {
			return nil, fmt.Errorf("can't append null data")
		}

		if len(ids) == len(data) {
			kk = append(kk, ids[i])
			p = append(p, s2.Pair{ID: ids[i], Data: data[i]})
		} else {
			id = future.Get(s.Channel)
			if testFlag {
				x, _ := rand.Int(rand.Reader, big.NewInt(1<<32))
				if v, _ := testDedup.LoadOrStore(id, x.Int64()); v != x.Int64() {
					panic("fatal: duplicated id")
				}
			}

			var idx [16]byte // id(8b) + keyhash(4b) + random(1b) + shard(1b) + cmd(1b) + extra(1b)
			binary.BigEndian.PutUint64(idx[:], uint64(id))
			copy(idx[8:12], kh[:])
			rand.Read(idx[12:13])
			idx[13] = 0 // shard index, not used by now
			idx[14] = byte(s.Channel)<<4 | s2.PairCmdAppend
			idx[15] = 0 // extra

			k := s2.Bytes(idx[:])
			kk = append(kk, k)
			p = append(p, s2.Pair{ID: k, Data: data[i]})
		}
	}

	if _, err := s.rawSet(key, p, ttlSec, nil); err != nil {
		return nil, err
	}

	if wait && id > 0 {
		id.Wait()
	}
	return kk, nil
}

func (s *Server) setMissing(key string, before, after []s2.Pair,
	consolidate, consolidateLeft, consolidateRight bool) error {
	bkPrefix := kkp(key)

	con := func(tx *pebble.Batch) {
		m := map[future.Future]bool{}
		for _, p := range s2.TrimPairsForConsolidation(after, !consolidateLeft, !consolidateRight) {
			if p.C {
				continue
			}
			cid := p.Future().ToCookie(consolidatedMark)
			if m[cid] {
				continue
			}
			m[cid] = true
			idx := s2.ConvertFutureTo16B(cid)
			tx.Set(append(bkPrefix, idx[:]...), nil, pebble.Sync)
		}
	}

	before = sortPairs(before, true)
	var missing []s2.Pair
	for _, a := range after {
		idx := sort.Search(len(before), func(i int) bool { return !before[i].Less(a) })
		if idx < len(before) && before[idx].Equal(a) {
			// Existed
		} else {
			missing = append(missing, a)
		}
	}

	if len(missing) == 0 {
		if consolidate {
			tx := s.DB.NewBatch()
			defer tx.Close()
			if con(tx); tx.Count() > 0 {
				if err := tx.Commit(pebble.Sync); err != nil {
					logrus.Errorf("setMissing consolidation: %v", err)
					return err
				}
			}
		}
		s.Survey.PeerOnOK.Incr(1)
		return nil
	}

	if s.test.NoSetMissing {
		panic("test: no set missing")
	}

	start := time.Now()

	count, err := s.rawSet(key, missing, 0, func(tx *pebble.Batch) {
		if consolidate {
			con(tx)
		}
	})
	s.Survey.PeerOnMissing.Incr(time.Since(start).Milliseconds())
	if err != nil {
		logrus.Errorf("setMissing: %v", err)
		return err
	}
	s.Survey.PeerOnMissingN.Incr(int64(count))
	return nil
}

func (s *Server) rawSet(key string, data []s2.Pair, ttlSec int64, f func(*pebble.Batch)) (int, error) {
	bkPrefix := kkp(key)

	add, del, err := s.getHLL(key)
	if err != nil {
		return 0, err
	}

	ck := s2.HashStr128(key)
	tx := s.DB.NewBatch()
	defer tx.Close()

	for _, kv := range data {
		if _, ok := s.fillCache.Get(kv.ID); ok {
			continue
		}
		if err := tx.Set(append(bkPrefix, kv.ID...), kv.Data, pebble.Sync); err != nil {
			return 0, err
		}
		s.fillCache.Add(kv.ID, struct{}{})
		s.updateWatermarkCache(ck, kv.ID)
		add.Add(uint32(s2.HashBytes(kv.ID)))
	}

	kh := sha1.Sum([]byte(key))
	if err := tx.Set(append(append([]byte("z"), kh[:4]...), key...), nil, pebble.Sync); err != nil {
		return 0, err
	}

	if f != nil {
		f(tx)
	}

	if ttlSec > 0 {
		if s.ttlOnce.lock(key) {
			defer s.ttlOnce.unlock(key)
			s.Survey.TTLOnce.Incr(s.ttlOnce.count())

			f := future.Future(future.UnixNano() - ttlSec*1e9)
			idx := s2.ConvertFutureTo16B(f)
			iter := s.DB.NewIter(&pebble.IterOptions{
				LowerBound: bkPrefix,
				UpperBound: append(bkPrefix, idx[:]...),
			})
			defer iter.Close()

			count := 0
			for iter.First(); iter.Valid() && count < *ttlEvictLimit; iter.Next() {
				if err := s.deleteElement(tx, bkPrefix, iter.Key(), del); err != nil {
					return 0, err
				}
				count++
			}
			if count > 0 {
				idx = s2.ConvertFutureTo16B(f.ToCookie(eolMark))
				if err := tx.Set(append(bkPrefix, idx[:]...), nil, pebble.Sync); err != nil {
					return 0, err
				}
			}
			s.Survey.AppendExpire.Incr(int64(count))
		}
	}

	if err := s.setHLL(tx, key, add, del); err != nil {
		return 0, err
	}
	return int(tx.Count()), tx.Commit(pebble.Sync)
}

func (s *Server) LookupID(id []byte) (data []byte, key string, err error) {
	_ = id[15]
	iter := newPrefixIter(s.DB, append([]byte{'z'}, id[8:12]...))
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()[5:]
		bkPrefix := kkp(*(*string)(unsafe.Pointer(&key)))

		v, err := s.Get(append(bkPrefix, id...))
		if err != nil {
			return nil, "", err
		}
		if v == nil {
			continue
		}
		return v, string(key), nil
	}
	return
}

const (
	RangeDesc     = 1
	RangeDistinct = 2
	RangeRaw      = 4
)

func (s *Server) Range(key string, start []byte, n int, flag int) (data []s2.Pair, partial bool, err error) {
	bkPrefix := kkp(key)

	c := newPrefixIter(s.DB, bkPrefix)
	defer c.Close()

	// for c.First(); c.Valid(); c.Next() {
	// 	fmt.Println(c.Key())
	// }

	c.Last()
	s.wmCache.Update16(s2.HashStr128(key), func(old [16]byte) (new [16]byte) {
		k := bytes.TrimPrefix(c.Key(), bkPrefix)
		if bytes.Compare(k, old[:]) > 0 {
			copy(new[:], k)
			return new
		}
		return old
	})

	desc := flag&RangeDesc > 0
	if desc {
		// OLDER                            NEWER
		//
		//    blk 0   |     blk 1    |    blk 2
		//            |   ,-- start  |
		//            |  /           |
		// ~~~K0---K1-+-K2---k3---cm-+-K4---K5~~~
		//                        |
		//                        `-- actual start
		//
		// 'cm' is the consolidation mark of 'blk 1' which contains K2 and K3.

		cm := s2.Convert16BToFuture(start).ToCookie(consolidatedMark)
		buf := s2.ConvertFutureTo16B(cm)
		c.SeekLT(append(bkPrefix, s2.IncBytesInplace(buf[:])...))
		// fmt.Println(start, buf, cm, s2.Convert16BToFuture(start))
	} else {
		c.SeekGE(append(bkPrefix, start...))
	}

	cm := map[future.Future]bool{}
	dedup := map[string]bool{}
	var dedupTx *pebble.Batch
	var hllAdd, hllDel s2.HyperLogLog

	if flag&RangeDistinct > 0 {
		if s.delOnce.lock(key) {
			s.Survey.DistinctOnce.Incr(s.delOnce.count())
			dedupTx = s.DB.NewBatch()
			defer dedupTx.Close()
			defer s.delOnce.unlock(key)

			hllAdd, hllDel, err = s.getHLL(key)
			if err != nil {
				return nil, false, err
			}
		}
	}

	ns := future.UnixNano()
	rangeAll := flag&RangeRaw > 0
	for c.Valid() {
		k := bytes.TrimPrefix(c.Key(), bkPrefix)
		p := s2.Pair{
			ID:   s2.Bytes(k),
			Data: s2.Bytes(c.Value()),
		}
		if rangeAll {
			data = append(data, p)
			goto NEXT
		}

		if v, ok := p.Future().Cookie(); ok {
			if v == consolidatedMark {
				cm[p.Future()] = true
			} else if v == eolMark {
				if desc {
					break
				}
			} else {
				return nil, false, fmt.Errorf("invalid mark: %x", v)
			}
		} else {
			if dedupTx != nil && dedup[p.DataForDistinct()] {
				s.deleteElement(dedupTx, bkPrefix, c.Key(), hllDel)
				goto NEXT
			}

			if desc {
				// Desc-ranging may start beyond 'start' cursor, shown by the graph above.
				if bytes.Compare(k, start) <= 0 {
					data = append(data, p)
					dedup[p.DataForDistinct()] = true
				}
			} else {
				data = append(data, p)
				dedup[p.DataForDistinct()] = true
			}
		}

	NEXT:
		if len(data) >= n+2 {
			// If we have collected enough Pairs where last two Pairs belong to different blocks,
			// we can safely exit the loop.
			d0, d1 := data[len(data)-1], data[len(data)-2]
			if d0.UnixNano()/future.Block != d1.UnixNano()/future.Block {
				break
			}
		}

		if future.UnixNano()-ns > int64(s.Config.TimeoutRange)*1e6 {
			// Iterating timed out
			partial = true
			break
		}

		if dedupTx != nil && dedupTx.Count() > uint32(s.Config.DistinctLimit) {
			// Too many deletions in one request.
			partial = true
			break
		}

		moveIter(c, desc)
	}

	if len(data) > n {
		data = data[:n]
	}

	for i, p := range data {
		cid := p.Future().ToCookie(consolidatedMark)
		if cm[cid] {
			data[i].C = true
		}
	}

	if dedupTx != nil && dedupTx.Count() > 0 {
		s.Survey.RangeDistinct.Incr(int64(dedupTx.Count()))
		s.setHLL(dedupTx, key, hllAdd, hllDel)

		// Use NoSync here because loss of deletions is okay.
		// They will be eventually deleted again next time we call Range().
		if err := dedupTx.Commit(pebble.NoSync); err != nil {
			logrus.Errorf("range distinct commit failed: %v", err)
		}
	}
	return
}

func (s *Server) ScanHash(cursor string, count int) (nextCursor string, keys []string) {
	iter := newPrefixIter(s.DB, []byte("h"))
	defer iter.Close()

	for iter.SeekGE(skp(cursor)); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()[1:]))
		if len(keys) >= count {
			if iter.Next() {
				nextCursor = string(iter.Key()[1:])
			}
			break
		}
	}
	return
}

func (s *Server) ScanIndex(cursor string, count int) (nextCursor string, keys []string) {
	iter := newPrefixIter(s.DB, []byte("z"))
	defer iter.Close()

	kh := sha1.Sum([]byte(cursor))

	start := append(append([]byte("z"), kh[:4]...), cursor...)
	for iter.SeekGE(start); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()[5:]))
		if len(keys) >= count {
			if iter.Next() {
				nextCursor = string(iter.Key()[5:])
			}
			break
		}
	}
	return
}

func (s *Server) Scan(cursor string, count int) (nextCursor string, keys []string) {
	iter := newPrefixIter(s.DB, []byte("l"))
	defer iter.Close()

	cPrefix := kkp(cursor)
	var tmp []byte
	for iter.SeekGE(cPrefix); iter.Valid(); {
		k := iter.Key()
		k = k[:bytes.IndexByte(k, 0)]
		keys = append(keys, string(k[1:]))

		tmp = append(append(tmp[:0], k...), 1)
		iter.SeekGE(tmp)

		if len(keys) == count {
			if iter.Next() {
				x := iter.Key()
				nextCursor = string(x[:bytes.IndexByte(x, 0)][1:])
			}
			break
		}
	}
	return
}

func (s *Server) HSet(key string, wait bool, kvs, ids [][]byte) ([][]byte, error) {
	if len(kvs) == 0 {
		return nil, nil
	}

	kh := sha1.Sum([]byte(key))

	var maxID future.Future
	var kk [][]byte
	m := map[string]hashmapData{}
	for i := 0; i < len(kvs); i += 2 {
		k := *(*string)(unsafe.Pointer(&kvs[i]))
		v := kvs[i+1]
		if len(k) == 0 || len(v) == 0 {
			return nil, fmt.Errorf("HSet: member and value can't be null")
		}
		if len(ids) > 0 {
			id := ids[i/2]
			maxID = s2.Convert16BToFuture(id)
			kk = append(kk, id)
		} else {
			maxID = future.Get(s.Channel)
			idx := s2.ConvertFutureTo16B(maxID)
			copy(idx[8:12], kh[:])
			rand.Read(idx[12:13])
			idx[13] = 0 // shard index, not used by now
			idx[14] = byte(s.Channel)<<4 | s2.PairCmdHSet
			idx[15] = 0 // extra
			kk = append(kk, idx[:])
		}
		m[k] = hashmapData{ts: int64(maxID), key: kvs[i], data: v}
	}

	if err := s.DB.Merge(skp(key), hashmapMergerBytes(m), pebble.Sync); err != nil {
		return nil, err
	}
	if wait {
		future.Future(maxID).Wait()
	}
	return kk, nil
}

func (s *Server) HGet(key string, member []byte) (res []byte, ts int64, err error) {
	buf, rd, err := s.DB.Get(skp(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	defer rd.Close()

	if err := hashmapMergerIter(buf, func(d hashmapData) bool {
		if bytes.Equal(d.key, member) {
			future.Future(d.ts).Wait()
			res = d.data
			ts = d.ts
			return false
		}
		return true
	}); err != nil {
		return nil, 0, err
	}
	return
}

func (s *Server) HGetAll(key string, matchValue []byte, inclKey, inclValue, inclTime bool) (res [][]byte, err error) {
	buf, rd, err := s.DB.Get(skp(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer rd.Close()

	var max int64
	if err := hashmapMergerIter(buf, func(d hashmapData) bool {
		if matchValue != nil && !bytes.Equal(matchValue, d.data) {
			return true
		}
		if d.ts > max {
			max = d.ts
		}
		if inclKey {
			res = append(res, d.key)
		}
		if inclValue {
			res = append(res, d.data)
		}
		if inclTime {
			res = append(res, strconv.AppendInt(nil, d.ts/1e6/10*10, 10))
		}
		return true
	}); err != nil {
		return nil, err
	}

	future.Future(max).Wait()
	return
}

func (s *Server) HLen(key string) (count int, err error) {
	buf, rd, err := s.DB.Get(skp(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	defer rd.Close()
	count = int(binary.BigEndian.Uint32(buf[1:]))
	return
}

func (s *Server) hChecksum(key string) (v [20]byte, size int, err error) {
	buf, rd, err := s.DB.Get(skp(key))
	if err == pebble.ErrNotFound {
	} else if err != nil {
		return v, 0, err
	} else {
		defer rd.Close()
	}
	return sha1.Sum(buf), len(buf), nil
}
