package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/extdb"
	"github.com/coyove/s2db/s2pkg"
	"github.com/coyove/sdss/future"
	"github.com/sirupsen/logrus"
)

const (
	consolidatedMark = 1
	eolMark          = 2
	maxCursor        = "\x7f\xff\xff\xff\xcd\x0d\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00"
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

func (s *Server) getHLL(key string) (s2pkg.HyperLogLog, s2pkg.HyperLogLog, error) {
	v, err := extdb.Get(s.DB, append([]byte("H"), key...))
	if err != nil {
		return nil, nil, err
	}
	switch len(v) {
	case s2pkg.HLLSize * 2:
		return v[:s2pkg.HLLSize], v[s2pkg.HLLSize:], nil
	case s2pkg.HLLSize:
		return v, nil, nil
	case 0:
		return nil, nil, nil
	}
	return nil, nil, fmt.Errorf("invalid HLL size %d of %q", len(v), key)
}

func (s *Server) setHLL(tx *pebble.Batch, key string, add, del s2pkg.HyperLogLog) error {
	return tx.Set(append([]byte("H"), key...), append(add, del...), pebble.Sync)
}

func (s *Server) deleteElement(tx *pebble.Batch, bkPrefix, key []byte, hllDel s2pkg.HyperLogLog) error {
	if err := tx.Delete(key, pebble.Sync); err != nil {
		return err
	}

	idx := bytes.TrimPrefix(key, bkPrefix)
	hllDel.Add(uint32(s2pkg.HashBytes(idx)))

	if _, ok := s2pkg.Convert16BToFuture(idx).Cookie(); ok {
		return nil
	}
	return tx.Delete(append([]byte("i"), idx...), pebble.Sync)
}

func (s *Server) Append(key string, data [][]byte, ttlSec int64, wait bool) ([][]byte, error) {
	if len(data) > 0xffff {
		return nil, fmt.Errorf("too many elements to append")
	}
	if key == "" {
		return nil, fmt.Errorf("append to null key")
	}

	id := future.Get(s.Channel)
	if testFlag {
		x, _ := rand.Int(rand.Reader, big.NewInt(1<<32))
		if v, _ := testDedup.LoadOrStore(id, x.Int64()); v != x.Int64() {
			panic("fatal: duplicated id")
		}
	}
	if wait {
		defer id.Wait()
	}

	hllAdd, hllDel, err := s.getHLL(key)
	if err != nil {
		return nil, err
	}

	bkPrefix := extdb.GetKeyPrefix(key)
	ck := s2pkg.HashStr128(key)

	tx := s.DB.NewBatch()
	defer tx.Close()

	var idx [16]byte // id(8b) + random(3b) + index(2b) + shard(1b) + cmd(1b) + extra(1b)
	binary.BigEndian.PutUint64(idx[:], uint64(id))
	rand.Read(idx[8:11])

	var kk [][]byte
	for i := 0; i < len(data); i++ {
		if len(data[i]) == 0 {
			return nil, fmt.Errorf("can't append null data")
		}

		binary.BigEndian.PutUint16(idx[11:], uint16(i))
		idx[13] = 0 // shard index, not used by now
		idx[14] = byte(s.Channel)<<4 | s2pkg.PairCmdAppend

		kk = append(kk, idx[:])

		if err := tx.Set(append(bkPrefix, idx[:]...), data[i], pebble.Sync); err != nil {
			return nil, err
		}

		if err := tx.Set(append([]byte("i"), idx[:]...), []byte(key), pebble.Sync); err != nil {
			return nil, err
		}

		s.updateWatermarkCache(ck, idx[:])
		hllAdd.Add(uint32(s2pkg.HashBytes(idx[:])))
	}

	if ttlSec > 0 {
		m := &s.ttlOnce[s2pkg.HashStr(key)%lockShards]
		_, loaded := m.LoadOrStore(key, true)
		if !loaded {
			defer m.Delete(key)

			f := future.Future(future.UnixNano()/1e9*1e9 - ttlSec*1e9)
			idx := s2pkg.ConvertFutureTo16B(f)
			iter := s.DB.NewIter(&pebble.IterOptions{
				LowerBound: bkPrefix,
				UpperBound: append(bkPrefix, idx[:]...),
			})
			defer iter.Close()

			count := 0
			for iter.First(); iter.Valid() && count < *ttlEvictLimit; iter.Next() {
				if err := s.deleteElement(tx, bkPrefix, iter.Key(), hllDel); err != nil {
					return nil, err
				}
				count++
			}
			idx = s2pkg.ConvertFutureTo16B(f.ToCookie(eolMark))
			if err := tx.Set(append(bkPrefix, idx[:]...), nil, pebble.Sync); err != nil {
				return nil, err
			}
			s.Survey.AppendExpire.Incr(1)
		}
	}

	if err := s.setHLL(tx, key, hllAdd, hllDel); err != nil {
		return nil, err
	}
	if err := tx.Commit(pebble.Sync); err != nil {
		return nil, err
	}
	return kk, nil
}

func (s *Server) setMissing(key string, before, after []s2pkg.Pair, consolidate bool) {
	bkPrefix := extdb.GetKeyPrefix(key)

	con := func(tx *pebble.Batch) {
		m := map[future.Future]bool{}
		for _, p := range s2pkg.TrimPairsForConsolidation(after) {
			if p.C {
				continue
			}
			cid := p.Future().ToCookie(consolidatedMark)
			if m[cid] {
				continue
			}
			m[cid] = true
			idx := s2pkg.ConvertFutureTo16B(cid)
			tx.Set(append(bkPrefix, idx[:]...), nil, pebble.Sync)
		}
	}

	before = sortPairs(before, true)
	var missing []s2pkg.Pair
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
				}
			}
		}
		s.Survey.PeerOnOK.Incr(1)
		return
	}

	if s.test.NoSetMissing {
		panic("test: no set missing")
	}

	start := time.Now()

	if err := func() error {
		add, del, err := s.getHLL(key)
		if err != nil {
			return err
		}

		ck := s2pkg.HashStr128(key)
		tx := s.DB.NewBatch()
		defer tx.Close()

		for _, kv := range missing {
			if _, ok := s.fillCache.Get(kv.ID); ok {
				continue
			}
			if err := tx.Set(append(bkPrefix, kv.ID...), kv.Data, pebble.Sync); err != nil {
				return err
			}
			if err := tx.Set(append([]byte("i"), kv.ID...), []byte(key), pebble.Sync); err != nil {
				return err
			}
			s.fillCache.Add(kv.ID, struct{}{})
			s.updateWatermarkCache(ck, kv.ID)
			add.Add(uint32(s2pkg.HashBytes(kv.ID)))
		}

		if consolidate {
			con(tx)
		}

		s.setHLL(tx, key, add, del)
		s.Survey.PeerOnMissingN.Incr(int64(tx.Count()))
		return tx.Commit(pebble.Sync)
	}(); err != nil {
		logrus.Errorf("setMissing: %v", err)
	}

	s.Survey.PeerOnMissing.Incr(time.Since(start).Milliseconds())
}

func (s *Server) MGet(ids [][]byte) (data [][]byte, consolidated bool, err error) {
	if len(ids) == 0 {
		return nil, true, nil
	}

	var k []byte
	score := 0
	for _, id := range ids {
		k = append(append(k[:0], 'i'), id...)
		key, err := extdb.Get(s.DB, k)
		if err != nil {
			return nil, false, err
		}
		if len(key) == 0 {
			data = append(data, nil)
		} else {
			bkPrefix := extdb.GetKeyPrefix(*(*string)(unsafe.Pointer(&key)))
			v, err := extdb.Get(s.DB, append(bkPrefix, id...))
			if err != nil {
				return nil, false, err
			}
			if v == nil {
				return nil, false, fmt.Errorf("get %q.%s: not found", key, hexEncode(id))
			}
			data = append(data, v)

			mark := s2pkg.Convert16BToFuture(id).ToCookie(consolidatedMark)
			idx := s2pkg.ConvertFutureTo16B(mark)
			if con, _ := extdb.Get(s.DB, append(bkPrefix, idx[:]...)); con != nil {
				score++
			}
		}
	}
	consolidated = score == len(ids)
	return
}

const (
	RangeDesc     = 1
	RangeDistinct = 2
	RangeAll      = 4
)

func (s *Server) Range(key string, start []byte, n int, flag int) (data []s2pkg.Pair, timedout bool, err error) {
	bkPrefix := extdb.GetKeyPrefix(key)

	c := extdb.NewPrefixIter(s.DB, bkPrefix)
	defer c.Close()

	// for c.First(); c.Valid(); c.Next() {
	// 	fmt.Println(c.Key())
	// }

	c.Last()
	s.wmCache.Update16(s2pkg.HashStr128(key), func(old [16]byte) (new [16]byte) {
		if c.Valid() && bytes.Compare(c.Key(), old[:]) > 0 {
			copy(new[:], c.Key())
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

		cm := s2pkg.Convert16BToFuture(start).ToCookie(consolidatedMark)
		buf := s2pkg.ConvertFutureTo16B(cm)
		c.SeekLT(append(bkPrefix, s2pkg.IncBytesInplace(buf[:])...))
		// fmt.Println(start, buf, cm, s2pkg.Convert16BToFuture(start))
	} else {
		c.SeekGE(append(bkPrefix, start...))
	}

	cm := map[future.Future]bool{}
	dedup := map[string]bool{}
	var dedupTx *pebble.Batch
	var hllAdd, hllDel s2pkg.HyperLogLog

	if flag&RangeDistinct > 0 {
		m := &s.delOnce[s2pkg.HashStr(key)%lockShards]
		if _, loaded := m.LoadOrStore(key, true); !loaded {
			dedupTx = s.DB.NewBatch()
			defer dedupTx.Close()
			defer m.Delete(key)

			hllAdd, hllDel, err = s.getHLL(key)
			if err != nil {
				return nil, false, err
			}
		}
	}

	ns := future.UnixNano()
	rangeAll := flag&RangeAll > 0
	for c.Valid() {
		k := bytes.TrimPrefix(c.Key(), bkPrefix)
		p := s2pkg.Pair{
			ID:   s2pkg.Bytes(k),
			Data: s2pkg.Bytes(c.Value()),
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
			if dedupTx != nil && dedup[p.DataStrRef()] {
				s.deleteElement(dedupTx, bkPrefix, c.Key(), hllDel)
				goto NEXT
			}

			if desc {
				// Desc-ranging may start beyond 'start' cursor, shown by the graph above.
				if bytes.Compare(k, start) <= 0 {
					data = append(data, p)
					dedup[p.DataStrRef()] = true
				}
			} else {
				data = append(data, p)
				dedup[p.DataStrRef()] = true
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

		if future.UnixNano()-ns > int64(s.ServerConfig.TimeoutRange)*1e6 {
			// Iterating timed out
			timedout = true
			break
		}

		if dedupTx != nil && dedupTx.Count() > uint32(s.ServerConfig.DistinctLimit) {
			// Too many deletions in one request.
			timedout = true
			break
		}

		if desc {
			c.Prev()
		} else {
			c.Next()
		}
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

func (s *Server) Scan(cursor string, count int) (keys []string, nextCursor string) {
	iter := s.DB.NewIter(&pebble.IterOptions{
		LowerBound: []byte("l"),
		UpperBound: []byte("m"),
	})
	defer iter.Close()

	if count > 65536 {
		count = 65536
	}

	cPrefix := extdb.GetKeyPrefix(cursor)
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

func hexEncode(k []byte) []byte {
	_ = k[15]
	k0 := make([]byte, 32)
	hex.Encode(k0, k)
	return k0
}

func hexDecode(k []byte) []byte {
	_ = k[31]
	k0 := make([]byte, 16)
	if len(k) == 33 && k[16] == '_' {
		hex.Decode(k0[:8], k[:16])
		hex.Decode(k0[8:], k[17:])
	} else {
		hex.Decode(k0, k)
	}
	return k0
}

func sortPairs(p []s2pkg.Pair, asc bool) []s2pkg.Pair {
	sort.Slice(p, func(i, j int) bool {
		return p[i].Less(p[j]) == asc
	})

	for i := len(p) - 1; i > 0; i-- {
		if p[i].Equal(p[i-1]) {
			p[i-1].C = p[i-1].C || p[i].C // inherit the consolidation mark if any
			p = append(p[:i], p[i+1:]...)
		}
	}
	return p
}

func distinctPairsData(p []s2pkg.Pair) []s2pkg.Pair {
	m := map[string]bool{}
	for i := 0; i < len(p); {
		if m[p[i].DataStrRef()] {
			p = append(p[:i], p[i+1:]...)
		} else {
			m[p[i].DataStrRef()] = true
			i++
		}
	}
	return p
}

func orFlag(v int, c bool, f int) int {
	if c {
		v |= f
	}
	return v
}
