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

const consolidatedMark = 1

const maxCursor = "\x7f\xff\xff\xff\xcd\x0d\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00"

func (s *Server) Append(key string, data [][]byte, ttlSec int64, wait bool) ([][]byte, error) {
	if len(data) >= 65536 {
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

	bkPrefix := extdb.GetKeyPrefix(key)

	tx := s.DB.NewBatch()
	defer tx.Close()

	idx := make([]byte, 16) // id(8b) + random(4b) + index(2b) + cmd(2b)
	binary.BigEndian.PutUint64(idx[:], uint64(id))
	rand.Read(idx[8:12])

	var kk [][]byte
	for i := 0; i < len(data); i++ {
		binary.BigEndian.PutUint16(idx[12:], uint16(i))
		idx[14] = byte(s.Channel)<<4 | s2pkg.PairCmdAppend

		kk = append(kk, s2pkg.Bytes(idx))

		if err := tx.Set(append(bkPrefix, idx...), data[i], pebble.Sync); err != nil {
			return nil, err
		}

		if err := tx.Set(append([]byte("i"), idx...), []byte(key), pebble.Sync); err != nil {
			return nil, err
		}
	}

	if ttlSec > 0 {
		_, loaded := s.ttlOnce.LoadOrStore(key, true)
		if !loaded {
			defer s.ttlOnce.Delete(key)

			idx := s2pkg.ConvertFutureTo16B(future.Future(future.UnixNano() - ttlSec*1e9))
			iter := s.DB.NewIter(&pebble.IterOptions{
				LowerBound: bkPrefix,
				UpperBound: append(bkPrefix, idx[:]...),
			})
			defer iter.Close()

			count := 0
			for iter.First(); iter.Valid() && count < *ttlEvictLimit; iter.Next() {
				count++
				if err := tx.Delete(iter.Key(), pebble.Sync); err != nil {
					return nil, err
				}
				idx := bytes.TrimPrefix(iter.Key(), bkPrefix)
				if v, _ := s2pkg.Convert16BToFuture(idx).Cookie(); v == consolidatedMark {
					continue
				}
				if err := tx.Delete(append([]byte("i"), idx...), pebble.Sync); err != nil {
					return nil, err
				}
			}
			s.Survey.AppendExpire.Incr(1)
		}
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
		for _, kv := range s2pkg.TrimPairsForConsolidation(after) {
			cid := kv.Future().ToCookie(consolidatedMark)
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
		tx := s.DB.NewBatch()
		defer tx.Close()

		for _, kv := range missing {
			cacheKey := string(kv.ID)
			if _, ok := s.fillCache.GetSimple(cacheKey); ok {
				continue
			}
			if err := tx.Set(append(bkPrefix, kv.ID...), kv.Data, pebble.Sync); err != nil {
				return err
			}
			if err := tx.Set(append([]byte("i"), kv.ID...), []byte(key), pebble.Sync); err != nil {
				return err
			}
			s.fillCache.AddSimple(cacheKey, nil)
		}

		if consolidate {
			con(tx)
		}
		if tx.Count() == 0 {
			return nil
		}
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

func (s *Server) Range(key string, start []byte, n int) (data []s2pkg.Pair, err error) {
	desc := false
	if n < 0 {
		desc, n = true, -n
	}

	bkPrefix := extdb.GetKeyPrefix(key)

	c := s.DB.NewIter(&pebble.IterOptions{
		LowerBound: bkPrefix,
		UpperBound: s2pkg.IncBytes(bkPrefix),
	})
	defer c.Close()

	// for c.First(); c.Valid(); c.Next() {
	// 	fmt.Println(c.Key())
	// }

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

	for c.Valid() {
		k := bytes.TrimPrefix(c.Key(), bkPrefix)
		p := s2pkg.Pair{
			ID:   s2pkg.Bytes(k),
			Data: s2pkg.Bytes(c.Value()),
		}
		if v, ok := p.Future().Cookie(); ok && v == consolidatedMark {
			cm[p.Future()] = true
		} else {
			if desc {
				// Desc-ranging may start above 'start' cursor, shown by the graph above.
				if bytes.Compare(k, start) <= 0 {
					data = append(data, p)
				}
			} else {
				data = append(data, p)
			}
		}

		if len(data) >= n+2 {
			// To exit the loop, we either exhausted the cursor, or collected enough
			// Pairs where last two Pairs belong to different blocks.
			d0, d1 := data[len(data)-1], data[len(data)-2]
			if d0.UnixNano()/future.Block != d1.UnixNano()/future.Block {
				break
			}
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
			p = append(p[:i], p[i+1:]...)
		}
	}
	return p
}

func iabs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
