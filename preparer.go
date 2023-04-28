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

	bkPrefix, _ := extdb.GetKeyPrefix(key)

	tx := s.DB.NewBatch()
	defer tx.Close()

	idx := make([]byte, 16) // id(8b) + random(4b) + index(2b) + cmd(2b)
	binary.BigEndian.PutUint64(idx[:], uint64(id))
	rand.Read(idx[8:12])

	var kk [][]byte
	for i := 0; i < len(data); i++ {
		binary.BigEndian.PutUint16(idx[12:], uint16(i))
		idx[15] = 1

		kk = append(kk, s2pkg.Bytes(idx))

		if err := tx.Set(append(bkPrefix, idx...), data[i], pebble.Sync); err != nil {
			return nil, err
		}

		if err := tx.Set(append([]byte("i"), idx...), []byte(key), pebble.Sync); err != nil {
			return nil, err
		}
	}

	if ttlSec > 0 {
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

	if err := tx.Commit(pebble.Sync); err != nil {
		return nil, err
	}
	return kk, nil
}

func (s *Server) ExpireBefore(key string, unixSec int64) error {
	_, err, _ := s.expireGroup.Do(key, func() (any, error) {
		bkPrefix, bkTombstone := extdb.GetKeyPrefix(key)

		if err := extdb.SetInt64(s.DB, bkTombstone, unixSec); err != nil {
			return nil, err
		}

		idx := make([]byte, 16) // id(8b) + random(4b) + cmd(2b) + index(2b)
		binary.BigEndian.PutUint64(idx[:], uint64(unixSec*1e9))
		return nil, s.DB.DeleteRange(bkPrefix, append(bkPrefix, idx...), pebble.Sync)
	})
	return err
}

func (s *Server) GetTombstone(key string) int64 {
	_, bkTombstone := extdb.GetKeyPrefix(key)
	localTombstone, _ := extdb.GetInt64(s.DB, bkTombstone)
	return localTombstone
}

func (s *Server) setMissing(key string, kvs []s2pkg.Pair, consolidate bool) {
	if len(kvs) == 0 {
		s.Survey.PeerOnOK.Incr(1)
		return
	}
	go func(start time.Time) {
		mu := &s.fillLocks[s2pkg.HashStr(key)&0xffff]
		mu.Lock()
		defer mu.Unlock()

		if err := func() error {
			tx := s.DB.NewBatch()
			defer tx.Close()

			bkPrefix, _ := extdb.GetKeyPrefix(key)

			c := 0
			for _, kv := range kvs {
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
				c += 2
				s.fillCache.AddSimple(cacheKey, nil)
			}

			if consolidate {
				kvs := s2pkg.TrimPairs(kvs)

				var cm [][16]byte
				for i := 0; i < len(kvs); i++ {
					cid := kvs[i].Future().ToCookie(consolidatedMark)
					idx := s2pkg.ConvertFutureTo16B(cid)
					if len(cm) == 0 || cm[len(cm)-1] != idx {
						cm = append(cm, idx)
					}
				}
				for i := 0; i < len(cm); i++ {
					key := append(bkPrefix, cm[i][:]...)
					if i == 0 {
						if old, _ := extdb.Get(s.DB, key); old == nil {
							if err := tx.Set(key, make([]byte, 16), pebble.Sync); err != nil {
								return err
							}
						}
					} else {
						if err := tx.Set(key, cm[i-1][:], pebble.Sync); err != nil {
							return err
						}
					}
					c++
				}
			}

			if c == 0 {
				return nil
			}
			s.Survey.PeerOnMissingN.Incr(int64(c))
			return tx.Commit(pebble.Sync)
		}(); err != nil {
			logrus.Errorf("setMissing: %v", err)
		}

		s.Survey.PeerOnMissing.Incr(time.Since(start).Milliseconds())
	}(time.Now())
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
			bkPrefix, _ := extdb.GetKeyPrefix(*(*string)(unsafe.Pointer(&key)))
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

	bkPrefix, _ := extdb.GetKeyPrefix(key)

	c := s.DB.NewIter(&pebble.IterOptions{
		LowerBound: bkPrefix,
		UpperBound: s2pkg.IncBytes(bkPrefix),
	})
	defer c.Close()

	// for c.First(); c.Valid(); c.Next() {
	// 	fmt.Println(c.Key())
	// }

	start = append(bkPrefix, start...)
	if desc {
		c.SeekLT(s2pkg.IncBytesInplace(start))
	} else {
		c.SeekGE(start)
	}

	chain := map[future.Future]future.Future{}
	var maxFuture future.Future

	for c.Valid() && len(data) < n {
		k := bytes.TrimPrefix(c.Key(), bkPrefix)
		p := s2pkg.Pair{
			ID:   s2pkg.Bytes(k),
			Data: s2pkg.Bytes(c.Value()),
		}
		if v, ok := p.Future().Cookie(); ok && v == consolidatedMark {
			chain[p.Future()] = future.Future(binary.BigEndian.Uint64(p.Data))
			if p.Future() > maxFuture {
				maxFuture = p.Future()
			}
		} else {
			if len(data) == 0 || len(data) == n-1 {
				cid := p.Future().ToCookie(consolidatedMark)
				idx := s2pkg.ConvertFutureTo16B(cid)
				v, _ := extdb.Get(s.DB, append(bkPrefix, idx[:]...))
				if v != nil {
					chain[cid] = future.Future(binary.BigEndian.Uint64(v))
					if cid > maxFuture {
						maxFuture = cid
					}
				}
			}
			data = append(data, p)
		}
		if desc {
			c.Prev()
		} else {
			c.Next()
		}
	}

	//fmt.Println(s.ln.Addr(), data, chain)

	// Verify that all 'futures' in 'chain' form a linked list, where newer 'futures'
	// point to older 'futures'. The oldest 'future' may point to null.
	for c, ok := maxFuture, false; ; {
		if c, ok = chain[c]; c == 0 {
			if !ok {
				return
			}
			break
		}
	}

	for i, p := range data {
		cid := p.Future().ToCookie(consolidatedMark)
		if _, ok := chain[cid]; ok {
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
