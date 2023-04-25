package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
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

func (s *Server) Append(key string, data [][]byte, wait bool) ([][]byte, error) {
	id := future.Get(s.Channel)
	if wait {
		defer id.Wait()
	}
	return s.runAppend(id, key, data)
}

func (s *Server) runAppend(id future.Future, key string, data [][]byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) >= 65536 {
		return nil, fmt.Errorf("too many elements to append")
	}

	bkPrefix, bkTombstone := extdb.GetKeyPrefix(key)

	tombstone, err := extdb.GetInt64(s.DB, bkTombstone)
	if err != nil {
		return nil, err
	}
	if tombstone > int64(id) {
		return nil, nil
	}

	tx := s.DB.NewBatch()
	defer tx.Close()

	idx := make([]byte, 16) // id(8b) + random(4b) + cmd(2b) + index(2b)
	binary.BigEndian.PutUint64(idx[:], uint64(id))
	rand.Read(idx[8:12])
	idx[12] = 1 // 'append' command code

	var kk [][]byte
	for i, p := range data {
		binary.BigEndian.PutUint16(idx[14:], uint16(i))
		kk = append(kk, s2pkg.Bytes(idx))

		if err := tx.Set(append(bkPrefix, idx...), p, pebble.Sync); err != nil {
			return nil, err
		}

		if err := tx.Set(append([]byte("i"), idx...), []byte(key), pebble.Sync); err != nil {
			return nil, err
		}
	}

	return kk, tx.Commit(pebble.Sync)
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
				var idx [16]byte
				for i := 0; i < len(kvs); i++ {
					cid := kvs[i].Future().ToCookie(consolidatedMark)
					binary.BigEndian.PutUint64(idx[:], uint64(cid))
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
	idx := make([]byte, 16)
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
			data = append(data, v)

			mark := future.Future(binary.BigEndian.Uint64(id)).ToCookie(consolidatedMark)
			binary.BigEndian.PutUint64(idx, uint64(mark))
			if con, _ := extdb.Get(s.DB, append(bkPrefix, idx...)); con != nil {
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

	m := map[future.Future]future.Future{}
	var maxFuture future.Future

	for c.Valid() && len(data) < n {
		k := bytes.TrimPrefix(c.Key(), bkPrefix)
		p := s2pkg.Pair{
			ID:   s2pkg.Bytes(k),
			Data: s2pkg.Bytes(c.Value()),
		}
		if v, ok := p.Future().Cookie(); ok && v == consolidatedMark {
			m[p.Future()] = future.Future(binary.BigEndian.Uint64(p.Data))
			if p.Future() > maxFuture {
				maxFuture = p.Future()
			}
		} else {
			data = append(data, p)
		}
		if desc {
			c.Prev()
		} else {
			c.Next()
		}
	}

	idx := make([]byte, 16)
	for i, p := range data {
		cid := p.Future().ToCookie(consolidatedMark)
		if _, ok := m[cid]; ok {
			data[i].C = true
			continue
		}
		binary.BigEndian.PutUint64(idx, uint64(cid))
		k := append(bkPrefix, idx...)
		if c.SeekGE(k); bytes.Equal(c.Key(), k) {
			m[cid] = future.Future(binary.BigEndian.Uint64(c.Value()))
			data[i].C = true
			if cid > maxFuture {
				maxFuture = cid
			}
		}
	}

	for c, ok := maxFuture, false; ; {
		if c, ok = m[c]; c == 0 {
			if !ok {
				for i := range data {
					data[i].C = false
				}
			}
			break
		}
	}
	return
}

func hexEncode(k []byte) []byte {
	k0 := make([]byte, len(k)*2)
	hex.Encode(k0, k)
	return k0
}

func hexDecode(k []byte) []byte {
	k0 := make([]byte, len(k)/2)
	hex.Decode(k0, k)
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
