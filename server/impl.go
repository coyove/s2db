package server

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"math/big"
	"sort"
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

func (s *Server) implAppend(key string, ids, data [][]byte, opts s2.AppendOptions) ([][]byte, future.Future, error) {
	if key == "" {
		return nil, 0, fmt.Errorf("empty key")
	}
	kh := sha1.Sum([]byte(key))

	var kk [][]byte
	var p []s2.Pair
	var id future.Future

	for i := 0; i < len(data); i++ {
		if len(data[i]) == 0 {
			return nil, 0, fmt.Errorf("can't append empty data")
		}
		if int(opts.DPLen) > len(data[i]) {
			return nil, 0, fmt.Errorf("dpLen exceeds data length")
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

			var idx [16]byte // id(8b) + keyhash(4b) + random(1b) + dplen(1b) + cmd(1b) + extra(1b)
			binary.BigEndian.PutUint64(idx[:], uint64(id))
			copy(idx[8:12], kh[:])
			rand.Read(idx[12:13])
			idx[13] = opts.DPLen
			idx[14] = byte(s.Channel)<<4 | s2.PairCmdAppend
			if opts.NoExpire {
				idx[14] |= s2.PairCmdAppendNoExpire
			}
			idx[15] = 0 // extra

			k := s2.Bytes(idx[:])
			kk = append(kk, k)
			p = append(p, s2.Pair{ID: k, Data: data[i]})
		}
	}

	if err := s.dbAppend(key, p, nil, opts.Defer); err != nil {
		return nil, 0, err
	}

	return kk, id, nil
}

func (s *Server) setMissing(key string, before, after []s2.Pair,
	consolidate, consolidateLeft, consolidateRight bool) error {
	bkPrefix := kkp(key)

	con := func(tx *pebble.Batch) {
		m := map[future.Future]bool{}
		for _, p := range s2.TrimPairsForConsolidation(after, !consolidateLeft, !consolidateRight) {
			if p.Con {
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

	if s.TestFlags.NoSetMissing {
		panic("test: no set missing")
	}

	s.dbAppend(key, missing, func(tx *pebble.Batch) {
		if consolidate {
			con(tx)
		}
	}, true)
	s.Survey.PeerOnMissing.Incr(1)
	return nil
}

func (s *Server) implLookupID(id []byte) (data []byte, key string, err error) {
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

func (s *Server) implRange(key string, start []byte, n int, opts s2.SelectOptions) (data []s2.Pair, err error) {
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

	if opts.Desc {
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

	ns := future.UnixNano()
	for c.Valid() {
		k := bytes.TrimPrefix(c.Key(), bkPrefix)
		p := s2.Pair{
			ID:   s2.Bytes(k),
			Data: s2.Bytes(c.Value()),
		}
		if opts.Raw {
			data = append(data, p)
			goto NEXT
		}

		if v, ok := p.Future().Cookie(); ok {
			if v == consolidatedMark {
				cm[p.Future()] = true
			} else if v == eolMark {
				if opts.Desc {
					break
				}
			} else {
				return nil, fmt.Errorf("invalid mark: %x", v)
			}
		} else {

			if opts.Desc {
				// Desc-ranging may start beyond 'start' cursor, shown by the graph above.
				if bytes.Compare(k, start) <= 0 {
					if opts.LeftOpen && len(data) == 0 && bytes.Equal(start, p.ID) {
					} else {
						data = append(data, p)
					}
				}
			} else {
				if opts.LeftOpen && len(data) == 0 && bytes.Equal(start, p.ID) {
				} else {
					data = append(data, p)
				}
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
			return nil, fmt.Errorf("range timed out")
		}

		moveIter(c, opts.Desc)
	}

	if len(data) > n {
		data = data[:n]
	}

	for i, p := range data {
		cid := p.Future().ToCookie(consolidatedMark)
		if cm[cid] {
			data[i].Con = true
		}
	}

	return
}

func (s *Server) ScanHash(cursor string, count int) (nextCursor string, keys []string) {
	iter := newPrefixIter(s.DB, []byte("h"))
	defer iter.Close()

	for iter.SeekGE(makeHashmapKey(cursor)); iter.Valid(); iter.Next() {
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

func (s *Server) ScanLookupIndex(cursor string, count int) (nextCursor string, keys []string) {
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

	var tmp []byte
	for iter.SeekGE(kkp(cursor)); iter.Valid(); {
		k := iter.Key()
		k = k[:bytes.IndexByte(k, 0)]
		keys = append(keys, string(k[1:]))

		if len(keys) == count+1 {
			nextCursor = keys[count]
			keys = keys[:count]
			break
		}

		tmp = append(append(tmp[:0], k...), 1)
		iter.SeekGE(tmp)
	}

	return
}

// func (s *Server) implHSet(key string, kvs, ids [][]byte) ([][]byte, future.Future, error) {
// 	if key == "" {
// 		return nil, 0, fmt.Errorf("empty key")
// 	}
// 	if len(kvs) == 0 {
// 		return nil, 0, nil
// 	}
//
// 	kh := sha1.Sum([]byte(key))
//
// 	var maxID future.Future
// 	var kk [][]byte
// 	m := map[string]hashmapData{}
// 	for i := 0; i < len(kvs); i += 2 {
// 		k := *(*string)(unsafe.Pointer(&kvs[i]))
// 		v := kvs[i+1]
// 		if len(k) == 0 || len(v) == 0 {
// 			return nil, 0, fmt.Errorf("hashmap: member and value can't be empty")
// 		}
// 		if len(ids) > 0 {
// 			id := ids[i/2]
// 			maxID = s2.Convert16BToFuture(id)
// 			kk = append(kk, id)
// 		} else {
// 			maxID = future.Get(s.Channel)
// 			idx := s2.ConvertFutureTo16B(maxID)
// 			copy(idx[8:12], kh[:])
// 			rand.Read(idx[12:13])
// 			idx[13] = 0 // reserved
// 			idx[14] = byte(s.Channel)<<4 | s2.PairCmdHSet
// 			idx[15] = 0 // extra
// 			kk = append(kk, idx[:])
// 		}
// 		m[k] = hashmapData{ts: int64(maxID), key: kvs[i], data: v}
// 	}
//
// 	if err := s.DB.Merge(makeHashmapKey(key), hashmapMergerBytes(m), pebble.Sync); err != nil {
// 		return nil, 0, err
// 	}
// 	return kk, maxID, nil
// }
//
// func (s *Server) implHGet(key string, member []byte, tsOnly bool) (res []byte, ts int64, err error) {
// 	buf, rd, err := s.DB.Get(makeHashmapKey(key))
// 	if err != nil {
// 		if err == pebble.ErrNotFound {
// 			return nil, 0, nil
// 		}
// 		return nil, 0, err
// 	}
// 	defer rd.Close()
//
// 	if err := hashmapIterBytes(buf, func(d hashmapData) bool {
// 		if bytes.Equal(d.key, member) {
// 			future.Future(d.ts).Wait()
// 			if !tsOnly {
// 				res = d.clone().data
// 			}
// 			ts = d.ts
// 			return false
// 		}
// 		return true
// 	}); err != nil {
// 		return nil, 0, err
// 	}
// 	return
// }
//
// func (s *Server) implHGetAll(key string, matchValue []byte, inclKey, inclValue, inclTime bool) (res [][]byte, err error) {
// 	buf, rd, err := s.DB.Get(makeHashmapKey(key))
// 	if err != nil {
// 		if err == pebble.ErrNotFound {
// 			return nil, nil
// 		}
// 		return nil, err
// 	}
// 	defer rd.Close()
//
// 	var max int64
// 	var mErr error
// 	if err := hashmapIterBytes(buf, func(d hashmapData) bool {
// 		if matched := false; matchValue != nil {
// 			matched, mErr = s2.GlobBytes(matchValue, d.data)
// 			if err != nil {
// 				return true
// 			}
// 			if !matched {
// 				return true
// 			}
// 		}
// 		if d.ts > max {
// 			max = d.ts
// 		}
// 		if inclKey {
// 			res = append(res, s2.Bytes(d.key))
// 		}
// 		if inclTime {
// 			res = append(res, strconv.AppendInt(nil, d.ts/1e6, 10))
// 		}
// 		if inclValue {
// 			res = append(res, s2.Bytes(d.data))
// 		}
// 		return true
// 	}); err != nil {
// 		return nil, err
// 	}
// 	if mErr != nil {
// 		return nil, mErr
// 	}
//
// 	future.Future(max).Wait()
// 	return
// }
//
// func (s *Server) implHLen(key string) (count int, err error) {
// 	buf, rd, err := s.DB.Get(makeHashmapKey(key))
// 	if err != nil {
// 		if err == pebble.ErrNotFound {
// 			return 0, nil
// 		}
// 		return 0, err
// 	}
// 	defer rd.Close()
// 	count = int(binary.BigEndian.Uint32(buf[1:]))
// 	return
// }
//
// func (s *Server) hChecksum(key string) (v [20]byte, size int, err error) {
// 	buf, rd, err := s.DB.Get(makeHashmapKey(key))
// 	if err == pebble.ErrNotFound {
// 		// Pass, calc sha1 empty buffer
// 	} else if err != nil {
// 		return v, 0, err
// 	} else {
// 		defer rd.Close()
// 	}
// 	return sha1.Sum(buf), len(buf), nil
// }

type dbPayload struct {
	key  string
	data []s2.Pair
	ext  func(*pebble.Batch)
	out  chan error
}

func (s *Server) dbAppend(key string, data []s2.Pair, ext func(*pebble.Batch), deferred bool) error {
	x := &dbPayload{
		key:  key,
		data: data,
		ext:  ext,
	}
	if !deferred {
		x.out = make(chan error, 1)
	}
	s.pipeline <- x
	if deferred {
		return nil
	}
	err := <-x.out
	return err
}

func (s *Server) pipelineWorker() {
	var payloads []*dbPayload
	for {
		p, ok := <-s.pipeline
		if !ok {
			break
		}

		payloads = append(payloads[:0], p)
		for len(payloads) < s.Config.PipelineLimit {
			select {
			case p, ok := <-s.pipeline:
				if !ok {
					goto EXIT
				}
				payloads = append(payloads, p)
			default:
				goto EXIT
			}
		}
	EXIT:

		s.Survey.Pipeline.Incr(int64(len(payloads)))
		start := future.UnixNano()

		tx := s.DB.NewBatch()
		for _, p := range payloads {
			bkPrefix := kkp(p.key)
			ck := s2.HashStr128(p.key)

			for _, kv := range p.data {
				if _, ok := s.fillCache.Get(kv.ID); ok {
					continue
				}
				tx.Set(append(bkPrefix, kv.ID...), kv.Data, pebble.Sync)
				s.fillCache.Add(kv.ID, struct{}{})
				s.updateWatermarkCache(ck, kv.ID)
			}

			kh := sha1.Sum([]byte(p.key))
			tx.Set(append(append([]byte("z"), kh[:4]...), p.key...), nil, pebble.Sync)

			if p.ext != nil {
				p.ext(tx)
			}
		}
		err := tx.Commit(pebble.Sync)
		for _, p := range payloads {
			if p.out != nil {
				p.out <- err
			}
		}
		if err != nil {
			logrus.Errorf("pipeline commit: %v", err)
		}
		s.Survey.PipelineLat.Incr((future.UnixNano() - start) / 1e6)
		tx.Close()
	}
}
