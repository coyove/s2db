package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/coyove/s2db/internal"
	"github.com/coyove/s2db/redisproto"
	"go.etcd.io/bbolt"
)

func qLenImpl(bk *bbolt.Bucket) (int64, int64, int64) {
	c := bk.Cursor()
	firstKey, _ := c.First()
	lastKey, _ := c.Last()
	if len(lastKey) == 16 && len(firstKey) == 16 {
		first := binary.BigEndian.Uint64(firstKey)
		last := binary.BigEndian.Uint64(lastKey)
		if first <= last {
			return int64(first), int64(last), int64(last - first + 1)
		}
		panic(-2)
	}
	return 0, 0, 0
}

func (s *Server) QLength(key string) (int64, error) {
	var count int64
	err := s.pick(key).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("q." + key))
		if bk == nil {
			return nil
		}
		_, _, count = qLenImpl(bk)
		return nil
	})
	return count, err
}

func (s *Server) QScan(key string, start, n int64, flags redisproto.Flags) ([][]byte, error) {
	var data [][]byte
	desc := false
	if n < 0 {
		n = -n
		desc = true
	}
	if n > int64(internal.RangeHardLimit) {
		n = int64(internal.RangeHardLimit)
	}
	err := s.pick(key).View(func(tx *bbolt.Tx) error {
		err := func() error {
			bk := tx.Bucket([]byte("q." + key))
			if bk == nil {
				return nil
			}
			first, last, count := qLenImpl(bk)
			if count == 0 {
				return nil
			}

			if start <= 0 {
				start = last + start
			} else {
				start = first + start - 1
			}

			if start < first || start > last {
				return nil
			}

			c := bk.Cursor()
			startBuf := internal.Uint64ToBytes(uint64(start))
			k, v := c.Seek(startBuf)
			if !bytes.HasPrefix(k, startBuf) {
				return fmt.Errorf("fatal: missing key")
			}
			for len(data) < int(n) && len(k) == 16 {
				data = append(data, append([]byte{}, v...))
				if flags.WITHINDEXES {
					data = append(data, []byte(strconv.FormatUint(binary.BigEndian.Uint64(k[:8]), 10)))
				}
				if desc {
					k, v = c.Prev()
				} else {
					k, v = c.Next()
				}
			}
			return nil
		}()
		if err == nil {
			s.addCache(key, flags.HashCode(), data)
		}
		return err
	})
	return data, err
}

func (s *Server) QGet(key string, idx int64) ([]byte, error) {
	var data []byte
	err := s.pick(key).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("q." + key))
		if bk == nil {
			return nil
		}
		first, last, count := qLenImpl(bk)
		if count == 0 {
			return nil
		}

		if idx <= 0 {
			idx += last
		}
		if idx < first || idx > last {
			return nil
		}

		c := bk.Cursor()
		startBuf := internal.Uint64ToBytes(uint64(idx))
		k, v := c.Seek(startBuf)
		if !bytes.HasPrefix(k, startBuf) {
			return fmt.Errorf("fatal: missing key")
		}
		data = v
		return nil
	})
	return data, err
}

func (s *Server) QHead(key string) (int64, error) {
	var data int64
	err := s.pick(key).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("q." + key))
		if bk == nil {
			return nil
		}
		data, _, _ = qLenImpl(bk)
		return nil
	})
	return data, err
}
