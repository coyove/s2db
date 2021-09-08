package main

import (
	"bytes"
	"encoding/binary"
	"fmt"

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

func (s *Server) qLength(name string) (int64, error) {
	var count int64
	err := s.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("q." + name))
		if bk == nil {
			return nil
		}
		_, _, count = qLenImpl(bk)
		return nil
	})
	return count, err
}

func (s *Server) qScan(name string, start, n int64) ([][]byte, error) {
	var data [][]byte
	desc := false
	if n < 0 {
		n = -n
		desc = true
	}
	if n > int64(HardLimit) {
		n = int64(HardLimit)
	}
	err := s.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("q." + name))
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
		startBuf := intToBytes(uint64(start))
		k, v := c.Seek(startBuf)
		if !bytes.HasPrefix(k, startBuf) {
			return fmt.Errorf("fatal: missing key")
		}
		for len(data) < int(n) && len(k) == 16 {
			data = append(data, v)
			if desc {
				k, v = c.Prev()
			} else {
				k, v = c.Next()
			}
		}
		return nil
	})
	return data, err
}

func (s *Server) qGet(name string, idx int64) ([]byte, error) {
	var data []byte
	err := s.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("q." + name))
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
		startBuf := intToBytes(uint64(idx))
		k, v := c.Seek(startBuf)
		if !bytes.HasPrefix(k, startBuf) {
			return fmt.Errorf("fatal: missing key")
		}
		data = v
		return nil
	})
	return data, err
}
