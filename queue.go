package main

import (
	"encoding/binary"

	"go.etcd.io/bbolt"
)

func (s *Server) qLength(name string) (int64, error) {
	var count int64
	err := s.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("q." + name))
		if bk == nil {
			return nil
		}
		c := bk.Cursor()
		firstKey, _ := c.First()
		lastKey, _ := c.Last()
		if len(lastKey) == 8 && len(firstKey) == 8 {
			first := binary.BigEndian.Uint64(firstKey)
			last := binary.BigEndian.Uint64(lastKey)
			count = int64(last - first + 1)
		}
		return nil
	})
	return count, err
}
