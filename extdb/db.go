package extdb

import (
	"bytes"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/s2pkg"
)

type Storage interface {
	Get([]byte) ([]byte, io.Closer, error)
	Set(key, value []byte, opts *pebble.WriteOptions) error
	Delete(key []byte, _ *pebble.WriteOptions) error
	DeleteRange(start, end []byte, _ *pebble.WriteOptions) error
	NewIter(*pebble.IterOptions) *pebble.Iterator
}

type LogTx struct {
	OutLogtail *uint64
	InLogtail  *uint64
	LogPrefix  []byte
	Storage
}

func GetKeyPrefix(key string) (prefix, tombstone []byte) {
	prefix = append(append(append(make([]byte, 64)[:0], 'l'), key...), 0)
	tombstone = append(append(make([]byte, 64)[:0], 'T'), key...)
	return
}

func NewPrefixIter(db *pebble.DB, key []byte) *pebble.Iterator {
	return db.NewIter(&pebble.IterOptions{
		LowerBound: key,
		UpperBound: s2pkg.IncBytes(key),
	})
}

func CursorGetKey(c *pebble.Iterator, key []byte) ([]byte, bool) {
	if c.SeekGE(key) && bytes.Equal(key, c.Key()) {
		return c.Value(), true
	}
	return nil, false
}

func CursorGetMaxKeyWithPrefix(c *pebble.Iterator, prefix []byte) ([]byte, []byte, bool) {
	if c.SeekLT(s2pkg.IncBytes(prefix)) && bytes.HasPrefix(c.Key(), prefix) {
		return c.Key(), c.Value(), true
	}
	return nil, nil, false
}

func GetKey(db Storage, key []byte) ([]byte, error) {
	buf, rd, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer rd.Close()
	return s2pkg.Bytes(buf), nil
}

func GetKeyFunc(db Storage, key []byte, f func([]byte) error) error {
	buf, rd, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil
		}
		return err
	}
	defer rd.Close()
	return f(buf)
}

func GetKeyNumber(db Storage, key []byte) (float64, int64, error) {
	buf, rd, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	defer rd.Close()
	if len(buf) != 8 {
		return 0, 0, fmt.Errorf("invalid number bytes (8)")
	}
	return s2pkg.BytesToFloat(buf), int64(s2pkg.BytesToUint64(buf)), nil
}

func SetKeyNumber(db Storage, key []byte, vf *float64, vi int64) error {
	if vf != nil {
		return db.Set(key, s2pkg.FloatToBytes(*vf), pebble.Sync)
	}
	return db.Set(key, s2pkg.Uint64ToBytes(uint64(vi)), pebble.Sync)
}
