package extdb

import (
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

func GetKeyNumber(db Storage, key []byte) (float64, uint64, bool, error) {
	buf, rd, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	defer rd.Close()
	if len(buf) != 8 {
		return 0, 0, false, fmt.Errorf("invalid number bytes (8)")
	}
	return s2pkg.BytesToFloat(buf), s2pkg.BytesToUint64(buf), true, nil
}

func IncrKey(db Storage, key []byte, v int64) (int64, error) {
	buf, rd, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return v, db.Set(key, s2pkg.Uint64ToBytes(uint64(v)), pebble.Sync)
		}
		return 0, err
	}
	old := int64(s2pkg.BytesToUint64(buf))
	rd.Close()
	old += v
	if old == 0 {
		return 0, db.Delete(key, pebble.Sync)
	}
	return old, db.Set(key, s2pkg.Uint64ToBytes(uint64(old)), pebble.Sync)
}
