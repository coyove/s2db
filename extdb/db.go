package extdb

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/s2pkg"
)

func GetKeyPrefix(key string) (prefix []byte) {
	prefix = append(append(append(make([]byte, 64)[:0], 'l'), key...), 0)
	return
}

func NewPrefixIter(db *pebble.DB, key []byte) *pebble.Iterator {
	return db.NewIter(&pebble.IterOptions{
		LowerBound: key,
		UpperBound: s2pkg.IncBytes(key),
	})
}

func Get(db *pebble.DB, key []byte) ([]byte, error) {
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

func GetInt64(db *pebble.DB, key []byte) (int64, error) {
	buf, rd, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	defer rd.Close()
	if len(buf) != 8 {
		return 0, fmt.Errorf("invalid number bytes (8)")
	}
	return int64(s2pkg.BytesToUint64(buf)), nil
}

func SetInt64(db *pebble.DB, key []byte, vi int64) error {
	return db.Set(key, s2pkg.Uint64ToBytes(uint64(vi)), pebble.Sync)
}
