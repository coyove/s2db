package ranges

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/s2pkg"
)

func GetZSetRangeKey(key string) ([]byte, []byte, []byte) {
	return GetZSetNameKey(key), []byte("zsetskv_" + key + "\x00"), GetZSetCounterKey(key)
}

func GetSetRangeKey(key string) ([]byte, []byte) {
	return []byte("zpset___" + key + "\x00"), GetSetCounterKey(key)
}

func GetZSetNameKey(key string) []byte {
	return []byte("zsetks__" + key + "\x00")
}

func GetZSetCounterKey(key string) []byte {
	return []byte("zsetctr_" + key)
}

func GetSetCounterKey(key string) []byte {
	return []byte("zpsetctr" + key)
}

func GetShardLogKey(shard int16) []byte {
	return []byte(fmt.Sprintf("log%04x_", shard))
}

func GetKVKey(key string) []byte {
	return []byte("zkv_____" + key)
}

func GetKey(key string) (prefix, tombstone []byte) {
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
