package ranges

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/s2pkg"
)

func GetZSetRangeKey(key string) ([]byte, []byte, []byte) {
	return GetZSetNameKey(key), []byte("zsetskv_" + key + "\x00"), GetZSetCounterKey(key)
}

func GetZSetNameKey(key string) []byte {
	return []byte("zsetks__" + key + "\x00")
}

func GetZSetCounterKey(key string) []byte {
	return []byte("zsetctr_" + key)
}

func GetShardLogKey(shard int16) []byte {
	return []byte(fmt.Sprintf("log%04x_", shard))
}

func NewPrefixIter(db *pebble.DB, key []byte) *pebble.Iterator {
	return db.NewIter(&pebble.IterOptions{
		LowerBound: key,
		UpperBound: s2pkg.IncBytes(key),
	})
}
