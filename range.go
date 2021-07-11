package main

import (
	"bytes"

	"go.etcd.io/bbolt"
)

type RangeLimit struct {
	Value     string
	Inclusive bool
}

func (z *DB) rangeLex(name string, start, end RangeLimit, delete bool) (pairs []Pair, err error) {
	f := func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset." + name))
		if bk == nil {
			return nil
		}

		startBuf, endBuf := []byte(start.Value), []byte(end.Value)

		if !start.Inclusive {
			startBuf = append(startBuf, 0)
		}

		endFlag := 0
		if !end.Inclusive {
			endFlag = -1
		}

		c := bk.Cursor()
		k, s := c.Seek(startBuf)

		for {
			if len(s) > 0 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) <= endFlag {
				pairs = append(pairs, Pair{string(k), bytesToFloat(s)})
			} else {
				break
			}
			k, s = c.Next()

		}

		if delete {
			return z.deletePair(tx, name, pairs...)
		}
		return nil
	}
	if delete {
		err = z.db.Update(f)
	} else {
		err = z.db.View(f)
	}
	return
}

func (z *DB) rangeScore(name string, start, end RangeLimit, delete bool) (pairs []Pair, err error) {
	f := func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset.score." + name))
		if bk == nil {
			return nil
		}

		startBuf, endBuf := floatToBytes(atof(start.Value)), append(floatToBytes(atof(end.Value)), 0xff)

		if !start.Inclusive {
			startBuf = floatBytesStep(startBuf, 1)
		}
		if !end.Inclusive {
			endBuf = floatBytesStep(endBuf, -1)
		}

		c := bk.Cursor()
		k, s := c.Seek(startBuf)

		for {
			if len(s) > 0 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) <= 0 {
				pairs = append(pairs, Pair{string(s), bytesToFloat(k[:16])})
			} else {
				break
			}
			k, s = c.Next()

		}
		if delete {
			return z.deletePair(tx, name, pairs...)
		}
		return nil
	}
	if delete {
		err = z.db.Update(f)
	} else {
		err = z.db.View(f)
	}
	return
}
