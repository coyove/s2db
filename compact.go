package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) compactShard(shard int) error {
	log := log.WithField("shard", strconv.Itoa(shard))

	x := &s.db[shard]
	path := x.DB.Path()
	os.Remove(path + ".compact")
	compactDB, err := bbolt.Open(path+".compact", 0666, bboltOptions)
	if err != nil {
		return err
	}

	if err := bbolt.Compact(compactDB, x.DB, int64(s.CompactTxSize)); err != nil {
		compactDB.Close()
		return err
	}

	log.Info("STAGE 1: point-in-time compaction finished, size: ", compactDB.Size())

	compactTail := func() (tail uint64, err error) {
		err = compactDB.View(func(tx *bbolt.Tx) error {
			bk := tx.Bucket([]byte("wal"))
			if bk != nil {
				if k, _ := bk.Cursor().Last(); len(k) == 8 {
					tail = binary.BigEndian.Uint64(k)
				}
			}
			return nil
		})
		return
	}

	for {
		ct, err := compactTail()
		if err != nil {
			return err
		}
		mt, err := s.myLogTail(shard)
		if err != nil {
			return err
		}
		if ct > mt {
			return fmt.Errorf("wtf")
		}
		if mt-ct <= uint64(s.CompactTxSize*2) {
			break // The gap is close enough, it is time to move on to the 3rd stage
		}
	}

	return nil
}
