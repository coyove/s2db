package main

import (
	"os"
	"runtime"
	"strconv"

	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) compactShard(shard int) error {
	log := log.WithField("shard", strconv.Itoa(shard))

	x := &s.db[shard]
	old := x.DB
	path := old.Path()
	for len(x.batchTx) > 0 { // wait batch worker to clear the queue
		runtime.Gosched()
	}
	old.Update(func(*bbolt.Tx) error { return nil })
	old.Close()

	roDB, err := bbolt.Open(path, 0666, bboltReadonlyOptions)
	if err != nil {
		return err
	}
	x.DB = roDB // harsh way: swap the shard db with readonly one

	log.Info("swapped rw->ro")

	compactDB, err := bbolt.Open(path+".compact", 0666, bboltOptions)
	if err != nil {
		return err
	}
	if err := bbolt.Compact(compactDB, roDB, int64(s.CompactTxSize)); err != nil {
		compactDB.Close()
		return err
	}

	roSize, compactSize := roDB.Size(), compactDB.Size()

	// Replace the database file on disk
	roDB.Close()
	if err := os.Remove(path); err != nil {
		compactDB.Close()
		return err
	}

	if err := os.Rename(compactDB.Path(), path); err != nil {
		compactDB.Close()
		return err // keep ...
	}

	log.Info("compact size: ", roSize, "->", compactSize)
	compactDB.Close()
	db, err := bbolt.Open(path, 0666, bboltOptions)
	if err != nil {
		return err
	}
	x.DB = db
	return nil
}
