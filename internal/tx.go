package internal

import (
	"sync"

	"go.etcd.io/bbolt"
)

type OnetimeLimitedTx struct {
	mu       sync.Mutex
	db       *bbolt.DB
	tx       *bbolt.Tx
	bkMap    map[string]*bbolt.Bucket
	puts     int
	size     int
	finished bool
}

func CreateOnetimeLimitedTx(db *bbolt.DB, size int) (*OnetimeLimitedTx, error) {
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	x := &OnetimeLimitedTx{
		tx:    tx,
		db:    db,
		bkMap: make(map[string]*bbolt.Bucket),
		size:  size,
	}
	return x, nil
}

type OnetimeLimitedTxPut struct {
	BkName     string
	Seq        uint64
	Finishing  func(tx *bbolt.Tx, bk *bbolt.Bucket) error
	Key, Value []byte
}

func (tx *OnetimeLimitedTx) Put(p *OnetimeLimitedTxPut) (err error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	bk, ok := tx.bkMap[p.BkName]
	if !ok {
		bk, err = tx.tx.CreateBucketIfNotExists([]byte(p.BkName))
		if err != nil {
			return err
		}
		tx.bkMap[p.BkName] = bk
		bk.FillPercent = 0.9
		bk.SetSequence(p.Seq)
	}

	if p.Finishing != nil {
		if err := p.Finishing(tx.tx, bk); err != nil {
			return err
		}
		delete(tx.bkMap, p.BkName)
		return
	}

	if err := bk.Put(p.Key, p.Value); err != nil {
		return err
	}

	tx.puts++

	if tx.puts >= tx.size {
		err := tx.tx.Commit()
		if err != nil {
			return err
		}
		tx.bkMap = make(map[string]*bbolt.Bucket)
		tx.tx, err = tx.db.Begin(true)
		if err != nil {
			return err
		}
		tx.puts = 0
	}
	return nil
}

func (tx *OnetimeLimitedTx) Close() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if !tx.finished {
		tx.finished = true
		tx.tx.Rollback()
	}
}

func (tx *OnetimeLimitedTx) Finish() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.finished = true
	return tx.tx.Commit()
}

type BucketWalker struct {
	Bucket      *bbolt.Bucket
	BucketName  string
	Tx          *OnetimeLimitedTx
	QueueTTL    int
	WALStartBuf []byte
	Total       *int64
	QueueDrops  *int64
}
