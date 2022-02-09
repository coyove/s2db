package s2pkg

import (
	"sync"

	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

type LimitedTx struct {
	mu       sync.Mutex
	db       *bbolt.DB
	tx       *bbolt.Tx
	bkMap    map[string]*bbolt.Bucket
	puts     int
	size     int
	finished bool

	MapSize Survey
}

func CreateLimitedTx(db *bbolt.DB, size int) (*LimitedTx, error) {
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	x := &LimitedTx{
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

func (tx *LimitedTx) Put(p *OnetimeLimitedTxPut) (err error) {
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
	tx.MapSize.Incr(int64(len(tx.bkMap)))

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

func (tx *LimitedTx) Close() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.tx.Rollback()
}

func (tx *LimitedTx) Finish() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.finished = true
	return tx.tx.Commit()
}

type BucketWalker struct {
	Bucket          *bbolt.Bucket
	BucketName      string
	Tx              *LimitedTx
	QueueTTL        int
	LogtailStartBuf []byte
	Total           *int64
	QueueDrops      *int64
	QueueDeletes    *int64
	ZSetCardFix     *int64
	Logger          *logrus.Entry
}
