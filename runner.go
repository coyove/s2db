package main

import (
	"time"

	"github.com/secmask/go-redisproto"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) runPreparedRangeTx(name string, f func(tx *bbolt.Tx) ([]Pair, int, error)) (pairs []Pair, count int, err error) {
	err = s.pick(name).View(func(tx *bbolt.Tx) error {
		pairs, count, err = f(tx)
		return err
	})
	return
}

func (s *Server) runPreparedTxAndWrite(name string, f func(tx *bbolt.Tx) (interface{}, error), w *redisproto.Writer) error {
	t := &batchTask{f: f, out: make(chan interface{})}
	s.db[shardIndex(name)].batchTx <- t
	out := <-t.out
	if err, _ := out.(error); err != nil {
		return w.WriteError(err.Error())
	}
	s.cache.Remove(name, s)
	switch res := out.(type) {
	case int:
		return w.WriteInt(int64(res))
	case float64:
		return w.WriteBulkString(ftoa(res))
	default:
		panic(-1)
	}
}

type batchTask struct {
	f   func(*bbolt.Tx) (interface{}, error)
	out chan interface{}
}

func (s *Server) batchWorker(shard int) {
	x := &s.db[shard]
	tasks := []*batchTask{}
	blocking := false

	for {
		tasks = tasks[:0]

		for len(tasks) < s.ServerConfig.MaxBatchRun {
			if blocking {
				t, ok := <-x.batchTx
				if !ok {
					goto EXIT
				}
				tasks = append(tasks, t)
				blocking = false
			} else {
				select {
				case t, ok := <-x.batchTx:
					if !ok {
						if len(tasks) == 0 {
							goto EXIT
						}
						goto RUN_TASKS
					} else {
						tasks = append(tasks, t)
					}
				default:
					goto RUN_TASKS
				}
			}
		}
	RUN_TASKS:

		if len(tasks) == 0 {
			blocking = true
			continue
		}

		start := time.Now()
		outs := make([]interface{}, len(tasks))
		err := x.Update(func(tx *bbolt.Tx) error {
			var err error
			for i, t := range tasks {
				outs[i], err = t.f(tx)
				if err != nil {
					return err
				}
			}
			return nil
		})
		for i, t := range tasks {
			if err != nil {
				t.out <- err
			} else {
				t.out <- outs[i]
			}
		}

		s.survey.batchLat.Incr(time.Since(start).Milliseconds())
		s.survey.batchSize.Incr(int64(len(tasks)))
	}

EXIT:
	log.Info("#", shard, " batch worker exited")
	x.batchCloseSignal <- true
}
