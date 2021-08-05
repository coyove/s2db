package main

import (
	"runtime/debug"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"gitlab.litatom.com/zhangzezhong/zset/redisproto"
	"go.etcd.io/bbolt"
)

func (s *Server) runPreparedRangeTx(name string, f func(tx *bbolt.Tx) ([]Pair, int, error)) (pairs []Pair, count int, err error) {
	err = s.pick(name).View(func(tx *bbolt.Tx) error {
		pairs, count, err = f(tx)
		return err
	})
	return
}

func (s *Server) runPreparedTxAndWrite(name string, deferred bool, f func(tx *bbolt.Tx) (interface{}, error), w *redisproto.Writer) error {
	t := &batchTask{f: f, out: make(chan interface{}, 1)}
	s.db[shardIndex(name)].batchTx <- t
	if deferred {
		return w.WriteSimpleString("OK")
	}
	out := <-t.out
	if err, _ := out.(error); err != nil {
		return w.WriteError(err.Error())
	}
	s.removeCache(name)
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
	log := log.WithField("shard", strconv.Itoa(shard))
	defer func() {
		if r := recover(); r != nil {
			log.Error(r, " ", string(debug.Stack()))
			time.Sleep(time.Second)
			go s.batchWorker(shard)
		}
	}()

	x := &s.db[shard]
	tasks := []*batchTask{}
	blocking := false

	for {
		tasks = tasks[:0]

		for len(tasks) < s.ServerConfig.BatchMaxRun {
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
		if err != nil {
			log.Error("error occurred: ", err, " ", len(tasks), " tasks discarded")
		}
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
	log.Info("batch worker exited")
	x.batchCloseSignal <- true
}