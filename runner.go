package main

import (
	"runtime/debug"
	"strconv"
	"time"

	"github.com/coyove/s2db/redisproto"
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

func (s *Server) runPreparedTxAndWrite(name string, deferred bool, f func(tx *bbolt.Tx) (interface{}, error), w *redisproto.Writer) error {
	t := &batchTask{f: f, out: make(chan interface{}, 1)}
	s.db[shardIndex(name)].batchTx <- t
	if deferred {
		s.removeCache(name)
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
	case int64:
		return w.WriteInt(res)
	case float64:
		return w.WriteBulkString(ftoa(res))
	default:
		panic(-99)
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

		s.runTasks(tasks, shard)
	}

EXIT:
	log.Info("batch worker exited")
	x.batchCloseSignal <- true
}

func (s *Server) runTasks(tasks []*batchTask, shard int) {
	start := time.Now()
	for ; s.db[shard].compactReplacing && time.Since(start).Seconds() < float64(s.CompactRunWait); time.Sleep(100 * time.Millisecond) {
		// During the compaction replacing process (starting at stage 3), the shard becomes temporarily unavailable for writing
		// so we wait CompactRunWait seconds for its recovery
	}
	outs := make([]interface{}, len(tasks))
	err := s.db[shard].Update(func(tx *bbolt.Tx) error {
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

	s.Survey.BatchLat.Incr(time.Since(start).Milliseconds())
	s.Survey.BatchSize.Incr(int64(len(tasks)))
}
