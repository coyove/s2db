package main

import (
	"fmt"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

type rangeFunc func(s2pkg.LogTx) ([]s2pkg.Pair, int, error)

func (s *Server) runPreparedRangeTx(key string, f rangeFunc, success func([]s2pkg.Pair, int)) (pairs []s2pkg.Pair, count int, err error) {
	err = s.pick(key).View(func(tx *bbolt.Tx) error {
		pairs, count, err = f(s2pkg.LogTx{Tx: tx})
		if err == nil {
			success(pairs, count)
		}
		return err
	})
	return
}

type preparedTx struct {
	f func(tx s2pkg.LogTx) (interface{}, error)
}

func (s *Server) runPreparedTx(cmd, key string, deferred bool, ptx preparedTx) (interface{}, error) {
	t := &batchTask{f: ptx.f, key: key, out: make(chan interface{}, 1)}
	if s.Closed {
		return nil, fmt.Errorf("server closing stage")
	}

	defer func(start time.Time) {
		diffMs := time.Since(start).Milliseconds()
		s.Survey.SysWrite.Incr(diffMs)
		x, _ := s.Survey.Command.LoadOrStore(cmd, new(s2pkg.Survey))
		x.(*s2pkg.Survey).Incr(diffMs)
	}(time.Now())

	s.db[shardIndex(key)].batchTx <- t
	if deferred {
		return nil, nil
	}
	out := <-t.out
	if err, _ := out.(error); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) runPreparedTxAndWrite(cmd, key string, deferred bool, ptx preparedTx, w *redisproto.Writer) error {
	out, err := s.runPreparedTx(cmd, key, deferred, ptx)
	if err != nil {
		return w.WriteError(err.Error())
	}
	if deferred {
		return w.WriteSimpleString("OK")
	}
	switch res := out.(type) {
	case int:
		return w.WriteInt(int64(res))
	case int64:
		return w.WriteInt(res)
	case float64:
		return w.WriteBulkString(s2pkg.FormatFloat(res))
	default:
		panic(-99)
	}
}

type batchTask struct {
	f   func(s2pkg.LogTx) (interface{}, error)
	key string
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

		s.runTasks(log, tasks, shard)
	}

EXIT:
	log.Info("batch worker exited")
	x.batchCloseSignal <- true
}

func (s *Server) runTasks(log *log.Entry, tasks []*batchTask, shard int) {
	start := time.Now()
	outs := make([]interface{}, len(tasks))
	logs := make(map[uint64][]byte, len(tasks))
	// During the compaction replacing process (starting at stage 3), the shard becomes temporarily unavailable for writing
	s.db[shard].compactLocker.Lock(func() { log.Info("runner is waiting for compactor") })
	err := s.db[shard].Update(func(tx *bbolt.Tx) (err error) {
		ltx := s2pkg.LogTx{Logs: logs, Tx: tx}
		for i, t := range tasks {
			outs[i], err = t.f(ltx)
			if err != nil {
				return err
			}
		}
		return nil
	})
	s.db[shard].compactLocker.Unlock()
	if err != nil {
		s.Survey.SysWriteDiscards.Incr(int64(len(tasks)))
		log.Error("error occurred: ", err, " ", len(tasks), " tasks discarded")
	}
	for i, t := range tasks {
		if err != nil {
			t.out <- err
		} else {
			s.removeCache(t.key)
			t.out <- outs[i]
		}
	}

	s.Survey.BatchLat.Incr(time.Since(start).Milliseconds())
	s.Survey.BatchSize.Incr(int64(len(tasks)))
}
