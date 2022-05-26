package main

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/redisproto"
	"github.com/coyove/s2db/s2pkg"
	log "github.com/sirupsen/logrus"
)

const (
	RunNormal = iota + 1
	RunDefer
	RunSync
)

func parseRunFlag(in *redisproto.Command) int {
	if len(in.Argv) > 2 {
		if bytes.EqualFold(in.Argv[2], []byte("--defer--")) {
			in.Argv = append(in.Argv[:2], in.Argv[3:]...)
			return RunDefer
		}
		if bytes.EqualFold(in.Argv[2], []byte("--sync--")) {
			in.Argv = append(in.Argv[:2], in.Argv[3:]...)
			return RunSync
		}
		if bytes.EqualFold(in.Argv[2], []byte("--normal--")) {
			in.Argv = append(in.Argv[:2], in.Argv[3:]...)
		}
	}
	return RunNormal
}

type rangeFunc func(s2pkg.LogTx) ([]s2pkg.Pair, int, error)

func (s *Server) runPreparedRangeTx(key string, f rangeFunc) (pairs []s2pkg.Pair, count int, err error) {
	pairs, count, err = f(s2pkg.LogTx{Storage: s.DB})
	return
}

type preparedTx struct {
	f func(tx s2pkg.LogTx) (interface{}, error)
}

func (s *Server) runPreparedTx(cmd, key string, runType int, ptx preparedTx) (interface{}, error) {
	t := &batchTask{
		f:       ptx.f,
		key:     key,
		runType: runType,
		out:     make(chan interface{}, 1),
	}
	if s.Closed {
		return nil, fmt.Errorf("server closing stage")
	}

	defer func(start time.Time) {
		diffMs := time.Since(start).Milliseconds()
		s.Survey.SysWrite.Incr(diffMs)
		x, _ := s.Survey.Command.LoadOrStore(cmd, new(s2pkg.Survey))
		x.(*s2pkg.Survey).Incr(diffMs)
		if runType != RunDefer {
			x, _ := s.Survey.Command.LoadOrStore(cmd+"_S", new(s2pkg.Survey))
			x.(*s2pkg.Survey).Incr(diffMs)
		}
	}(time.Now())

	s.shards[shardIndex(key)].batchTx <- t
	if runType == RunDefer {
		return nil, nil
	}
	out := <-t.out
	if err, _ := out.(error); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) runPreparedTxAndWrite(cmd, key string, runType int, ptx preparedTx, w *redisproto.Writer) error {
	out, err := s.runPreparedTx(cmd, key, runType, ptx)
	if err != nil {
		return w.WriteError(err.Error())
	}
	if runType == RunDefer {
		return w.WriteSimpleString("OK")
	}
	switch res := out.(type) {
	case int:
		return w.WriteInt(int64(res))
	case int64:
		return w.WriteInt(res)
	case float64:
		return w.WriteBulkString(s2pkg.FormatFloat(res))
	case error:
		return w.WriteError(res.Error())
	default:
		panic(fmt.Sprintf("invalid result type: %T", out))
	}
}

type batchTask struct {
	f       func(s2pkg.LogTx) (interface{}, error)
	key     string
	runType int

	out    chan interface{}
	outLog uint64
}

func (s *Server) batchWorker(shard int) {
	log := log.WithField("shard", "#"+strconv.Itoa(shard))

	defer s2pkg.Recover(func() {
		time.Sleep(time.Second)
		go s.batchWorker(shard)
	})

	x := &s.shards[shard]
	tasks := []*batchTask{}

	blocking := false
	for {
		tasks = tasks[:0]
		firstRunSlept := false

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
					// If we only receive 1 deferred task and have not slept yet, the runner will sleep X ms to
					// see if there are any more tasks to come.
					if !firstRunSlept && s.BatchFirstRunSleep > 0 && len(tasks) == 1 && tasks[0].runType == RunDefer {
						tmr := time.NewTimer(time.Millisecond * time.Duration(s.BatchFirstRunSleep))
					CONTINUE_SLEEP:
						select {
						case t, ok := <-x.batchTx:
							if ok {
								tasks = append(tasks, t)
								if t.runType == RunDefer {
									if len(tasks) < s.BatchMaxRun {
										goto CONTINUE_SLEEP
									}
								}
							}
							// During sleep, if we received a non-deferred task (or channel closed), we should exit immediately
						case <-tmr.C:
						}
						tmr.Stop()
						firstRunSlept = true
					} else {
						goto RUN_TASKS
					}
				}
			}
		}

	RUN_TASKS:
		if len(tasks) == 0 {
			blocking = true
			continue
		}
		if firstRunSlept {
			s.Survey.FirstRunSleep.Incr(int64(len(tasks)))
		}
		s.runTasks(log, tasks, shard)
	}

EXIT:
	x.batchCloseSignal <- true
}

func (s *Server) runTasks(log *log.Entry, tasks []*batchTask, shard int) {
	start := time.Now()
	outs := make([]interface{}, len(tasks))

	b := s.DB.NewIndexedBatch()
	defer b.Close()

	ltx := s2pkg.LogTx{
		OutLogtail: new(uint64),
		LogPrefix:  getShardLogKey(int16(shard)),
		Storage:    b,
	}

	var err error
	for i, t := range tasks {
		outs[i], err = t.f(ltx)
		if err != nil {
			s.Survey.SysWriteDiscards.Incr(int64(len(tasks)))
			log.Error("error occurred: ", err, " ", len(tasks), " tasks discarded")
			break
		}
		tasks[i].outLog = *ltx.OutLogtail
	}

	if err == nil {
		s.Survey.DBBatchSize.Incr(int64(b.Count()))
		err = b.Commit(pebble.Sync)
	}

	for i, t := range tasks {
		if err != nil {
			t.out <- err
		} else {
			s.removeCache(t.key)
			if t.runType == RunSync && s.Slave.IsAcked(s) {
				s.shards[shard].syncWaiter.WaitAt(t.outLog, t.out, outs[i])
			} else {
				t.out <- outs[i]
			}
		}
	}

	select {
	case s.shards[shard].pusherTrigger <- true:
	default:
	}
	s.Survey.BatchLat.Incr(time.Since(start).Milliseconds())
	s.Survey.BatchSize.Incr(int64(len(tasks)))
}
