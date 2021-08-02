package main

import (
	"strings"
	"time"

	"github.com/secmask/go-redisproto"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) parseZAdd(cmd, name string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	var xx, nx, ch, data bool
	var idx = 2
	for ; ; idx++ {
		switch strings.ToUpper(string(command.Get(idx))) {
		case "XX":
			xx = true
			continue
		case "NX":
			nx = true
			continue
		case "CH":
			ch = true
			continue
		case "DATA":
			data = true
			continue
		}
		break
	}

	pairs := []Pair{}
	if !data {
		for i := idx; i < command.ArgCount(); i += 2 {
			s, err := atof2(command.Get(i))
			if err != nil {
				return func(*bbolt.Tx) (interface{}, error) { return nil, err }
			}
			pairs = append(pairs, Pair{Key: string(command.Get(i + 1)), Score: s})
		}
	} else {
		for i := idx; i < command.ArgCount(); i += 3 {
			s, err := atof2(command.Get(i))
			if err != nil {
				return func(*bbolt.Tx) (interface{}, error) { return nil, err }
			}
			pairs = append(pairs, Pair{Key: string(command.Get(i + 1)), Score: s, Data: command.Get(i + 2)})
		}
	}
	return s.prepareZAdd(name, pairs, nx, xx, ch, dumpCommand(command))
}

func (s *Server) parseDel(cmd, name string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	dd := dumpCommand(command)
	switch cmd {
	case "DEL":
		return s.prepareDel(name, dd)
	case "ZREM":
		return s.prepareZRem(name, restCommandsToKeys(2, command), dd)
	}
	start, end := string(command.Get(2)), string(command.Get(3))
	switch cmd {
	case "ZREMRANGEBYLEX":
		return s.prepareZRemRangeByLex(name, start, end, dd)
	case "ZREMRANGEBYSCORE":
		return s.prepareZRemRangeByScore(name, start, end, dd)
	case "ZREMRANGEBYRANK":
		return s.prepareZRemRangeByRank(name, atoip(start), atoip(end), dd)
	default:
		panic(-1)
	}
}

func (s *Server) parseZIncrBy(cmd, name string, command *redisproto.Command) func(*bbolt.Tx) (interface{}, error) {
	by, err := atof2(command.Get(2))
	if err != nil {
		return func(*bbolt.Tx) (interface{}, error) { return nil, err }
	}
	return s.prepareZIncrBy(name, string(command.Get(3)), by, dumpCommand(command))
}

type addTask struct {
	f   func(*bbolt.Tx) (interface{}, error)
	out chan interface{}
}

func (s *Server) deferAddWorker(shard int) {
	x := &s.db[shard]
	tasks := []*addTask{}
	blocking := false

	for {
		tasks = tasks[:0]

		for len(tasks) < s.ServerConfig.MaxBatchRun {
			if blocking {
				t, ok := <-x.deferAdd
				if !ok {
					goto EXIT
				}
				tasks = append(tasks, t)
				blocking = false
			} else {
				select {
				case t, ok := <-x.deferAdd:
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
	x.deferCloseSignal <- true
}
