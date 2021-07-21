package main

import (
	"math"
	"strings"

	"github.com/secmask/go-redisproto"
)

func (s *Server) runZAdd(w *redisproto.Writer, name string, command *redisproto.Command) error {
	var xx, nx, ch, data, deferAdd bool
	var idx = 2
	var scoreGt float64 = math.NaN()
	var err error
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
		case "SCOREGT":
			idx++
			scoreGt, err = atof(string(command.Get(idx)))
			if err != nil {
				return w.WriteError(err.Error())
			}
			continue
		case "DEFER":
			deferAdd = true
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
				return w.WriteError(err.Error())
			}
			pairs = append(pairs, Pair{Key: string(command.Get(i + 1)), Score: s})
		}
	} else {
		for i := idx; i < command.ArgCount(); i += 3 {
			s, err := atof2(command.Get(i))
			if err != nil {
				return w.WriteError(err.Error())
			}
			pairs = append(pairs, Pair{Key: string(command.Get(i + 1)), Score: s, Data: command.Get(i + 2)})
		}
	}

	if deferAdd {
		select {
		default:
			return w.WriteSimpleString("OK")
		}
	}

	added, updated, err := s.ZAdd(name, pairs, nx, xx, scoreGt, dumpCommand(command))
	if err != nil {
		return w.WriteError(err.Error())
	}
	s.cache.Remove(name, s)
	if ch {
		return w.WriteInt(int64(added + updated))
	}
	return w.WriteInt(int64(added))
}

func (s *Server) runZAddBatchShard(w *redisproto.Writer, name string, command *redisproto.Command) error {
	pairs := []*addTask{}
	names := map[string]bool{}
	for i := 1; i < command.ArgCount(); i += 4 {
		name := string(command.Get(i))
		s, err := atof2(command.Get(i + 1))
		if err != nil {
			return w.WriteError(err.Error())
		}
		pairs = append(pairs, &addTask{
			name: name,
			pair: Pair{
				Key:   string(command.Get(i + 2)),
				Score: s,
				Data:  command.Get(i + 3),
			},
		})
		names[name] = true
	}

	err := s.ZAddBatchShard(pairs, dumpCommand(command))
	if err != nil {
		return w.WriteError(err.Error())
	}
	for name := range names {
		s.cache.Remove(name, s)
	}
	return w.WriteInt(int64(len(pairs)))
}

func (s *Server) runZRemRange(w *redisproto.Writer, cmd, name string, command *redisproto.Command) error {
	start, end := string(command.Get(2)), string(command.Get(3))

	var p []Pair
	var err error
	dd := dumpCommand(command)

	switch cmd {
	case "ZREMRANGEBYLEX":
		p, err = s.ZRemRangeByLex(name, start, end, dd)
	case "ZREMRANGEBYSCORE":
		p, err = s.ZRemRangeByScore(name, start, end, dd)
	case "ZREMRANGEBYRANK":
		p, err = s.ZRemRangeByRank(name, atoi(start), atoi(end), dd)
	}
	if err != nil {
		return w.WriteError(err.Error())
	}
	s.cache.Remove(name, s)
	return w.WriteInt(int64(len(p)))
}

func (s *Server) runZRem(w *redisproto.Writer, name string, command *redisproto.Command) error {
	c, err := s.ZRem(name, restCommandsToKeys(2, command), dumpCommand(command))
	if err != nil {
		return w.WriteError(err.Error())
	}
	s.cache.Remove(name, s)
	return w.WriteInt(int64(c))
}

func (s *Server) runDel(w *redisproto.Writer, name string, command *redisproto.Command) error {
	c, err := s.Del(name, dumpCommand(command))
	if err != nil {
		return w.WriteError(err.Error())
	}
	s.cache.Remove(name, s)
	return w.WriteInt(int64(c))
}

func (s *Server) runZIncrBy(w *redisproto.Writer, name string, command *redisproto.Command) error {
	by, err := atof2(command.Get(2))
	if err != nil {
		return w.WriteError(err.Error())
	}
	v, err := s.ZIncrBy(name, string(command.Get(3)), by, dumpCommand(command))
	if err != nil {
		return w.WriteError(err.Error())
	}
	s.cache.Remove(name, s)
	return w.WriteBulkString(ftoa(v))
}

type addTask struct {
	name string
	pair Pair
}

func (s *Server) deferAddWorker(shard int) {
	x := &s.db[shard]
	// 	tasks := []*addTask{}
	// 	tmp := &bytes.Buffer{}
	// 	dummy := redisproto.NewWriter(tmp)
	//
	// 	for {
	// 		tasks = tasks[:0]
	// 		for start := nanotime.Now(); ; {
	// 			select {
	// 			case t, ok := <-x.deferAdd:
	// 				if !ok {
	// 					goto EXIT
	// 				}
	// 				tasks = append(tasks, t)
	// 			default:
	// 			}
	//
	// 			if len(tasks) == 0 {
	// 				time.Sleep(time.Millisecond * 100)
	// 				continue
	// 			}
	// 			if len(tasks) >= 50 || nanotime.Since(start) > time.Millisecond*100 {
	// 				break
	// 			}
	// 		}
	//
	// 		tmp.Reset()
	// 		s.runZAdd(dummy, name)
	// 	}
	// EXIT:
	x.deferCloseSignal <- true
}
