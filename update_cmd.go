package main

import (
	"strings"

	"github.com/secmask/go-redisproto"
)

func (s *Server) runZAdd(w *redisproto.Writer, name string, command *redisproto.Command) error {
	var xx, nx, ch, data, deferAdd bool
	idx := 2
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
		case "DEFER":
			deferAdd = true
			continue
		}
		break
	}

	pairs := []Pair{}
	if !data {
		for i := idx; i < command.ArgCount(); i += 2 {
			pairs = append(pairs, Pair{Key: string(command.Get(i + 1)), Score: atof(string(command.Get(i)))})
		}
	} else {
		for i := idx; i < command.ArgCount(); i += 3 {
			pairs = append(pairs, Pair{Key: string(command.Get(i + 1)), Score: atof(string(command.Get(i))), Data: command.Get(i + 2)})
		}
	}
	if deferAdd {
		// WIP
		return w.WriteInt(int64(len(pairs)))
	}

	added, updated, err := s.ZAdd(name, pairs, nx, xx, dumpCommand(command))
	if err != nil {
		return w.WriteError(err.Error())
	}
	s.cache.Remove(name, s)
	if ch {
		return w.WriteInt(int64(added + updated))
	}
	return w.WriteInt(int64(added))
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
	v, err := s.ZIncrBy(name, string(command.Get(3)), atof(string(command.Get(2))), dumpCommand(command))
	if err != nil {
		return w.WriteError(err.Error())
	}
	s.cache.Remove(name, s)
	return w.WriteBulkString(ftoa(v))
}
