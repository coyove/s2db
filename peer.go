package main

import (
	"context"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/wire"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

type endpoint struct {
	mu     sync.RWMutex
	client *redis.Client
	config wire.RedisConfig
	server *Server
	job    sync.Once
	jobq   chan *commandIn
}

type commandIn struct {
	e *endpoint
	redis.Cmder
	wait chan *commandIn
}

func (e *endpoint) CreateRedis(uri string) (changed bool, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if uri != e.config.URI {
		if uri != "" {
			cfg, err := wire.ParseConnString(uri)
			if err != nil {
				return false, err
			}
			old := e.client
			e.config, e.client = cfg, cfg.GetClient()
			if old != nil {
				old.Close()
			}
			e.job.Do(func() {
				e.jobq = make(chan *commandIn, 1e3)
				for i := 0; i < runtime.NumCPU()*5; i++ {
					go e.work()
				}
			})
		} else {
			e.client.Close()
			e.client = nil
			e.config = wire.RedisConfig{}
		}
		changed = true
	}
	return
}

func (e *endpoint) work() {
	defer func() {
		logrus.Debugf("%s worker exited", e.Config().URI)
	}()

	ctx := context.TODO()
	for {
		if e.Redis() == nil {
			time.Sleep(time.Second)
			continue
		}

		cmd, ok := <-e.jobq
		if !ok {
			return
		}
		commands := []*commandIn{cmd}

	MORE:
		select {
		case cmd, ok := <-e.jobq:
			if !ok {
				return
			}
			commands = append(commands, cmd)
			if len(commands) < e.server.Config.BatchLimit {
				goto MORE
			}
		default:
		}

		cli := e.Redis()
		if cli == nil {
			for _, cmd := range commands {
				cmd.SetErr(redis.ErrClosed)
				cmd.wait <- cmd
			}
			time.Sleep(time.Second)
			continue
		}

		p := cli.Pipeline()
		for _, cmd := range commands {
			p.Process(ctx, cmd.Cmder)
		}
		p.Exec(ctx)
		for _, cmd := range commands {
			cmd.wait <- cmd
		}
		e.server.Survey.PeerBatchSize.Incr(int64(len(commands)))
	}
}

func (e *endpoint) Redis() *redis.Client {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.client
}

func (e *endpoint) Config() wire.RedisConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.config
}

func (e *endpoint) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.jobq != nil {
		close(e.jobq)
	}
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (s *Server) OtherPeersCount() (c int) {
	for i, p := range s.Peers {
		if p.Redis() != nil && s.Channel != int64(i) {
			c++
		}
	}
	return
}

func (s *Server) HasOtherPeers() bool {
	for i, p := range s.Peers {
		if p.Redis() != nil && s.Channel != int64(i) {
			return true
		}
	}
	return false
}

func (s *Server) ForeachPeer(f func(i int, p *endpoint, c *redis.Client)) {
	for i, p := range s.Peers {
		if cli := p.Redis(); cli != nil && s.Channel != int64(i) {
			f(i, p, cli)
		}
	}
}

type SendCmdOptions struct {
	LongWait bool // some commands (APPEND) may require longer waits
	Quorum   bool // return immediately upon receiving enough acknowledgements
}

func (s *Server) ForeachPeerSendCmd(
	opts SendCmdOptions,
	req func() redis.Cmder,
	resp func(redis.Cmder) bool,
) (sent, success int) {
	pstart := future.UnixNano()

	out := make(chan *commandIn, len(s.Peers))
	total := 0
	for i := 0; i < len(s.Peers); i++ {
		p := s.Peers[i]
		if cli := p.Redis(); cli != nil && s.Channel != int64(i) {
			select {
			case p.jobq <- &commandIn{e: p, Cmder: req(), wait: out}:
				sent++
			case <-time.After(time.Duration(s.Config.TimeoutPeer) * time.Millisecond):
				logrus.Errorf("failed to send peer job (%s), timed out", p.Config().Addr)
			}
			total++
		}
	}
	if sent == 0 {
		return
	}

	w := time.Duration(s.Config.TimeoutPeer) * time.Millisecond
	if opts.LongWait {
		w = time.Duration(s.Config.TimeoutPeerLong) * time.Millisecond
	}

	recv := 0
	goal := sent
	if opts.Quorum {
		goal = (total+1)/2 + 1 - 1
	}

MORE:
	select {
	case res := <-out:
		addr := res.e.Config().Addr
		x, _ := s.Survey.PeerLatency.LoadOrStore(addr, new(s2.Survey))
		x.(*s2.Survey).Incr((future.UnixNano() - pstart) / 1e6)

		if err := res.Cmder.Err(); err != nil {
			if !s.errThrot.Throttle(addr, err) {
				logrus.Errorf("[%s] failed to request %s: %v", strings.ToUpper(res.Cmder.Name()), addr, err)
			}
		} else {
			if resp == nil {
				success++
			} else if resp(res.Cmder) {
				success++
			}
			if success >= goal {
				break
			}
		}
		if recv++; recv < sent {
			goto MORE
		}
	case <-time.After(w):
		_, fn, ln, _ := runtime.Caller(1)
		logrus.Errorf("%s:%d failed to request peer, timed out (%d/%d)", filepath.Base(fn), ln, recv, sent)
	}
	return
}

func (s *Server) requireQuorum(hexIds [][]byte, f func() redis.Cmder) [][]byte {
	if s.HasOtherPeers() {
		sent, success := s.ForeachPeerSendCmd(SendCmdOptions{LongWait: true, Quorum: true}, f, nil)
		hexIds = append([][]byte{
			strconv.AppendInt(nil, int64(sent)+1, 10),
			strconv.AppendInt(nil, int64(success)+1, 10),
		}, hexIds...)
	} else {
		hexIds = append([][]byte{[]byte("1"), []byte("1")}, hexIds...)
	}
	return hexIds
}
