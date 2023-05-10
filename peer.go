package main

import (
	"context"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/coyove/s2db/s2pkg"
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
	wait   chan *commandIn
	pstart int64
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
			if len(commands) < e.server.ServerConfig.BatchLimit {
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

func (s *Server) ForeachPeerSendCmd(f func() redis.Cmder) (int, <-chan *commandIn) {
	recv := 0
	out := make(chan *commandIn, len(s.Peers))
	s.ForeachPeer(func(_ int, p *endpoint, cli *redis.Client) {
		select {
		case p.jobq <- &commandIn{e: p, Cmder: f(), wait: out, pstart: future.UnixNano()}:
			recv++
		case <-time.After(time.Duration(s.ServerConfig.TimeoutPeer) * time.Millisecond):
			logrus.Errorf("failed to send peer job (%s), timed out", p.Config().Addr)
		}
	})
	return recv, out
}

func (s *Server) ProcessPeerResponse(recv int, out <-chan *commandIn, f func(redis.Cmder) bool) (success int) {
	if recv == 0 {
		return
	}
MORE:
	select {
	case res := <-out:
		x, _ := s.Survey.PeerLatency.LoadOrStore(res.e.Config().Addr, new(s2pkg.Survey))
		x.(*s2pkg.Survey).Incr((future.UnixNano() - res.pstart) / 1e6)

		if err := res.Cmder.Err(); err != nil {
			uri := res.e.Config().URI
			if !s.errThrot.Throttle(uri, err) {
				logrus.Errorf("[%s] failed to request %s: %v", res.Cmder.Name(), uri, err)
			}
			if s.test.MustAllPeers {
				panic("not all peers respond")
			}
		} else {
			if f(res.Cmder) {
				success++
			}
		}
		if recv--; recv > 0 {
			goto MORE
		}
	case <-time.After(time.Duration(s.ServerConfig.TimeoutPeer) * time.Millisecond):
		_, fn, ln, _ := runtime.Caller(1)
		logrus.Errorf("%s:%d failed to request peer, timed out, remains: %v", filepath.Base(fn), ln, recv)
	}
	return
}
