package server

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coyove/s2db/s2"
	"github.com/coyove/s2db/s2/resp"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

type endpoint struct {
	mu     sync.RWMutex
	index  int
	client *redis.Client
	config resp.RedisConfig
	server *Server

	job struct {
		once   sync.Once
		q      chan *endpointCmd
		closed atomic.Bool
		wait   sync.WaitGroup
	}
}

type endpointCmd struct {
	redis.Cmder
	ep    *endpoint
	out   chan *endpointCmd
	start int64
	async bool
}

func (e *endpoint) Set(uri string) (changed bool, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if uri != e.config.URI {
		if uri != "" {
			cfg, err := resp.ParseConnString(uri)
			if err != nil {
				return false, err
			}
			old := e.client
			e.config, e.client = cfg, cfg.GetClient()
			if old != nil {
				old.Close()
			}
			e.job.once.Do(func() {
				e.job.q = make(chan *endpointCmd, 1e3)
				for i := 0; i < runtime.NumCPU()*5; i++ {
					e.job.wait.Add(1)
					go e.work()
				}
			})
		} else {
			// We don't call e.Close() because following e.Set() may exist and reset a new redis.
			// e.Close() will only be called when 'e' is not needed anymore (e.g. server close).
			e.client.Close()
			e.client = nil
			e.config = resp.RedisConfig{}
		}
		changed = true
	}
	return
}

func (e *endpoint) work() {
	defer func() {
		e.job.wait.Done()
		logrus.Debugf("%s worker exited", e.Config().Addr)
	}()

	ctx := context.TODO()
	for !e.job.closed.Load() {
		if e.Redis() == nil {
			time.Sleep(time.Second)
			continue
		}

		cmd, ok := <-e.job.q
		if !ok {
			return
		}
		commands := []*endpointCmd{cmd}

	MORE:
		select {
		case cmd, ok := <-e.job.q:
			if !ok {
				break
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
				cmd.out <- cmd
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
			addr := cmd.ep.config.Addr
			if cmd.async {
				if err := cmd.Err(); err != nil {
					if !cmd.ep.server.errThrot.Throttle(addr, err) {
						if !strings.Contains(err.Error(), "failed on purpose") {
							logrus.Errorf("async command error: %v", err)
						}
					}
				}
			} else {
				cmd.out <- cmd
			}
			x, _ := cmd.ep.server.Survey.PeerLatency.LoadOrStore(addr, new(s2.Survey))
			x.(*s2.Survey).Incr((future.UnixNano() - cmd.start) / 1e6)
		}
		e.server.Survey.PeerBatchSize.Incr(int64(len(commands)))
	}
}

func (e *endpoint) Redis() *redis.Client {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.client
}

func (e *endpoint) Config() resp.RedisConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.config
}

func (e *endpoint) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.job.closed.Store(true)
	if e.job.q != nil {
		close(e.job.q)
		e.job.wait.Wait()
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
	Oneshot bool // exit when first resp() == true
	Async   bool
}

func (s *Server) ForeachPeerSendCmd(
	opts SendCmdOptions,
	req func() redis.Cmder,
	resp func(redis.Cmder) bool,
) (sent, success int) {
	pstart := future.UnixNano()
	pcmd := req()

	out := make(chan *endpointCmd, future.Channels)
	total := 0
	for i := 0; i < len(s.Peers); i++ {
		p := s.Peers[i]
		if cli := p.Redis(); cli != nil && s.Channel != int64(i) {
			select {
			case p.job.q <- &endpointCmd{
				ep:    p,
				start: pstart,
				Cmder: req(),
				out:   out,
				async: opts.Async,
			}:
				sent++
			case <-time.After(time.Duration(s.Config.TimeoutPeer) * time.Millisecond):
				logrus.Errorf("failed to send peer job (%s), queue is full", p.Config().Addr)
			}
			total++
		}
	}

	if sent == 0 || opts.Async {
		return
	}

	w := time.Duration(s.Config.TimeoutPeer) * time.Millisecond
	recv := 0

	var ackList [future.Channels]bool
MORE:
	select {
	case res := <-out:
		ackList[res.ep.index] = true
		addr := res.ep.Config().Addr

		if err := res.Cmder.Err(); err != nil {
			if !s.errThrot.Throttle(addr, err) {
				logrus.Errorf("[%s] failed to request %s: %v", strings.ToUpper(res.Cmder.Name()), addr, err)
			}
		} else {
			if resp == nil {
				success++
				if opts.Oneshot {
					return
				}
			} else if resp(res.Cmder) {
				success++
				if opts.Oneshot {
					return
				}
			}
		}
		if recv++; recv < sent {
			goto MORE
		}
	case <-time.After(w):
		_, fn, ln, _ := runtime.Caller(1)
		var remains []string
		s.ForeachPeer(func(i int, ep *endpoint, cli *redis.Client) {
			if !ackList[i] {
				remains = append(remains, ep.config.Addr)
			}
		})
		logrus.Errorf("[%s] %s:%d timed out to request all peers (%d/%d), remains: %v",
			strings.ToUpper(pcmd.Name()), filepath.Base(fn), ln, recv, sent, remains)
		s.Survey.PeerTimeout.Incr(1)
	}
	return
}
