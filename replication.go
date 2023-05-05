package main

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/coyove/s2db/wire"
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
				for i := 0; i < runtime.NumCPU(); i++ {
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
			goto MORE
		default:
		}

		cli := e.Redis()
		if cli == nil {
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
