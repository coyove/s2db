package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/coyove/s2db/wire"
	"github.com/go-redis/redis/v8"
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

func (e *endpoint) CreateRedis(connString string) (changed bool, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if connString != e.config.Raw {
		if connString != "" {
			cfg, err := wire.ParseConnString(connString)
			if err != nil {
				return false, err
			}
			if cfg.Name == "" {
				return false, fmt.Errorf("sevrer name must be set")
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

func (e *endpoint) send(cmd redis.Cmder, out chan *commandIn) bool {
	select {
	case e.jobq <- &commandIn{e: e, Cmder: cmd, wait: out}:
		return true
	case <-time.After(time.Duration(e.server.ServerConfig.PeerTimeout) * time.Millisecond):
		return false
	}
}

func (e *endpoint) work() {
	ctx := context.TODO()
	for {
		cli := e.Redis()
		if cli == nil {
			time.Sleep(time.Second)
			continue
		}

		var commands []*commandIn
		cmd, ok := <-e.jobq
		if !ok {
			return
		}
		commands = append(commands, cmd)

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

		p := cli.Pipeline()
		for _, cmd := range commands {
			p.Process(ctx, cmd.Cmder)
		}
		p.Exec(ctx)
		for _, cmd := range commands {
			cmd.wait <- cmd
		}
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
