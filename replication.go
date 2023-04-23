package main

import (
	"fmt"
	"sync"

	"github.com/coyove/s2db/wire"
	"github.com/go-redis/redis/v8"
)

type endpoint struct {
	mu     sync.RWMutex
	client *redis.Client
	config wire.RedisConfig
	server *Server
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
		} else {
			e.client.Close()
			e.client = nil
			e.config = wire.RedisConfig{}
		}
		changed = true
	}
	return
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
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}
