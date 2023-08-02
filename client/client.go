package client

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync"

	"github.com/coyove/s2db/s2"
	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
)

type Session struct {
	rdb []*redis.Client
	mu  sync.Mutex
	ids []string
}

func Begin(peers ...*redis.Client) *Session {
	if len(peers) == 0 {
		panic("no peer available")
	}
	return &Session{rdb: peers}
}

func (a *Session) ShuffleServers() *Session {
	a.rdb = append([]*redis.Client{}, a.rdb...)
	rand.Shuffle(len(a.rdb), func(i, j int) {
		a.rdb[i], a.rdb[j] = a.rdb[j], a.rdb[i]
	})
	return a
}

func (a *Session) AppendDistinct(ctx context.Context, opts *s2.AppendOptions, key string, prefix string, data any) (string, error) {
	if opts == nil {
		opts = &s2.AppendOptions{}
	}
	if len(prefix) > 255 {
		return "", fmt.Errorf("distinct prefix %q exceeds 255 bytes", prefix)
	}
	opts.DPLen = byte(len(prefix))
	res, err := a.Append(ctx, opts, key, append([]byte(prefix), s2.ToBytes(data)...))
	if err != nil {
		return "", err
	}
	return res[0], nil
}

func (a *Session) Append(ctx context.Context, opts *s2.AppendOptions, key string, data ...any) ([]string, error) {
	if opts == nil {
		opts = &s2.AppendOptions{}
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("can't append empty data")
	}

	args := []any{"APPEND", key, data[0]}
	if !opts.NoSync {
		args = append(args, "SYNC")
	}
	if opts.DPLen > 0 {
		args = append(args, "DP", opts.DPLen)
	}
	if opts.Effect {
		args = append(args, "EFFECT")
	}
	if opts.NoExpire {
		args = append(args, "NOEXP")
	}
	if opts.Defer {
		args = append(args, "DEFER")
	}
	for i := 1; i < len(data); i++ {
		args = append(args, "AND", data[i])
	}
	return a.send(ctx, redis.NewStringSliceCmd(ctx, args...))
}

func (a *Session) WaitEffect() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.ids) == 0 {
		return
	}

	x := a.ids[0]
	for i := 1; i < len(a.ids); i++ {
		if a.ids[i] > x {
			x = a.ids[i]
		}
	}

	id, _ := hex.DecodeString(x)
	future.Future(binary.BigEndian.Uint64(id)).Wait()
}

func (a *Session) Select(ctx context.Context, opts *s2.SelectOptions, key string, cursor string, n int) (p []s2.Pair, err error) {
	if opts == nil {
		opts = &s2.SelectOptions{}
	}
	args := []any{"SELECT", key, cursor, n}
	if opts.Desc {
		args = append(args, "DESC")
	}
	if opts.Raw {
		args = append(args, "RAW")
	}
	if opts.Async {
		args = append(args, "ASYNC")
	}
	cmd := redis.NewStringSliceCmd(ctx, args...)

	for _, db := range a.rdb {
		db.Process(ctx, cmd)
		err = cmd.Err()
		if err != nil {
			continue
		}

		res := cmd.Val()
		for i := 0; i < len(res); i += 3 {
			var x s2.Pair
			x.ID, _ = hex.DecodeString(res[i])
			x.Data = []byte(res[i+2])
			t := s2.ParseUint64(res[i+1]) % 10
			x.Con = t&1 > 0
			x.All = t&2 > 0
			p = append(p, x)
		}
		return
	}

	return
}

func (a *Session) Lookup(ctx context.Context, id string) (data []byte, err error) {
	cmd := redis.NewStringCmd(ctx, "LOOKUP", id)
	for _, db := range a.rdb {
		db.Process(ctx, cmd)
		err = cmd.Err()
		if err != nil {
			continue
		}
		return cmd.Bytes()
	}
	return
}

func (a *Session) Count(ctx context.Context, key string, start, end any, max int) (count int64, err error) {
	cmd := redis.NewIntCmd(ctx, "COUNT", key, start, end, max)
	for _, db := range a.rdb {
		db.Process(ctx, cmd)
		err = cmd.Err()
		if err != nil {
			continue
		}
		return cmd.Result()
	}
	return
}

func (a *Session) send(ctx context.Context, cmd *redis.StringSliceCmd) ([]string, error) {
	err := fmt.Errorf("all failed")
	for _, db := range a.rdb {
		db.Process(ctx, cmd)
		err = cmd.Err()
		if err != nil {
			continue
		}

		res := cmd.Val()

		a.mu.Lock()
		a.ids = append(a.ids, res...)
		a.mu.Unlock()

		return res, nil
	}

	return nil, err
}
