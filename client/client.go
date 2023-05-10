package client

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strconv"
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
	rand.Shuffle(len(a.rdb), func(i, j int) {
		a.rdb[i], a.rdb[j] = a.rdb[j], a.rdb[i]
	})
	return a
}

func (a *Session) Append(ctx context.Context, key string, data ...any) ([]string, error) {
	ids, err := a.do(ctx, key, false, 0, data...)
	return ids.KeyIDs, err
}

func (a *Session) AppendTTL(ctx context.Context, key string, ttlSec int64, data ...any) ([]string, error) {
	ids, err := a.do(ctx, key, false, ttlSec, data...)
	return ids.KeyIDs, err
}

type AppendResult struct {
	Peers, Success int
	KeyIDs         []string
}

func (a AppendResult) Quorum() bool {
	return a.Success >= a.Peers/2+1
}

func (a AppendResult) String() string {
	return fmt.Sprintf("%v (ack: %d/%d)", a.KeyIDs, a.Success, a.Peers)
}

func (a *Session) AppendQuorum(ctx context.Context, key string, ttlSec int64, data ...any) (AppendResult, error) {
	return a.do(ctx, key, true, ttlSec, data...)
}

func (a *Session) do(ctx context.Context, key string, q bool, ttlSec int64, data ...any) (AppendResult, error) {
	err := fmt.Errorf("all failed")

	args := []any{"APPEND", key, data[0], "TTL", ttlSec}
	if q {
		args = append(args, "QUORUM")
	}
	for i := 1; i < len(data); i++ {
		args = append(args, "AND", data[i])
	}

	var r AppendResult

	cmd := redis.NewStringSliceCmd(ctx, args...)
	for _, db := range a.rdb {
		db.Process(ctx, cmd)
		err = cmd.Err()
		if err != nil {
			continue
		}

		res := cmd.Val()
		if q {
			if len(res) < 2 {
				return r, fmt.Errorf("invalid quorum response")
			}
			r.Peers, _ = strconv.Atoi(res[0])
			r.Success, _ = strconv.Atoi(res[1])
			res = res[2:]
		} else {
			r.Peers = 1
			r.Success = 1
		}

		a.mu.Lock()
		a.ids = append(a.ids, res...)
		a.mu.Unlock()

		r.KeyIDs = res
		return r, nil
	}

	return r, err
}

func (a *Session) Close() {
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

const (
	DESC     = 1
	ASC      = 2
	DISTINCT = 4
	LOCAL    = 8
	ONCE     = 16
	RAW      = 32
)

func (a *Session) Select(ctx context.Context, key string, cursor string, n int, flag int) (p []s2.Pair, err error) {
	args := []any{"SELECT", key, cursor, n}
	if flag&DESC > 0 {
		args = append(args, "DESC")
	}
	if flag&DISTINCT > 0 {
		args = append(args, "DISTINCT")
	}
	if flag&LOCAL > 0 {
		args = append(args, "LOCAL")
	}
	if flag&RAW > 0 {
		args = append(args, "RAW")
	}
	cmd := redis.NewStringSliceCmd(ctx, args...)

	for _, db := range a.rdb {
		db.Process(ctx, cmd)
		err = cmd.Err()
		if err != nil {
			if flag&ONCE > 0 {
				return nil, err
			}
			continue
		}

		res := cmd.Val()
		for i := 0; i < len(res); i += 3 {
			var x s2.Pair
			x.ID, _ = hex.DecodeString(res[i])
			x.Data = []byte(res[i+2])
			x.C = s2.ParseUint64(res[i+1])%2 == 1
			p = append(p, x)
		}
		return
	}

	return
}

func (a *Session) Set(ctx context.Context, key string, data any) (string, error) {
	ids, err := a.do(ctx, key, false, 1, data)
	if err != nil {
		return "", err
	}
	return ids.KeyIDs[0], nil
}

func (a *Session) SetQuorum(ctx context.Context, key string, data any) (AppendResult, error) {
	return a.do(ctx, key, true, 1, data)
}

func (a *Session) Get(ctx context.Context, key string) (data []byte, err error) {
	cmd := redis.NewStringCmd(ctx, "SELECT", key, "RECENT", 1, "DESC")
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
