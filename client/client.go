package client

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/coyove/sdss/future"
	"github.com/go-redis/redis/v8"
)

type Appender struct {
	rdb []*redis.Client
	ids []string
}

func Begin(peers []*redis.Client) *Appender {
	return &Appender{rdb: peers}
}

func (a *Appender) Append(ctx context.Context, key string, data ...any) ([]string, error) {
	ids, err := a.do(ctx, key, false, 0, data...)
	return ids.KeyIDs, err
}

func (a *Appender) AppendTTL(ctx context.Context, key string, ttlSec int64, data ...any) ([]string, error) {
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

func (a *Appender) AppendQuorum(ctx context.Context, key string, ttlSec int64, data ...any) (AppendResult, error) {
	return a.do(ctx, key, true, ttlSec, data...)
}

func (a *Appender) do(ctx context.Context, key string, q bool, ttlSec int64, data ...any) (AppendResult, error) {
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
		a.ids = append(a.ids, res...)
		r.KeyIDs = res
		return r, nil
	}

	return r, err
}

func (a *Appender) Close() {
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
