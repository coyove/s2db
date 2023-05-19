package client

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"unsafe"

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
	ids, err := a.doAppend(ctx, key, false, 0, data...)
	return ids.KeyIDs, err
}

func (a *Session) AppendTTL(ctx context.Context, key string, ttlSec int64, data ...any) ([]string, error) {
	ids, err := a.doAppend(ctx, key, false, ttlSec, data...)
	return ids.KeyIDs, err
}

type Result struct {
	Peers, Success int
	KeyIDs         []string
}

func (a Result) Quorum() bool {
	return a.Success >= a.Peers/2+1
}

func (a Result) String() string {
	return fmt.Sprintf("%v (ack: %d/%d)", a.KeyIDs, a.Success, a.Peers)
}

func (a *Session) AppendQuorum(ctx context.Context, key string, ttlSec int64, data ...any) (Result, error) {
	return a.doAppend(ctx, key, true, ttlSec, data...)
}

func (a *Session) doAppend(ctx context.Context, key string, q bool, ttlSec int64, data ...any) (Result, error) {
	args := []any{"APPEND", key, data[0], "TTL", ttlSec}
	if q {
		args = append(args, "QUORUM")
	}
	for i := 1; i < len(data); i++ {
		args = append(args, "AND", data[i])
	}
	return a.send(ctx, redis.NewStringSliceCmd(ctx, args...), q)
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
	DESC     = 1  // select in desc order
	ASC      = 2  // select in asc order
	DISTINCT = 4  // distinct data
	LOCAL    = 8  // select local peer data
	RAW      = 16 // select raw Pairs
	ALL      = 32 // select all peers data
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
	if flag&ALL > 0 {
		args = append(args, "ALL")
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
			x.C = s2.ParseUint64(res[i+1])%2 == 1
			p = append(p, x)
		}
		return
	}

	return
}

func (a *Session) Set(ctx context.Context, key string, data any) (string, error) {
	ids, err := a.doAppend(ctx, key, false, 1, data)
	if err != nil {
		return "", err
	}
	return ids.KeyIDs[0], nil
}

func (a *Session) SetQuorum(ctx context.Context, key string, data any) (Result, error) {
	return a.doAppend(ctx, key, true, 1, data)
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

func (a *Session) HSet(ctx context.Context, key string, kvs ...any) error {
	_, err := a.doHSet(ctx, key, false, kvs)
	return err
}

func (a *Session) HSetQuorum(ctx context.Context, key string, kvs ...any) (Result, error) {
	return a.doHSet(ctx, key, true, kvs)
}

func (a *Session) doHSet(ctx context.Context, key string, q bool, kvs []any) (Result, error) {
	args := []any{"HSET", key, kvs[0], kvs[1]}
	if q {
		args = append(args, "QUORUM")
	}
	for i := 2; i < len(kvs); i += 2 {
		args = append(args, "SET", kvs[i], kvs[i+1])
	}
	return a.send(ctx, redis.NewStringSliceCmd(ctx, args...), q)
}

func (a *Session) send(ctx context.Context, cmd *redis.StringSliceCmd, q bool) (Result, error) {
	var r Result
	err := fmt.Errorf("all failed")
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

func (a *Session) HGetAll(ctx context.Context, key string, match []byte, sync bool) (data map[string]string, err error) {
	args := []any{"HGETALL", key}
	if sync {
		args = append(args, "SYNC")
	}
	if match != nil {
		args = append(args, "MATCH", match)
	}

	for _, db := range a.rdb {
		v, err := db.Do(ctx, args...).Result()
		if err != nil {
			continue
		}
		if bulk, ok := v.(string); ok {
			bulks, err := s2.DecompressBulks(strings.NewReader(bulk))
			if err != nil {
				// Serious error, no fallback.
				return nil, err
			}
			data = make(map[string]string, len(bulks))
			for i := 0; i < len(bulks); i += 2 {
				k := *(*string)(unsafe.Pointer(&bulks[i]))
				v := *(*string)(unsafe.Pointer(&bulks[i+1]))
				data[k] = v
			}
			return data, nil
		}
		res := v.([]any)
		data = make(map[string]string, len(res))
		for i := 0; i < len(res); i += 2 {
			data[res[i].(string)] = res[i+1].(string)
		}
		return data, nil
	}
	return
}
