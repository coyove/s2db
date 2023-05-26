package client

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
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
	return a.doAppend(ctx, key, false, 0, data...)
}

func (a *Session) AppendTTL(ctx context.Context, key string, ttlSec int64, data ...any) ([]string, error) {
	return a.doAppend(ctx, key, false, ttlSec, data...)
}

func (a *Session) AppendTTLSync(ctx context.Context, key string, ttlSec int64, data ...any) ([]string, error) {
	return a.doAppend(ctx, key, true, ttlSec, data...)
}

func (a *Session) doAppend(ctx context.Context, key string, q bool, ttlSec int64, data ...any) ([]string, error) {
	args := []any{"APPEND", key, data[0], "TTL", ttlSec}
	if q {
		args = append(args, "SYNC")
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
	S_DESC     = 1  // select in desc order
	S_ASC      = 2  // select in asc order
	S_DISTINCT = 4  // distinct data
	S_RAW      = 16 // select raw Pairs
)

func (a *Session) Select(ctx context.Context, key string, cursor string, n int, flag int) (p []s2.Pair, err error) {
	args := []any{"SELECT", key, cursor, n}
	if flag&S_DESC > 0 {
		args = append(args, "DESC")
	}
	if flag&S_DISTINCT > 0 {
		args = append(args, "DISTINCT")
	}
	if flag&S_RAW > 0 {
		args = append(args, "RAW")
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
			x.C = t&1 > 0
			x.Q = t&2 > 0
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

func (a *Session) HSet(ctx context.Context, key string, kvs ...any) error {
	_, err := a.doHSet(ctx, key, false, kvs)
	return err
}

func (a *Session) HSetSync(ctx context.Context, key string, kvs ...any) ([]string, error) {
	return a.doHSet(ctx, key, true, kvs)
}

func (a *Session) doHSet(ctx context.Context, key string, q bool, kvs []any) ([]string, error) {
	args := []any{"HSET", key, kvs[0], kvs[1]}
	if q {
		args = append(args, "SYNC")
	}
	for i := 2; i < len(kvs); i += 2 {
		args = append(args, "SET", kvs[i], kvs[i+1])
	}
	return a.send(ctx, redis.NewStringSliceCmd(ctx, args...), q)
}

func (a *Session) send(ctx context.Context, cmd *redis.StringSliceCmd, q bool) ([]string, error) {
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

func (a *Session) HGetAll(ctx context.Context, key string, match []byte) (data map[string]string, err error) {
	return a.doHGetAll(ctx, key, match, false)
}

func (a *Session) MustHGetAll(ctx context.Context, key string, match []byte) (data map[string]string, err error) {
	return a.doHGetAll(ctx, key, match, true)
}

func (a *Session) doHGetAll(ctx context.Context, key string, match []byte, sync bool) (data map[string]string, err error) {
	args := []any{"HGETALL", key}
	if match != nil {
		args = append(args, "MATCH", match)
	}

	for _, db := range a.rdb {
		if sync {
			if err := db.Do(ctx, "HSYNC", key).Err(); err != nil {
				continue
			}
		}
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
