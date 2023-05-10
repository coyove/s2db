package s2

import (
	"errors"
	"net"
	"strings"
	"sync"
	"syscall"

	"github.com/coyove/sdss/future"
)

func GetRemoteIP(addr net.Addr) net.IP {
	switch addr := addr.(type) {
	case *net.TCPAddr:
		return addr.IP
	case *net.UnixAddr:
		return net.IPv4(127, 0, 0, 1)
	}
	return net.IPv4bcast
}

type ErrorThrottler struct {
	m sync.Map
}

func (s *ErrorThrottler) Throttle(key string, err error) bool {
	if err == nil {
		return false
	}

	switch msg := strings.ToLower(err.Error()); {
	case errors.Is(err, syscall.ECONNREFUSED):
	case errors.Is(err, syscall.ECONNRESET):
	case strings.Contains(msg, "connection refused"):
	case strings.Contains(msg, "connection reset"):
	default:
		return false
	}

	now := future.UnixNano()
	last, loaded := s.m.LoadOrStore(key, now)
	if !loaded {
		return false
	}
	if now-last.(int64) < 1e9 {
		return true
	}
	s.m.Store(key, now)
	return false
}
