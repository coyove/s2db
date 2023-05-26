package s2

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

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

func udpBuildPayload(tmp []byte, i, tot int, data []byte) []byte {
	tmp[0] = 0xff
	binary.BigEndian.PutUint16(tmp[1:], uint16(i))
	binary.BigEndian.PutUint16(tmp[3:], uint16(tot))
	h := sha1.Sum(data)
	copy(tmp[5:], h[:4])
	n := copy(tmp[12:], data)
	return tmp[:12+n]
}

func udpParsePayload(tmp []byte) (i, tot int, data []byte) {
	i = int(binary.BigEndian.Uint16(tmp[1:]))
	tot = int(binary.BigEndian.Uint16(tmp[3:]))
	data = tmp[12:]
	return
}

const BS = 4096

type udpLego struct {
	total []bool
	data  []byte
}

func (u *udpLego) missing() (buf []byte) {
	buf = append(buf, 0x01)
	for i := range u.total {
		if !u.total[i] {
			buf = binary.BigEndian.AppendUint16(buf, uint16(i))
		}
	}
	if len(buf) == 1 {
		return nil
	}
	return buf
}

func (u *udpLego) add(i, tot int, part []byte) {
	if len(u.total) != tot {
		u.total = make([]bool, tot)
		u.data = make([]byte, tot*BS)
	}
	u.total[i] = true
	n := copy(u.data[i*BS:], part)
	if i == tot-1 {
		u.data = u.data[:i*BS+n]
	}
}

func (u *udpLego) complete() bool {
	for i := range u.total {
		if !u.total[i] {
			return false
		}
	}
	return true
}

func RequestUDP(conn net.Conn, p []byte) ([]byte, error) {
	defer conn.Close()

	tmp := make([]byte, 65536)
	blocks := len(p) / BS
	if blocks*BS != len(p) {
		blocks++
	}

	for i := 0; i < len(p); i += BS {
		end := i + BS
		if end > len(p) {
			end = len(p)
		}
		conn.Write(udpBuildPayload(tmp, i/BS, blocks, p[i:end]))
	}

	retries := 0
	lego := &udpLego{}

	for {
		if retries == 0 {
			conn.SetReadDeadline(time.Now().Add(time.Second))
		} else {
			conn.SetReadDeadline(time.Now().Add(time.Millisecond))
		}
		n, err := conn.Read(tmp)
		if err != nil {
			if err.(net.Error).Timeout() {
				retries++
				conn.Write(lego.missing())
				continue
			}
			return nil, err
		}

		resp := tmp[:n]
		switch resp[0] {
		case 0x01:
			for i := 1; i < len(resp); i += 2 {
				idx := int(binary.BigEndian.Uint16(resp[i:]))
				end := idx*BS + BS
				if end > len(p) {
					end = len(p)
				}
				conn.Write(udpBuildPayload(tmp, idx, blocks, p[i:end]))
			}
			retries++
		case 0xff:
			i, tot, data := udpParsePayload(resp)
			lego.add(i, tot, data)
			if lego.complete() {
				return lego.data, nil
			}
		}
	}
}
