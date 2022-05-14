package s2pkg

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
)

type BufioConn struct {
	net.Conn
	*bufio.Reader
	ctr *int64
}

func NewBufioConn(conn net.Conn, ctr *int64) BufioConn {
	atomic.AddInt64(ctr, 1)
	return BufioConn{conn, bufio.NewReader(conn), ctr}
}

func (bc BufioConn) Read(p []byte) (int, error) {
	return bc.Reader.Read(p)
}

func (bc BufioConn) Close() error {
	atomic.AddInt64(bc.ctr, -1)
	return bc.Conn.Close()
}

type LocalListener struct {
	c chan net.Conn
}

func NewLocalListener() *LocalListener {
	return &LocalListener{c: make(chan net.Conn)}
}

func (ll *LocalListener) Feed(conn BufioConn) {
	ll.c <- conn
}

func (ll *LocalListener) Accept() (net.Conn, error) {
	c := <-ll.c
	if c == nil {
		return nil, fmt.Errorf("listener closed")
	}
	return c, nil
}

func (ll *LocalListener) Close() error {
	defer Recover(nil)
	close(ll.c)
	return nil
}

func (ll *LocalListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
}

func IsRemoteOfflineError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "refused") || strings.Contains(err.Error(), "i/o timeout")
}
