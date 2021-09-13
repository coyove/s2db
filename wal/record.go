package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

const entries = 262144

type WALBlock struct {
	mu    sync.RWMutex
	start int64
	f     *os.File
}

func (w *WALBlock) Write(idx int64, p []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	internalIdx := idx - w.start
	if internalIdx < 0 || internalIdx >= entries {
		return fmt.Errorf("out of range: %d, expectind %d-%d", idx, w.start, w.start+entries)
	}
	if len(p) > 0xffffff { // 3 bytes
		return fmt.Errorf("payload too large: %d", len(p))
	}
	if idx < 0 || idx > 0xffffffffffffff { // 7 bytes
		return fmt.Errorf("invalid index")
	}
	off, err := w.f.Seek(0, 2)
	if err != nil {
		return err
	}
	if off > 0xffffffffff { // 5 bytes
		return fmt.Errorf("offset too large: %x", off)
	}
	if off < entries*16 {
		return fmt.Errorf("offset too small: %x", off)
	}

	buf := [16]byte{}
	binary.BigEndian.PutUint64(buf[:], uint64(idx))
	binary.BigEndian.PutUint64(buf[8:], uint64(off)<<24|uint64(len(p)))

	n, err := w.f.Write(p)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	if _, err := w.f.Seek(internalIdx*16, 0); err != nil {
		return err
	}
	return nil
}
