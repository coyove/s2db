package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const entries = 262144

var ErrNotFound = fmt.Errorf("entry not found")

type walBlock struct {
	start int64
	f     *os.File
}

type WAL struct {
	openLock         sync.Mutex
	readLock         sync.Mutex
	path             string
	cachedBlockWrite *walBlock
	cachedBlockRead  *walBlock
}

func Open(path string) (*WAL, error) {
	err := os.MkdirAll(path, 0777)
	if err != nil {
		return nil, err
	}
	return &WAL{path: path}, nil
}

func (w *WAL) Write(index int64, p []byte) error {
	w.openLock.Lock()
	defer w.openLock.Unlock()

	b, err := w.openWalBlock(index/entries*entries, &w.cachedBlockWrite)
	if err != nil {
		return err
	}
	return b.Write(index, p)
}

func (w *WAL) Read(index int64) (p []byte, err error) {
	w.readLock.Lock()
	defer w.readLock.Unlock()

	b, err := w.openWalBlock(index/entries*entries, &w.cachedBlockRead)
	if err != nil {
		return nil, err
	}
	return b.Read(index)
}

func (w *WAL) Close() error {
	if w.cachedBlockWrite != nil {
		w.cachedBlockWrite.Close()
	}
	if w.cachedBlockRead != nil {
		w.cachedBlockRead.Close()
	}
	return nil
}

func (w *WAL) openWalBlock(start int64, bp **walBlock) (*walBlock, error) {
	if *bp != nil && (*bp).start == start {
		return *bp, nil
	}

	src := filepath.Join(w.path, fmt.Sprintf("%016x", start))
	if *bp != nil && *bp == w.cachedBlockRead {
		if _, err := os.Stat(src); err != nil {
			return nil, ErrNotFound
		}
	}
	f, err := os.OpenFile(src, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	end, _ := f.Seek(0, 2)
	if end == 0 {
		x := make([]byte, entries)
		for i := 0; i < 16; i++ { // write entries * 16 bytes
			n, err := f.Write(x)
			if err != nil || n != len(x) {
				return nil, fmt.Errorf("init header short write: %v", err)
			}
		}
	} else if end < entries*16 {
		return nil, fmt.Errorf("EOF too small")
	}

	if *bp != nil {
		if err := (*bp).Close(); err != nil {
			return nil, err
		}
	}

	b := &walBlock{start: start, f: f}
	*bp = b
	return b, nil
}

func (w *walBlock) Write(idx int64, p []byte) error {
	internalIdx := idx - w.start
	if internalIdx < 0 || internalIdx >= entries {
		return fmt.Errorf("out of range: %d, expecting %d-%d", idx, w.start, w.start+entries)
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
	if err != nil || n != len(p) {
		return fmt.Errorf("data short write: %v", err)
	}
	if _, err := w.f.Seek(internalIdx*16, 0); err != nil {
		return err
	}
	n, err = w.f.Write(buf[:])
	if err != nil || n != 16 {
		return fmt.Errorf("header short write: %v", err)
	}
	if _, err := w.f.Seek(internalIdx*16, 0); err != nil {
		return err
	}
	n, err = w.f.Write([]byte{0xff})
	if err != nil || n != 1 {
		return fmt.Errorf("flag short write: %v", err)
	}
	return nil
}

func (w *walBlock) Read(idx int64) (p []byte, err error) {
	internalIdx := idx - w.start
	if internalIdx < 0 || internalIdx >= entries {
		return nil, fmt.Errorf("out of range: %d, expecting %d-%d", idx, w.start, w.start+entries)
	}
	if _, err := w.f.Seek(internalIdx*16, 0); err != nil {
		return nil, err
	}
	buf := [16]byte{}
	if _, err := io.ReadFull(w.f, buf[:]); err != nil {
		return nil, err
	}
	if buf[0] != 0xff {
		return nil, ErrNotFound
	}
	buf[0] = 0
	idx2 := int64(binary.BigEndian.Uint64(buf[:]))
	if idx2 != idx {
		return nil, fmt.Errorf("index not matched: %x and %x", idx, idx2)
	}
	off := int64(binary.BigEndian.Uint64(buf[8:]) >> 24)
	sz := int64(binary.BigEndian.Uint64(buf[8:]) & 0xffffff)
	if _, err := w.f.Seek(off, 0); err != nil {
		return nil, err
	}
	p = make([]byte, sz)
	if _, err := io.ReadFull(w.f, p); err != nil {
		return nil, err
	}
	return p, nil
}

func (w *walBlock) Close() error {
	return w.f.Close()
}
