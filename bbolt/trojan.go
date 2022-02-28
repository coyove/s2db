package bbolt

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/sirupsen/logrus"
)

var (
	bboltNoSortCheck        = flag.Bool("bbolt-no-sort-check", false, "")
	bboltLimitFreeCountSize = flag.Int("bbolt-limit-freecount-size", 128, "limit the size (in KiB) of freelist")
)

// KeyN is a shortcut of Bucket.Stats().KeyN, which assumes there is no nested buckets
func (b *Bucket) KeyN() (n int) {
	b.forEachPage(func(p *page, depth int) {
		if (p.flags & leafPageFlag) != 0 {
			n += int(p.count)
		}
	})
	return
}

func (b *DB) FreelistSize() int {
	if b.freelistSize == 0 {
		b.Update(func(tx *Tx) error {
			b.freelistSize = b.freelist.size()
			return nil
		})
	}
	return b.freelistSize
}

func (b *DB) FreelistDistribution() string {
	var c bytes.Buffer
	b.Update(func(tx *Tx) error {
		for size, bm := range b.freelist.freemaps {
			c.WriteString(strconv.Itoa(int(size)) + ":" + strconv.Itoa(len(bm)) + " ")
		}
		return nil
	})
	return c.String()
}

func (b *DB) Size() int64 {
	fi, err := b.file.Stat()
	if err != nil {
		logrus.Error("database file stat: ", err)
		return -1
	}
	return fi.Size()
}

func (b *DB) Dump(path string, safeSizeMargin int) (int64, error) {
	sz := b.Size()
	if sz == -1 {
		return sz, fmt.Errorf("failed to get db size")
	}
	dumpFile, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer dumpFile.Close()
	// Re-mmap data file to a larger size, hopefully big enough so that read tx won't block writes
	if err := b.Update(func(tx *Tx) error { return b.mmap(int(sz) + safeSizeMargin) }); err != nil {
		return 0, err
	}
	if err := b.View(func(tx *Tx) error {
		tx.WriteFlag = 0x4000
		_, err := tx.WriteTo(dumpFile)
		return err
	}); err != nil {
		return 0, err
	}
	return dumpFile.Seek(0, 2)
}

func (b *DB) DumpTo(w io.Writer, safeSizeMargin int) error {
	sz := b.Size()
	if sz == -1 {
		return fmt.Errorf("failed to get db size")
	}
	// Re-mmap data file to a larger size, hopefully big enough so that read tx won't block writes
	if err := b.Update(func(tx *Tx) error { return b.mmap(int(sz) + safeSizeMargin) }); err != nil {
		return err
	}
	return b.View(func(tx *Tx) error { _, err := tx.WriteTo(w); return err })
}
