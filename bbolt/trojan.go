package bbolt

import (
	"bytes"
	"flag"
	"fmt"
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

func (tx *Tx) CollectUnreachedPages(f func(pgid)) {
	// Force loading free list if opened in ReadOnly mode.
	tx.db.loadFreelist()

	// Check if any pages are double freed.
	freed := make(map[pgid]bool)
	all := make([]pgid, tx.db.freelist.count())
	tx.db.freelist.copyall(all)
	for _, id := range all {
		freed[id] = true
	}

	// Track every reachable page.
	reachable := make(map[pgid]*page)
	reachable[0] = tx.page(0) // meta0
	reachable[1] = tx.page(1) // meta1
	if tx.meta.freelist != pgidNoFreelist {
		for i := uint32(0); i <= tx.page(tx.meta.freelist).overflow; i++ {
			reachable[tx.meta.freelist+pgid(i)] = tx.page(tx.meta.freelist)
		}
	}

	// Recursively check buckets.
	tx.checkBucket(&tx.root, reachable, freed, ch)

	// Ensure all pages below high water mark are either reachable or freed.
	for i := pgid(0); i < tx.meta.pgid; i++ {
		_, isReachable := reachable[i]
		if !isReachable && !freed[i] {
			ch <- fmt.Errorf("page %d: unreachable unfreed", int(i))
		}
	}

	// Close the channel to signal completion.
	close(ch)
}

func (tx *Tx) collectUnreachedPagesBucket(b *Bucket, reachable map[pgid]*page, freed map[pgid]bool, ch chan error) {
	// Ignore inline buckets.
	if b.root == 0 {
		return
	}

	// Check every page used by this bucket.
	b.tx.forEachPage(b.root, 0, func(p *page, _ int) {
		if p.id > tx.meta.pgid {
			ch <- fmt.Errorf("page %d: out of bounds: %d", int(p.id), int(b.tx.meta.pgid))
		}

		// Ensure each page is only referenced once.
		for i := pgid(0); i <= pgid(p.overflow); i++ {
			var id = p.id + i
			if _, ok := reachable[id]; ok {
				ch <- fmt.Errorf("page %d: multiple references", int(id))
			}
			reachable[id] = p
		}

		// We should only encounter un-freed leaf and branch pages.
		if freed[p.id] {
			ch <- fmt.Errorf("page %d: reachable freed", int(p.id))
		} else if (p.flags&branchPageFlag) == 0 && (p.flags&leafPageFlag) == 0 {
			ch <- fmt.Errorf("page %d: invalid type: %s", int(p.id), p.typ())
		}
	})

	// Check each bucket within this bucket.
	_ = b.ForEach(func(k, v []byte) error {
		if child := b.Bucket(k); child != nil {
			tx.checkBucket(child, reachable, freed, ch)
		}
		return nil
	})
}
