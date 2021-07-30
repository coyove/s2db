package bbolt

// KeyN is a shortcut of Bucket.Stats().KeyN, which assumes there is no nested buckets
func (b *Bucket) KeyN() (n int) {
	b.forEachPage(func(p *page, depth int) {
		if (p.flags & leafPageFlag) != 0 {
			n += int(p.count)
		}
	})
	return
}
