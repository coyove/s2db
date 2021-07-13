package main

import (
	"fmt"
	"testing"
)

func TestCache(t *testing.T) {
	c := NewCache(100)
	c.Add(&CacheItem{CmdHash: [2]uint64{1}, Key: "a"})
	c.Add(&CacheItem{CmdHash: [2]uint64{1}, Key: "a"})
	c.Add(&CacheItem{CmdHash: [2]uint64{2}, Key: "b"})
	c.Add(&CacheItem{CmdHash: [2]uint64{3}, Key: "a"})

	c.Remove("a")
	if c.ll.Len() != 1 {
		t.Fatal(c.ll.Len())
	}

	for i := c.ll.Front(); i != nil; i = i.Next() {
		fmt.Println(i.Value.(*entry).value)
	}
}
