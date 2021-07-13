package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestCommandJoinSplit(t *testing.T) {
	rand.Seed(time.Now().Unix())

	for c := 0; c < 1e4; c++ {
		b := [][]byte{}
		for i := 0; i < 10; i++ {
			x := make([]byte, rand.Intn(200)+200)
			for i := range x {
				x[i] = byte(rand.Int())
			}
			b = append(b, x)
		}
		cmd2, _ := splitCommand((joinCommand(b...)))
		for i := 0; i < cmd2.ArgCount(); i++ {
			if !bytes.Equal(cmd2.Get(i), b[i]) {
				t.FailNow()
			}
		}
	}
}

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
