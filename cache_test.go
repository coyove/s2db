package main

import (
	"bytes"
	"fmt"
	"math"
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
		cmd2, _ := splitCommand(string((joinCommand(b...))))
		for i := 0; i < cmd2.ArgCount(); i++ {
			if !bytes.Equal(cmd2.Get(i), b[i]) {
				t.FailNow()
			}
		}
	}
}

func TestCache(t *testing.T) {
	// c := NewCache(100)
	// c.Add(&CacheItem{CmdHash: [2]uint64{1}, Key: "a"})
	// c.Add(&CacheItem{CmdHash: [2]uint64{1}, Key: "a"})
	// c.Add(&CacheItem{CmdHash: [2]uint64{2}, Key: "b"})
	// c.Add(&CacheItem{CmdHash: [2]uint64{3}, Key: "a"})

	// c.Remove("a")
	// if c.ll.Len() != 1 {
	// 	t.Fatal(c.ll.Len())
	// }

	// for i := c.ll.Front(); i != nil; i = i.Next() {
	// 	fmt.Println(i.Value.(*entry).value)
	// }
}

func TestFloatBytesComparison(t *testing.T) {
	rand.Seed(time.Now().Unix())

	do := func(k float64) {
		for i := 0; i < 1e6; i++ {
			a := rand.Float64() * k
			b := rand.Float64() * k
			s := bytes.Compare(floatToBytes(a), floatToBytes(b))
			if a > b && s == 1 {
			} else if a < b && s == -1 {
			} else {
				t.Fatal(a, b, s)
			}
		}

		for i := 0; i < 1e6; i++ {
			a := rand.Float64() * k
			b := -rand.Float64() * k
			s := bytes.Compare(floatToBytes(a), floatToBytes(b))
			if s != 1 {
				t.Fatal(a, b, floatToBytes(a), floatToBytes(b))
			}
		}

		for i := 0; i < 1e6; i++ {
			a := -rand.Float64() * k
			b := -rand.Float64() * k
			s := bytes.Compare(floatToBytes(a), floatToBytes(b))
			if a > b && s == 1 {
			} else if a < b && s == -1 {
			} else {
				t.Fatal(a, b, s)
			}
		}

		for i := 0; i < 1e6; i++ {
			a := -rand.Float64() * k
			if x := bytesToFloat(floatToBytes(a)); math.Abs((x-a)/a) > 1e-6 {
				t.Fatal(a, x)
			}
		}
	}

	do(1)
	do(2)
	do(math.Float64frombits(0x7FEFFFFFFFFFFFFF)) // max float64 below +inf

	fmt.Println(floatToBytes(math.Inf(1)))
	fmt.Println(floatToBytes(math.Float64frombits(0x7FEFFFFFFFFFFFFF)))
	fmt.Println(floatToBytes(math.Inf(-1)))
	fmt.Println(floatToBytes(math.Float64frombits(1 << 63)))
	fmt.Println(floatToBytes(0))

	if !bytes.Equal(floatToBytes(0), floatToBytes(math.Float64frombits(1<<63))) {
		t.FailNow()
	}
}
