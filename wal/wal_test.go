package wal

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestWAL(t *testing.T) {
	data := func() []byte {
		n := rand.Intn(32) + 32
		return make([]byte, n)
	}
	w, _ := Open("test")
	m := map[int][]byte{}
	for i := 0; i < 1e6; i += rand.Intn(1e3) + 1 {
		b := data()
		m[i] = b
		if err := w.Write(int64(i), b); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	fmt.Println(len(m))
}
