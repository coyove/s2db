package server

import (
	"testing"
)

func TestFindKV(t *testing.T) {
	assert := func(a, b string, c float64) {
		if c2 := findKV(a, b); c2 != c {
			t.Fatal(a, b, "=>", c2, "!=", c)
		}
	}
	assert("a", "b", 0)
	assert("ab=1", "b", 1)
	assert("A=0BC=12.5c", "bc", 12.5)
	assert("A=0BC=12.625c", "bc", 12.625)
	assert("A=0BC=-12.625.c", "bc", -12.625)
	assert("A=0,BC=12.389.c=4", ".c", 4)
}

func BenchmarkFindKV(b *testing.B) {
	for i := 0; i < b.N; i++ {
		findKV("A=0BC=12.625c", "bc")
	}
}
