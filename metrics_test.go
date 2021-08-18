package main

import (
	"path/filepath"
	"regexp"
	"testing"
)

func TestMetrics(t *testing.T) {
}

func BenchmarkGlobMatch(b *testing.B) {
	for i := 0; i < b.N; i++ {
		filepath.Match("ab*e", "abccccccd")
	}
}

func BenchmarkRegexpMatch(b *testing.B) {
	b.StopTimer()
	rx := regexp.MustCompile("ab.*e")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		rx.MatchString("abccccccd")
	}
}
