package main

import (
	"fmt"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

func TestMetrics(t *testing.T) {
	s := Survey{}
	// for i := range s.data {
	// 	s.data[i] = rand.Int31()
	// }
	for i := 0; i < 3; i++ {
		s.Incr(int64(i) + 1)
		time.Sleep(time.Second * 2)
	}
	fmt.Println(s.QPS())
	fmt.Println(s.data)
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
