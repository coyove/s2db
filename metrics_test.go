package main

import (
	"fmt"
	"sync"
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

func BenchmarkChan(b *testing.B) {
	c := make(chan chan bool)
	go func() {
		for x := range c {
			x <- true
		}
	}()
	x := make(chan bool)
	for i := 0; i < b.N; i++ {
		c <- x
		<-x
	}
}

func BenchmarkChan2(b *testing.B) {
	c := make(chan *sync.WaitGroup)
	go func() {
		for x := range c {
			x.Done()
		}
	}()
	x := &sync.WaitGroup{}
	for i := 0; i < b.N; i++ {
		x.Add(1)
		c <- x
		x.Wait()
	}
}
