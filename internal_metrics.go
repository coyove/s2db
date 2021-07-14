package main

import (
	"sync"
	"time"
)

type Survey struct {
	mu   sync.Mutex
	max  int
	data [][2]float64
}

func (s *Survey) Append(v float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = append(s.data, [2]float64{v, float64(time.Now().Unix())})
	if len(s.data) > s.max {
		s.data = s.data[len(s.data)-s.max:]
	}
}

func (s *Survey) QPS() (data [][2]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) < 2 {
		return
	}

	x := s.data

	t := x[0][1]
	sum := x[0][0]

	for x = x[1:]; len(x) > 0; x = x[1:] {
		if x[0][1]-t >= 1 || len(x) == 1 {
			data = append(data, [2]float64{sum / (x[0][1] - t), t})
			t = x[0][1]
			sum = x[0][0]
		} else {
			sum += x[0][0]
		}
	}
	return
}

func (s *Survey) MinuteAvg() (data [][2]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) < 1 {
		return
	}

	x := s.data

	t := x[0][1]
	sum := x[0][0]
	count := 1.0

	for x = x[1:]; len(x) > 0; x = x[1:] {
		if x[0][1]-t >= 60 || len(x) == 1 {
			data = append(data, [2]float64{sum / count, t})
			t = x[0][1]
			count = 1
			sum = x[0][0]
		} else {
			sum += x[0][0]
		}
	}
	return
}
