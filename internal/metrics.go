package internal

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const SurveyRange = 3600

type Survey struct {
	tick   sync.Once
	NoTick bool
	data   [SurveyRange]int64
	count  [SurveyRange]int32
	ts     [SurveyRange]uint32
}

func (s *Survey) _i() (uint64, uint32) {
	ts := time.Now().Unix()
	return uint64(ts) % SurveyRange, uint32(ts)
}

func (s *Survey) _decr(x uint64) uint64 {
	x--
	if x == math.MaxUint64 {
		return SurveyRange - 1
	}
	return x
}

func (s *Survey) _incr(x uint64) uint64 {
	x++
	if x >= SurveyRange {
		return 0
	}
	return x
}

func (s *Survey) dotick() {
	idx, _ := s._i()
	next := (idx + 1) % SurveyRange
	s.data[next] = 0
	s.count[next] = 0
}

func (s *Survey) Incr(c int64) {
	s.tick.Do(func() {
		if s.NoTick {
			return
		}
		go func() {
			s.dotick()
			for range time.Tick(time.Second) {
				s.dotick()
			}
		}()
	})
	idx, ts := s._i()
	next := (idx + 1) % SurveyRange
	s.data[next] = 0
	s.count[next] = 0
	atomic.AddInt64(&s.data[idx], c)
	atomic.AddInt32(&s.count[idx], 1)
	s.ts[idx] = ts
}

func (s Survey) String() string {
	q1, q5, q15 := s.QPS()
	return fmt.Sprintf("%.2f %.2f %.2f", q1, q5, q15)
}

func (s Survey) MeanString() string {
	q1, q5, q15 := s.Mean()
	return fmt.Sprintf("%.2f %.2f %.2f", q1, q5, q15)
}

func (s Survey) GoString() string {
	return "qps: " + s.String() + " mean: " + s.MeanString()
}

func (s Survey) QPS() (q1, q5, q15 float64) {
	idx, ts := s._i()
	sec := []int64{}

	for startIdx := idx; ; {
		if s.ts[idx] >= ts-SurveyRange {
			sec = append(sec, s.data[idx])
		} else {
			sec = append(sec, 0)
		}
		idx = s._decr(idx)
		if idx == s._incr(startIdx) {
			break
		}
	}

	sum := 0.0
	for i := 0; i < len(sec); i++ {
		sum += float64(sec[i])
		if i == 59 {
			q1 = sum / 60
		} else if i == 299 {
			q5 = sum / 300
		} else if i == len(sec)-1 {
			q15 = sum / 3600
		}
	}
	return
}

func (s Survey) Mean() (q1, q5, q15 float64) {
	idx, ts := s._i()
	total, count := 0.0, 0.0

	sec := 0
	for startIdx := idx; ; {
		sec++
		if s.ts[idx] >= ts-SurveyRange {
			total += float64(s.data[idx])
			count += float64(s.count[idx])
		}
		if sec == 60 {
			q1 = total / count
		} else if sec == 300 {
			q5 = total / count
		}
		idx = s._decr(idx)
		if idx == s._incr(startIdx) {
			break
		}
	}
	q15 = total / count
	return
}
