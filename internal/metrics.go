package internal

import (
	"fmt"
	"sync/atomic"
	"time"
)

const (
	surveyRangeSec    = 3600
	surveyIntervalSec = 20
	surveyCount       = surveyRangeSec / surveyIntervalSec
)

type Survey struct {
	value [surveyCount]int64
	count [surveyCount]int32
	ts    [surveyCount]uint32
}

func (s *Survey) _i() (uint64, uint32) {
	ts := time.Now().Unix()
	sec := uint64(ts) / surveyIntervalSec
	return sec % surveyCount, uint32(sec) * surveyIntervalSec
}

func (s *Survey) Incr(c int64) {
	idx, ts := s._i()
	atomic.AddInt64(&s.value[idx], c)
	atomic.AddInt32(&s.count[idx], 1)
	s.ts[idx] = ts
}

func (s Survey) String() string {
	return s.QPSString()
}

func (s Survey) QPSString() string {
	q, _ := s.Calc()
	return fmt.Sprintf("%.2f %.2f %.2f", q[0], q[1], q[2])
}

func (s Survey) MeanString() string {
	_, m := s.Calc()
	return fmt.Sprintf("%.2f %.2f %.2f", m[0], m[1], m[2])
}

func (s Survey) GoString() string {
	return "qps: " + s.String() + " mean: " + s.MeanString()
}

func (s *Survey) Calc() (qps, mean [3]float64) {
	idx, ts := s._i()
	var value, count int64

	for i := uint64(0); i < surveyCount; i++ {
		switch i {
		case 60 / surveyIntervalSec:
			qps[0] = float64(count) / 60
			mean[0] = float64(value) / float64(count)
		case 300 / surveyIntervalSec:
			qps[1] = float64(count) / 300
			mean[1] = float64(value) / float64(count)
		}
		ii := (idx - i + surveyCount) % surveyCount
		if ts-s.ts[ii] < surveyRangeSec {
			value += s.value[ii]
			count += int64(s.count[ii])
		}
		if i == surveyCount-1 {
			qps[2] = float64(count) / surveyRangeSec
			mean[2] = float64(value) / float64(count)
		}
	}
	return
}
