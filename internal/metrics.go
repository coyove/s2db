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
	Value [surveyCount]int64
	Count [surveyCount]int32
	Ts    [surveyCount]uint32
}

func (s *Survey) _i() (uint64, uint32) {
	ts := time.Now().Unix()
	sec := uint64(ts) / surveyIntervalSec
	return sec % surveyCount, uint32(sec) * surveyIntervalSec
}

func (s *Survey) Incr(v int64) {
	idx, ts := s._i()
	oldValue := atomic.LoadInt64(&s.Value[idx])
	oldCount := atomic.LoadInt32(&s.Count[idx])
	oldTs := atomic.LoadUint32(&s.Ts[idx])
	if oldTs != ts {
		if atomic.CompareAndSwapUint32(&s.Ts[idx], oldTs, ts) {
			atomic.AddInt64(&s.Value[idx], -oldValue)
			atomic.AddInt32(&s.Count[idx], -oldCount)
		}
	}
	atomic.AddInt64(&s.Value[idx], v)
	atomic.AddInt32(&s.Count[idx], 1)
}

func (s Survey) String() string {
	return s.QPSString()
}

func (s Survey) QPSString() string {
	q, _ := s.Metrics()
	return fmt.Sprintf("%.2f %.2f %.2f", q[0], q[1], q[2])
}

func (s Survey) MeanString() string {
	_, m := s.Metrics()
	return fmt.Sprintf("%.2f %.2f %.2f", m[0], m[1], m[2])
}

func (s Survey) GoString() string {
	return "qps: " + s.String() + " mean: " + s.MeanString()
}

// Metrics returns metrics grouped in 1 min, 5 mins, 60 mins
func (s *Survey) Metrics() (qps, mean [3]float64) {
	idx, ts := s._i()
	var value, count int64

	for i := uint64(1); i < surveyCount; i++ {
		switch i {
		case 60/surveyIntervalSec + 1:
			qps[0] = float64(count) / 60
			mean[0] = float64(value) / float64(count)
		case 300/surveyIntervalSec + 1:
			qps[1] = float64(count) / 300
			mean[1] = float64(value) / float64(count)
		}
		ii := (idx - i + surveyCount) % surveyCount
		if ts-s.Ts[ii] <= surveyRangeSec {
			value += s.Value[ii]
			count += int64(s.Count[ii])
		}
		if i == surveyCount-1 {
			qps[2] = float64(count) / surveyRangeSec
			mean[2] = float64(value) / float64(count)
		}
	}
	return
}
