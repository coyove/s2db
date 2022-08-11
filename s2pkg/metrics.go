package s2pkg

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

type metrics struct {
	QPS  [3]float64
	Mean [3]float64
	Max  [3]int64
}

type Survey struct {
	Max   [surveyCount]int64
	Value [surveyCount]int64
	Count [surveyCount]int32
	Ts    [surveyCount]uint32
}

func (s *Survey) _i() (uint64, uint32) {
	ts := time.Now().Unix()
	sec := uint64(ts) / surveyIntervalSec
	return sec % surveyCount, uint32(sec) * surveyIntervalSec
}

func (s *Survey) Incr(v int64) (int64, int32) {
	idx, ts := s._i()
	oldValue := atomic.LoadInt64(&s.Value[idx])
	oldCount := atomic.LoadInt32(&s.Count[idx])
	oldTs := atomic.LoadUint32(&s.Ts[idx])
	if oldTs != ts {
		if atomic.CompareAndSwapUint32(&s.Ts[idx], oldTs, ts) {
			atomic.AddInt64(&s.Value[idx], -oldValue)
			atomic.AddInt32(&s.Count[idx], -oldCount)

			// Changing "Max" directly will yield incorrect result as other goroutines may call
			// Incr() with bigger values concurrently. But the overall results within longer
			// timeframes are relatively accurate because multiple "Max"s will be aggregated.
			s.Max[idx] = v
		}
	}
	for v > s.Max[idx] {
		old := s.Max[idx]
		atomic.CompareAndSwapInt64(&s.Max[idx], old, v)
	}
	nv := atomic.AddInt64(&s.Value[idx], v)
	nc := atomic.AddInt32(&s.Count[idx], 1)
	return nv, nc
}

func (s Survey) String() string {
	return s.QPSString()
}

func (s Survey) QPSString() string {
	q := s.Metrics()
	return fmt.Sprintf("%.2f %.2f %.2f", q.QPS[0], q.QPS[1], q.QPS[2])
}

func (s Survey) MeanString() string {
	m := s.Metrics()
	return fmt.Sprintf("%.2f %.2f %.2f", m.Mean[0], m.Mean[1], m.Mean[2])
}

func (s Survey) MaxString() string {
	m := s.Metrics()
	return fmt.Sprintf("%d %d %d", m.Max[0], m.Max[1], m.Max[2])
}

func (s Survey) GoString() string {
	return "qps: " + s.QPSString() + " mean: " + s.MeanString() + " max: " + s.MaxString()
}

// Metrics returns metrics grouped in 1 min, 5 mins, 60 mins
func (s *Survey) Metrics() (m metrics) {
	idx, ts := s._i()
	var value, count int64

	for i := uint64(1); i < surveyCount; i++ {
		switch i {
		case 60/surveyIntervalSec + 1:
			m.QPS[0] = float64(count) / 60
			m.Mean[0] = float64(value) / float64(count)
		case 300/surveyIntervalSec + 1:
			m.QPS[1] = float64(count) / 300
			m.Mean[1] = float64(value) / float64(count)
		}
		ii := (idx - i + surveyCount) % surveyCount

		if ts-s.Ts[ii] <= surveyRangeSec {
			if i < 60/surveyIntervalSec+1 {
				if s.Max[ii] > m.Max[0] {
					m.Max[0], m.Max[1], m.Max[2] = s.Max[ii], s.Max[ii], s.Max[ii]
				}
			} else if i < 300/surveyIntervalSec+1 {
				if s.Max[ii] > m.Max[1] {
					m.Max[1], m.Max[2] = s.Max[ii], s.Max[ii]
				}
			} else {
				if s.Max[ii] > m.Max[2] {
					m.Max[2] = s.Max[ii]
				}
			}

			value += s.Value[ii]
			count += int64(s.Count[ii])
		}

		if i == surveyCount-1 {
			m.QPS[2] = float64(count) / surveyRangeSec
			m.Mean[2] = float64(value) / float64(count)
		}
	}
	return
}

type GroupedMetrics struct {
	Name      string
	Timestamp []int64 // seconds
	Value     []float64
}
