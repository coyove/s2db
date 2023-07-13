package server

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"net/url"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/s2"
	"github.com/coyove/sdss/future"
	client "github.com/influxdata/influxdb1-client"
	log "github.com/sirupsen/logrus"
)

var influxdb1Client struct {
	*client.Client
	Database string
}

type ServerSurvey struct {
	StartAt          time.Time
	Connections      int64
	FatalError       s2.Survey `metrics:"qps"`
	SysRead          s2.Survey
	SysReadP99Micro  s2.P99SurveyMinute
	SysWrite         s2.Survey
	SysWriteP99Micro s2.P99SurveyMinute
	SlowLogs         s2.Survey
	AppendSyncN      s2.Survey `metrics:"mean"`
	HSetSyncN        s2.Survey `metrics:"mean"`
	PeerOnMissingN   s2.Survey `metrics:"mean"`
	PeerOnMissing    s2.Survey
	PeerOnOK         s2.Survey `metrics:"qps"`
	AllConsolidated  s2.Survey `metrics:"qps"`
	SelectCacheHits  s2.Survey `metrics:"qps"`
	HIterCacheHits   s2.Survey `metrics:"qps"`
	AppendExpire     s2.Survey
	RangeDistinct    s2.Survey
	PeerBatchSize    s2.Survey
	PeerTimeout      s2.Survey `metrics:"qps"`
	HashMerger       s2.Survey
	HashSyncer       s2.Survey
	TTLOnce          s2.Survey `metrics:"mean"`
	DistinctOnce     s2.Survey `metrics:"mean"`
	HashSyncOnce     s2.Survey `metrics:"mean"`
	KeyHashRatio     s2.Survey `metrics:"mean"`
	PurgerDeletes    s2.Survey
	DistinctDeletes  s2.Survey
	DistinctBefore   s2.Survey
	L6WorkerProgress s2.Survey `metrics:"mean"`
	PeerLatency      sync.Map
	Command          sync.Map
}

type metricsPair struct {
	Member string
	Score  float64
}

type metricsGrouped struct {
	Name      string
	Timestamp []int64 // seconds
	Value     []float64
}

func (s *Server) appendMetricsPairs(ttl time.Duration) error {
	var pairs []metricsPair
	start := future.UnixNano()
	now := start - int64(60*time.Second)
	pairs = append(pairs,
		metricsPair{Member: "Connections", Score: float64(s.Survey.Connections)},
	)
	rv, rt := reflect.ValueOf(&s.Survey).Elem(), reflect.TypeOf(&s.Survey).Elem()
	for i := 0; i < rv.NumField(); i++ {
		if sv, ok := rv.Field(i).Interface().(s2.Survey); ok {
			m, n := sv.Metrics(), rt.Field(i)
			t := n.Tag.Get("metrics")
			if t == "mean" || t == "" {
				pairs = append(pairs,
					metricsPair{Member: n.Name + "_Mean", Score: m.Mean[0]},
					metricsPair{Member: n.Name + "_Max", Score: float64(m.Max[0])},
				)
			}
			if t == "qps" || t == "" {
				pairs = append(pairs, metricsPair{Member: n.Name + "_QPS", Score: m.QPS[0]})
			}
		}
	}
	s.Survey.PeerLatency.Range(func(k, v any) bool {
		s := v.(*s2.Survey).Metrics()
		pairs = append(pairs, metricsPair{Member: "Peer_" + k.(string) + "_Mean", Score: float64(s.Mean[0])})
		pairs = append(pairs, metricsPair{Member: "Peer_" + k.(string) + "_Max", Score: float64(s.Max[0])})
		pairs = append(pairs, metricsPair{Member: "Peer_" + k.(string) + "_QPS", Score: float64(s.QPS[0])})
		return true
	})
	s.Survey.Command.Range(func(k, v any) bool {
		m, n := v.(*s2.Survey).Metrics(), "Cmd"+k.(string)
		pairs = append(pairs,
			metricsPair{Member: n + "_Mean", Score: m.Mean[0]},
			metricsPair{Member: n + "_QPS", Score: m.QPS[0]},
			metricsPair{Member: n + "_Max", Score: float64(m.Max[0])},
		)
		return true
	})
	pairs = append(pairs, metricsPair{Member: "Goroutines", Score: float64(runtime.NumGoroutine())})
	pairs = append(pairs, metricsPair{Member: "SysReadP99", Score: s.Survey.SysReadP99Micro.P99() / 1e3})
	pairs = append(pairs, metricsPair{Member: "SysWriteP99", Score: s.Survey.SysWriteP99Micro.P99() / 1e3})
	if c := future.Chrony.Load(); c != nil {
		pairs = append(pairs, metricsPair{Member: "NTPError", Score: c.EstimatedOffsetErr})
	}

	lsmMetrics := s.DB.Metrics()
	dbm := reflect.ValueOf(lsmMetrics).Elem()
	rt = dbm.Type()
	for i := 0; i < dbm.NumField(); i++ {
		switch f := dbm.Field(i); f.Kind() {
		case reflect.Struct:
			rft := f.Type()
			for ii := 0; ii < f.NumField(); ii++ {
				pairs = append(pairs, metricsPair{Member: "DB_" + rt.Field(i).Name + "_" + rft.Field(ii).Name, Score: rvToFloat64(f.Field(ii))})
			}
		case reflect.Array:
		default:
			pairs = append(pairs, metricsPair{Member: "DB_" + rt.Field(i).Name, Score: rvToFloat64(f)})
		}
	}
	for lv, lvm := range lsmMetrics.Levels {
		rv := reflect.ValueOf(lvm)
		for i := 0; i < rv.NumField(); i++ {
			pairs = append(pairs, metricsPair{Member: "DB_Level" + strconv.Itoa(lv) + "_" + rv.Type().Field(i).Name,
				Score: rvToFloat64(rv.Field(i))})
		}
	}

	for _, ep := range strings.Split(s.Config.MetricsEndpoint, ",") {
		switch ep {
		case "-", "null", "none":
		default:
			b := s.DB.NewBatch()
			defer b.Close()
			for _, mp := range pairs {
				key := []byte("metrics_" + mp.Member + "\x00")
				if err := b.Set(binary.BigEndian.AppendUint64(key, uint64(now)), s2.FloatToBytes(mp.Score), pebble.Sync); err != nil {
					return err
				}
				if rand.Float64() <= 0.01 {
					if err := b.DeleteRange(key, binary.BigEndian.AppendUint64(key, uint64(now)-uint64(ttl)), pebble.Sync); err != nil {
						return err
					}
				}
			}
			if err := b.Commit(pebble.Sync); err != nil {
				return err
			}
		case "influxdb1":
			if influxdb1Client.Client == nil {
				continue
			}
			pairs = append(pairs, metricsPair{Member: "Heartbeat", Score: 1})
			tags := map[string]string{"ServerName": s.Config.ServerName}
			points := make([]client.Point, 0, len(pairs))
			for _, p := range pairs {
				points = append(points, client.Point{
					Measurement: "s2db." + p.Member,
					Tags:        tags,
					Fields:      map[string]interface{}{"value": p.Score, "count": 1},
					Time:        time.Unix(0, start-int64(time.Minute)),
					Precision:   "s",
				})
			}
			resp, err := influxdb1Client.Write(client.BatchPoints{
				Points:    points,
				Database:  influxdb1Client.Database,
				Precision: "s",
			})
			if err != nil {
				log.Error("influxdb1 error: ", err)
			} else if resp != nil && resp.Err != nil {
				log.Error("influxdb1 error: ", resp.Err)
			}
		}
	}

	if diff := future.UnixNano() - start; diff/1e6 > int64(s.Config.SlowLimit) {
		slowLogger.Infof("#%d\t% 4.3f\t%s\t%v", 0, float64(diff)/1e9, "127.0.0.1", "metrics")
	}

	return nil
}

func (s *Server) ListMetricsNames() (names []string) {
	key := []byte("metrics_")
	c := newPrefixIter(s.DB, key)
	defer c.Close()
	for c.First(); c.Valid() && bytes.HasPrefix(c.Key(), key); {
		k := c.Key()[8:]
		k = k[:bytes.IndexByte(k, 0)]
		names = append(names, string(k))
		c.SeekGE(append(append(key, k...), 1))
	}
	return
}

func (s *Server) GetMetrics(names []string, startNano, endNano int64) (m []metricsGrouped) {
	if endNano == 0 && startNano == 0 {
		startNano, endNano = future.UnixNano()-int64(time.Hour), future.UnixNano()
	}
	for _, f := range names {
		tm := map[int64]float64{}
		key := []byte("metrics_" + f + "\x00")
		c := s.DB.NewIter(&pebble.IterOptions{
			LowerBound: binary.BigEndian.AppendUint64(s2.Bytes(key), uint64(startNano)),
			UpperBound: binary.BigEndian.AppendUint64(s2.Bytes(key), uint64(endNano)),
		})
		defer c.Close()

		for c.First(); c.Valid(); c.Next() {
			ts := int64(binary.BigEndian.Uint64(c.Key()[len(key):]))
			vf := s2.BytesToFloat(c.Value())
			if math.IsNaN(vf) {
				vf = 0
			}
			min := ts / 1e9 / 60 * 60
			if vold, ok := tm[min]; ok {
				tm[min] = (vf + vold) / 2
			} else {
				tm[min] = vf
			}
		}

		var g metricsGrouped
		g.Name = f
		for min := startNano / 1e9 / 60 * 60; min <= endNano/1e9/60*60; min += 60 {
			g.Timestamp = append(g.Timestamp, min)
			g.Value = append(g.Value, tm[min])
		}
		m = append(m, g)
	}
	return m
}

func (s *Server) DeleteMetrics(name string) error {
	return s.DB.DeleteRange([]byte("metrics_"+name+"\x00"), []byte("metrics_"+name+"\x01"), pebble.Sync)
}

func (s *Server) getMetricsCommand(key string) interface{} {
	var sv *s2.Survey
	if rv := reflect.ValueOf(&s.Survey).Elem().FieldByName(key); rv.IsValid() {
		switch v := rv.Addr().Interface().(type) {
		case *s2.Survey:
			sv = v
		case *s2.P99SurveyMinute:
			return v
		}
	}
	if sv == nil {
		x, ok := s.Survey.Command.Load(key)
		if !ok {
			return nil
		}
		sv = x.(*s2.Survey)
	}
	return sv
}

func initInfluxDB1Client() {
	endpoint := *influxdb1MetricsEndpoint
	if endpoint == "" {
		return
	}

	end, err := url.Parse(endpoint)
	if err != nil {
		log.Errorf("invalid influxdb endpoint %q: %v", endpoint, err)
		return
	}

	var db string = "s2db"
	var username, password string
	var timeout = time.Second

	if end.User != nil {
		username = end.User.Username()
		password, _ = end.User.Password()
		end.User = nil
	}

	if to := end.Query().Get("Timeout"); to != "" {
		sec, _ := strconv.Atoi(to)
		timeout = time.Second * time.Duration(sec)
	}

	if to := end.Query().Get("DB"); to != "" {
		db = to
	}

	c, _ := client.NewClient(client.Config{
		URL:      *end,
		Username: username,
		Password: password,
		Timeout:  timeout,
	})
	if _, _, err := c.Ping(); err != nil {
		log.Errorf("failed to ping influxdb: %v", err)
		return
	}

	influxdb1Client.Client, influxdb1Client.Database = c, db
	return
}
