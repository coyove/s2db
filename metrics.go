package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"math"
	"net/url"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/clock"
	"github.com/coyove/s2db/ranges"
	"github.com/coyove/s2db/s2pkg"
	client "github.com/influxdata/influxdb1-client"
	log "github.com/sirupsen/logrus"
)

var (
	influxdb1MetricsEndpoint = flag.String("metrics.influxdb1", "", "")
	influxdb1Client          struct {
		*client.Client
		Database string
	}
)

func (s *Server) appendMetricsPairs(ttl time.Duration) error {
	var pairs []s2pkg.Pair
	start := clock.Now()
	now := start.UnixNano() - int64(60*time.Second)
	pairs = append(pairs, s2pkg.Pair{Member: "Connections", Score: float64(s.Survey.Connections)})
	rv, rt := reflect.ValueOf(s.Survey), reflect.TypeOf(s.Survey)
	for i := 0; i < rv.NumField(); i++ {
		if sv, ok := rv.Field(i).Interface().(s2pkg.Survey); ok {
			m, n := sv.Metrics(), rt.Field(i)
			t := n.Tag.Get("metrics")
			if t == "mean" || t == "" {
				pairs = append(pairs,
					s2pkg.Pair{Member: n.Name + "_Mean", Score: m.Mean[0]},
					s2pkg.Pair{Member: n.Name + "_Max", Score: float64(m.Max[0])},
				)
			}
			if t == "qps" || t == "" {
				pairs = append(pairs, s2pkg.Pair{Member: n.Name + "_QPS", Score: m.QPS[0]})
			}
		}
	}
	for _, m := range []*sync.Map{&s.Survey.Command, &s.Survey.ReverseProxy} {
		m.Range(func(k, v interface{}) bool {
			m, n := v.(*s2pkg.Survey).Metrics(), "Cmd"+k.(string)
			pairs = append(pairs,
				s2pkg.Pair{Member: n + "_Mean", Score: m.Mean[0]},
				s2pkg.Pair{Member: n + "_QPS", Score: m.QPS[0]},
				s2pkg.Pair{Member: n + "_Max", Score: float64(m.Max[0])},
			)
			return true
		})
	}
	pairs = append(pairs, s2pkg.Pair{Member: "Goroutines", Score: float64(runtime.NumGoroutine())})
	pairs = append(pairs, s2pkg.Pair{Member: "SysReadRTTP99", Score: s.Survey.SysReadRTTP99Micro.P99() / 1e3})
	pairs = append(pairs, s2pkg.Pair{Member: "SysReadP99", Score: s.Survey.SysReadP99Micro.P99() / 1e3})

	lsmMetrics := s.DB.Metrics()
	dbm := reflect.ValueOf(lsmMetrics).Elem()
	rt = dbm.Type()
	for i := 0; i < dbm.NumField(); i++ {
		switch f := dbm.Field(i); f.Kind() {
		case reflect.Struct:
			rft := f.Type()
			for ii := 0; ii < f.NumField(); ii++ {
				pairs = append(pairs, s2pkg.Pair{Member: "DB_" + rt.Field(i).Name + "_" + rft.Field(ii).Name, Score: rvToFloat64(f.Field(ii))})
			}
		case reflect.Array:
		default:
			pairs = append(pairs, s2pkg.Pair{Member: "DB_" + rt.Field(i).Name, Score: rvToFloat64(f)})
		}
	}
	for lv, lvm := range lsmMetrics.Levels {
		rv := reflect.ValueOf(lvm)
		for i := 0; i < rv.NumField(); i++ {
			pairs = append(pairs, s2pkg.Pair{Member: "DB_Level" + strconv.Itoa(lv) + "_" + rv.Type().Field(i).Name,
				Score: rvToFloat64(rv.Field(i))})
		}
	}

	for _, ep := range strings.Split(s.ServerConfig.MetricsEndpoint, ",") {
		switch ep {
		case "-", "null", "none":
		default:
			b := s.DB.NewBatch()
			defer b.Close()
			for _, mp := range pairs {
				key := []byte("metrics_" + mp.Member + "\x00")
				if err := b.Set(appendUint(key, uint64(now)), s2pkg.FloatToBytes(mp.Score), pebble.Sync); err != nil {
					return err
				}
				if clock.Rand() <= 0.01 {
					if err := b.DeleteRange(key, appendUint(key, uint64(now)-uint64(ttl)), pebble.Sync); err != nil {
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
			pairs = append(pairs, s2pkg.Pair{Member: "Heartbeat", Score: 1})
			tags := map[string]string{"ServerName": s.ServerName}
			points := make([]client.Point, 0, len(pairs))
			for _, p := range pairs {
				points = append(points, client.Point{
					Measurement: "s2db." + p.Member,
					Tags:        tags,
					Fields:      map[string]interface{}{"value": p.Score, "count": 1},
					Time:        start.Add(-time.Minute),
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

	if diff := time.Since(start); diff.Milliseconds() > int64(s.SlowLimit) {
		slowLogger.Infof("#%d\t% 4.3f\t%s\t%v", 0, diff.Seconds(), "127.0.0.1", "metrics")
	}

	return nil
}

func (s *Server) ListMetricsNames() (names []string) {
	key := []byte("metrics_")
	c := ranges.NewPrefixIter(s.DB, key)
	defer c.Close()
	for c.First(); c.Valid() && bytes.HasPrefix(c.Key(), key); c.Next() {
		k := c.Key()[8:]
		k = k[:bytes.IndexByte(k, 0)]
		names = append(names, string(k))
		c.SeekLT(append(append(key, k...), 0xff))
	}
	return
}

func (s *Server) GetMetricsPairs(startNano, endNano int64, names ...string) (m []s2pkg.GroupedMetrics, err error) {
	if endNano == 0 && startNano == 0 {
		startNano, endNano = clock.UnixNano()-int64(time.Hour), clock.UnixNano()
	}
	res := map[string]s2pkg.GroupedMetrics{}
	getter := func(f string) {
		key := []byte("metrics_" + f + "\x00")
		c := ranges.NewPrefixIter(s.DB, key)
		defer c.Close()

		for c.First(); c.Valid() && bytes.HasPrefix(c.Key(), key); c.Next() {
			ts := int64(binary.BigEndian.Uint64(c.Key()[len(key):]))
			if ts >= startNano && ts <= endNano {
				a := res[f]
				a.Name = f
				vf := s2pkg.BytesToFloat(c.Value())
				if math.IsNaN(vf) {
					vf = 0
				}
				tsMin := ts / 1e9 / 60 * 60
				if len(a.Timestamp) > 0 && a.Timestamp[len(a.Timestamp)-1] == tsMin {
					a.Value[len(a.Value)-1] = vf
				} else {
					a.Value = append(a.Value, vf)
					a.Timestamp = append(a.Timestamp, tsMin)
				}
				res[f] = a
			}
			if ts > endNano {
				break
			}
		}
	}
	for _, n := range names {
		getter(n)
	}
	return fillMetricsHoles(res, names, startNano, endNano), err
}

func fillMetricsHoles(res map[string]s2pkg.GroupedMetrics, names []string, startNano, endNano int64) (m []s2pkg.GroupedMetrics) {
	mints, maxts := startNano/1e9/60*60, endNano/1e9/60*60
	for _, name := range names {
		p := res[name]
		for c, ts := 0, mints; ts <= maxts; ts += 60 {
			if c >= len(p.Timestamp) {
				p.Timestamp = append(p.Timestamp, ts)
				p.Value = append(p.Value, 0)
			} else if p.Timestamp[c] != ts {
				p.Timestamp = append(p.Timestamp[:c], append([]int64{ts}, p.Timestamp[c:]...)...)
				p.Value = append(p.Value[:c], append([]float64{0}, p.Value[c:]...)...)
			}
			c++
		}
		m = append(m, p)
	}
	return m
}

func (s *Server) DeleteMetrics(name string) error {
	return s.DB.DeleteRange([]byte("metrics_"+name+"\x00"), []byte("metrics_"+name+"\x01"), pebble.Sync)
}

func (s *Server) MetricsCommand(key string) interface{} {
	var sv *s2pkg.Survey
	if rv := reflect.ValueOf(&s.Survey).Elem().FieldByName(key); rv.IsValid() {
		switch v := rv.Addr().Interface().(type) {
		case *s2pkg.Survey:
			sv = v
		case *s2pkg.P99SurveyMinute:
			return v
		}
	}
	if sv == nil {
		x, ok := s.Survey.Command.Load(key)
		if !ok {
			return nil
		}
		sv = x.(*s2pkg.Survey)
	}
	return sv
}

func rvToFloat64(v reflect.Value) float64 {
	if v.Kind() >= reflect.Int && v.Kind() <= reflect.Int64 {
		return float64(v.Int())
	}
	if v.Kind() >= reflect.Uint && v.Kind() <= reflect.Uint64 {
		return float64(v.Uint())
	}
	return 0
}

func getInfluxDB1Client(endpoint string) (*client.Client, string, error) {
	end, err := url.Parse(endpoint)
	if err != nil {
		return nil, "", err
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

	c, err := client.NewClient(client.Config{
		URL:      *end,
		Username: username,
		Password: password,
		Timeout:  timeout,
	})
	if err != nil {
		return nil, "", err
	}

	if _, _, err := c.Ping(); err != nil {
		return nil, "", err
	}
	return c, db, err
}
