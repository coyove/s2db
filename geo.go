package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/coyove/s2db/redisproto"
	s2pkg "github.com/coyove/s2db/s2pkg"
	"github.com/mmcloughlin/geohash"
	"go.etcd.io/bbolt"
)

func bitsForBox(w, h float64) uint {
	long := math.Atan2(w, 6378000) / math.Pi * 180
	lat := math.Atan2(h, 6356000) / math.Pi * 180
	b1 := math.Log2(360 / long)
	b2 := math.Log2(180 / lat)
	return uint(math.Min(b1, b2) * 2)
}

func geoDistHash(lat1, lon1 float64, hash uint64) float64 {
	lat2, long2 := geohash.DecodeIntWithPrecision(hash, 52)
	return geoDist(lat1, lon1, lat2, long2)
}

func geoDist(lat1, lon1, lat2, lon2 float64) float64 {
	const p = math.Pi / 180
	lat1 *= p
	lat2 *= p
	lon1 *= p
	lon2 *= p
	a := 0.5 - math.Cos(lat2-lat1)/2 + math.Cos(lat1)*math.Cos(lat2)*(1-math.Cos(lon2-lon1))/2
	return 12742000 * math.Asin(math.Sqrt(a)) // 2 * R; R = 6371 km
}

func (s *Server) runGeoDist(w *redisproto.Writer, name string, command *redisproto.Command) error {
	from := (command.At(2))
	to := (command.At(3))
	dist := math.NaN()
	err := s.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset." + name))
		if bk == nil {
			return nil
		}
		fromBuf := bk.Get(from)
		toBuf := bk.Get(to)
		if len(fromBuf) != 8 || len(toBuf) != 8 {
			return nil
		}
		fromLat, fromLong := geohash.DecodeIntWithPrecision(uint64(s2pkg.BytesToFloat(fromBuf)), 52)
		toLat, toLong := geohash.DecodeIntWithPrecision(uint64(s2pkg.BytesToFloat(toBuf)), 52)
		dist = geoDist(fromLat, fromLong, toLat, toLong)
		return nil
	})
	if err != nil {
		return w.WriteError(err.Error())
	}
	if math.IsNaN(dist) {
		return w.WriteBulks()
	}
	if string(command.At(4)) == "km" {
		return w.WriteBulkString(s2pkg.FormatFloat(dist / 1000))
	}
	return w.WriteBulkString(s2pkg.FormatFloat(dist))
}

func (s *Server) runGeoPos(w *redisproto.Writer, name string, command *redisproto.Command) error {
	if len(command.Argv) <= 2 {
		return w.WriteError("missing memebers")
	}

	coords := make([][2]float64, len(command.Argv)-2)
	for i := range coords {
		coords[i] = [2]float64{math.NaN(), math.NaN()}
	}

	err := s.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset." + name))
		if bk == nil {
			return nil
		}
		for i := 2; i < len(command.Argv); i++ {
			fromBuf := bk.Get(command.At(i))
			if len(fromBuf) != 8 {
				continue
			}
			coords[i-2][1], coords[i-2][0] = geohash.DecodeIntWithPrecision(uint64(s2pkg.BytesToFloat(fromBuf)), 52)
		}
		return nil
	})
	if err != nil {
		return w.WriteError(err.Error())
	}
	data := []interface{}{}
	for _, c := range coords {
		if math.IsNaN(c[0]) || math.IsNaN(c[1]) {
			data = append(data, nil)
		} else {
			data = append(data, []interface{}{s2pkg.FormatFloat(c[1]), s2pkg.FormatFloat(c[0])})
		}
	}
	return w.WriteObjectsSlice(data)
}

func (s *Server) runGeoRadius(w *redisproto.Writer, byMember bool, name string, h [2]uint64, weak time.Duration, command *redisproto.Command) error {
	var p []s2pkg.Pair
	var lat, long float64
	var key string
	var err error
	var options [][]byte

	if byMember {
		key = command.Get(2)
		options = command.Argv[3:]
	} else {
		long = command.Float64(2)
		lat = command.Float64(3)
		options = command.Argv[4:]
	}

	radius := s2pkg.MustParseFloat(string(options[0]))
	switch string(options[1]) {
	case "m":
	case "km":
		radius *= 1000
	default:
		return w.WriteError("unrecognized radius unit")
	}

	flags := (redisproto.Command{Argv: options}).Flags(2)
	if v, ok := s.Cache.Get(h); ok {
		p = v.Data.([]s2pkg.Pair)
	} else if x := s.getWeakCache(h, weak); weak > 0 && x != nil {
		p = x.([]s2pkg.Pair)
	} else {
		p, err = s.geoRange(name, key, lat, long, radius, flags.COUNT, flags.ANY, flags.WITHDATA)
		if err != nil {
			return w.WriteError(err.Error())
		}

		if flags.ASC {
			sort.Slice(p, func(i, j int) bool {
				return geoDistHash(lat, long, uint64(p[i].Score)) < geoDistHash(lat, long, uint64(p[j].Score))
			})
		}

		if flags.DESC {
			sort.Slice(p, func(i, j int) bool {
				return geoDistHash(lat, long, uint64(p[i].Score)) > geoDistHash(lat, long, uint64(p[j].Score))
			})
		}

		// s.addCache(wm, name, h, p)
		s.addWeakCache(h, p, s2pkg.SizePairs(p))
	}

	if !flags.WITHHASH && !flags.WITHCOORD && !flags.WITHDIST && !flags.WITHDATA {
		var data []string
		for _, p := range p {
			data = append(data, p.Member)
		}
		return w.WriteBulkStrings(data)
	}

	var data []interface{}
	for _, p := range p {
		tmp := []interface{}{p.Member}
		if flags.WITHHASH {
			tmp = append(tmp, s2pkg.FormatFloat(p.Score))
		}
		if flags.WITHDIST {
			tmp = append(tmp, s2pkg.FormatFloat(geoDistHash(lat, long, uint64(p.Score))))
		}
		if flags.WITHCOORD {
			lat, long := geohash.DecodeIntWithPrecision(uint64(p.Score), 52)
			tmp = append(tmp, s2pkg.FormatFloat(long), s2pkg.FormatFloat(lat))
		}
		if flags.WITHDATA {
			tmp = append(tmp, string(p.Data))
		}
		data = append(data, tmp)
	}
	return w.WriteObjectsSlice(data)
}

func (s *Server) geoRange(name, key string, lat, long float64, radius float64, limit int, any, withData bool) (pairs []s2pkg.Pair, err error) {
	bits := bitsForBox(radius, radius)
	if bits > 52 {
		bits = 52
	}
	if bits == 0 {
		return nil, fmt.Errorf("radius too big")
	}

	err = s.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset.score." + name))
		if bk == nil {
			return nil
		}

		if key != "" {
			buf := tx.Bucket([]byte("zset." + name)).Get([]byte(key))
			if len(buf) != 8 {
				return fmt.Errorf("%q not found in %q", key, name)
			}
			lat, long = geohash.DecodeIntWithPrecision(uint64(s2pkg.BytesToFloat(buf)), 52)
		}

		center := geohash.EncodeIntWithPrecision(lat, long, bits)
		neigs := geohash.NeighborsIntWithPrecision(center, bits)
		rand.Shuffle(len(neigs), func(i, j int) { neigs[i], neigs[j] = neigs[j], neigs[i] })
		neigs = append([]uint64{center}, neigs...)

		h := &geoHeap{lat: lat, long: long}

		// iterate neighbours, starting at center (itself)
		for c := bk.Cursor(); len(neigs) > 0; neigs = neigs[1:] {
			start := neigs[0] << (52 - bits)
			startBuf := s2pkg.FloatToBytes(float64(start))

			end := (neigs[0] + 1) << (52 - bits)
			endBuf := s2pkg.FloatToBytes(float64(end))

			for k, v := c.Seek(startBuf); ; k, v = c.Next() {
				if len(k) >= 8 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) < 0 {
					if s := s2pkg.BytesToFloat(k[:8]); geoDistHash(lat, long, uint64(s)) <= radius {
						p := s2pkg.Pair{Member: string(k[8:]), Score: s}
						if withData {
							p.Data = append([]byte{}, v...)
						}
						if !any {
							heap.Push(h, p)
							if h.Len() > limit {
								heap.Pop(h)
							}
						} else {
							h.Push(p)
						}
					}
					if any && h.Len() >= limit {
						goto ANY_OUT
					}
					continue
				}
				break
			}
			// not enough points (any mode) or need to collect all points (normal mode), goto next neighbour
		}
	ANY_OUT:

		if any {
			pairs = h.p
		} else {
			for h.Len() > 0 {
				pairs = append(pairs, heap.Pop(h).(s2pkg.Pair)) // from farest to closest
			}
		}
		return nil
	})
	return
}

type geoHeap struct {
	lat, long float64
	p         []s2pkg.Pair
}

func (h geoHeap) Len() int {
	return len(h.p)
}

func (h geoHeap) Less(i, j int) bool {
	return geoDistHash(h.lat, h.long, uint64(h.p[i].Score)) > geoDistHash(h.lat, h.long, uint64(h.p[j].Score))
}

func (h geoHeap) Swap(i, j int) {
	h.p[i], h.p[j] = h.p[j], h.p[i]
}

func (h *geoHeap) Push(x interface{}) {
	h.p = append(h.p, x.(s2pkg.Pair))
}

func (h *geoHeap) Pop() interface{} {
	old := h.p
	n := len(old)
	x := old[n-1]
	h.p = old[0 : n-1]
	return x
}
