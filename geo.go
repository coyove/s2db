package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sort"

	"github.com/mmcloughlin/geohash"
	"github.com/secmask/go-redisproto"
	"gitlab.litatom.com/zhangzezhong/zset/calc"
	"go.etcd.io/bbolt"
)

func bitsForBox(w, h float64) uint {
	long := math.Atan2(w, 6378000) / math.Pi * 180
	lat := math.Atan2(h, 6356000) / math.Pi * 180
	b1 := math.Log2(360 / long)
	b2 := math.Log2(180 / lat)
	return uint(math.Min(b1, b2) * 2)
}

func init() {
	// // h := geohash.EncodeIntWithPrecision(10, 20, 52)
	// fmt.Println(bitsForBox(1000, 1000))
	// for i := 0; i < 52; i++ {
	// 	// fmt.Println(geohash.BoundingBoxIntWithPrecision(h>>i, 52-uint(i)))
	// 	lat, long := errorWithPrecision(52 - uint(i))
	// 	fmt.Println(52-i, lat*6371000, long*6371000)
	// }
	// panic(1)
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
	from := (command.Get(2))
	to := (command.Get(3))
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
		fromLat, fromLong := geohash.DecodeIntWithPrecision(uint64(bytesToFloat(fromBuf)), 52)
		toLat, toLong := geohash.DecodeIntWithPrecision(uint64(bytesToFloat(toBuf)), 52)
		dist = geoDist(fromLat, fromLong, toLat, toLong)
		return nil
	})
	if err != nil {
		return w.WriteError(err.Error())
	}
	if math.IsNaN(dist) {
		return w.WriteBulks()
	}
	if string(command.Get(4)) == "km" {
		return w.WriteBulkString(ftoa(dist / 1000))
	}
	return w.WriteBulkString(ftoa(dist))
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
			fromBuf := bk.Get(command.Get(i))
			if len(fromBuf) != 8 {
				continue
			}
			coords[i-2][1], coords[i-2][0] = geohash.DecodeIntWithPrecision(uint64(bytesToFloat(fromBuf)), 52)
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
			data = append(data, []interface{}{ftoa(c[1]), ftoa(c[0])})
		}
	}
	return w.WriteObjectsSlice(data)
}

func (s *Server) runGeoRadius(w *redisproto.Writer, byMember bool, name string, h [2]uint64, wm int64, weak bool, command *redisproto.Command) error {
	var p []Pair
	var count = -1
	var any bool
	var withCoord, withDist, withHash, withData, asc, desc bool
	var lat, long float64
	var key string
	var err error
	options := command.Argv[4:]

	if byMember {
		key = string(command.Get(2))
		options = command.Argv[3:]
	} else {
		long, err = calc.Eval(string(command.Get(2)))
		if err != nil {
			return w.WriteError(err.Error())
		}
		lat, err = calc.Eval(string(command.Get(3)))
		if err != nil {
			return w.WriteError(err.Error())
		}
	}

	radius, err := calc.Eval(string(options[0]))
	if err != nil {
		return w.WriteError(err.Error())
	}
	switch string(options[1]) {
	case "m":
	case "km":
		radius *= 1000
	default:
		return w.WriteError("unrecognized radius unit")
	}

	options = append(options, nil)
	for i := 2; i < len(options)-1; i++ {
		if bytes.EqualFold(options[i], []byte("COUNT")) {
			count = atoip(string(options[i+1]))
			i++
			if bytes.EqualFold(options[i+1], []byte("ANY")) {
				any = true
				i++
			}
		} else if bytes.EqualFold(options[i], []byte("WITHCOORD")) {
			withCoord = true
		} else if bytes.EqualFold(options[i], []byte("WITHDIST")) {
			withDist = true
		} else if bytes.EqualFold(options[i], []byte("WITHHASH")) {
			withHash = true
		} else if bytes.EqualFold(options[i], []byte("WITHDATA")) {
			withData = true
		} else if bytes.EqualFold(options[i], []byte("ASC")) {
			asc = true
		} else if bytes.EqualFold(options[i], []byte("DESC")) {
			desc = true
		}
	}

	if v, ok := s.cache.Get(h); ok {
		p = v.Data.([]Pair)
	} else if x := s.getWeakCache(h); weak && x != nil {
		p = x.([]Pair)
	} else {
		p, err = (s.geoRange(name, key, lat, long, radius, count, any, withData))
		if err != nil {
			return w.WriteError(err.Error())
		}

		if asc {
			sort.Slice(p, func(i, j int) bool {
				return geoDistHash(lat, long, uint64(p[i].Score)) < geoDistHash(lat, long, uint64(p[j].Score))
			})
		}

		if desc {
			sort.Slice(p, func(i, j int) bool {
				return geoDistHash(lat, long, uint64(p[i].Score)) > geoDistHash(lat, long, uint64(p[j].Score))
			})
		}

		s.addCache(wm, name, h, p)
		s.weakCache.AddWeight(name, p, int64(sizePairs(p)))
	}

	if !withHash && !withCoord && !withDist && !withData {
		data := []string{}
		for _, p := range p {
			data = append(data, p.Key)
		}
		return w.WriteBulkStrings(data)
	}

	data := []interface{}{}
	for _, p := range p {
		tmp := []interface{}{p.Key}
		if withHash {
			tmp = append(tmp, ftoa(p.Score))
		}
		if withDist {
			tmp = append(tmp, ftoa(geoDistHash(lat, long, uint64(p.Score))))
		}
		if withCoord {
			lat, long := geohash.DecodeIntWithPrecision(uint64(p.Score), 52)
			tmp = append(tmp, ftoa(long), ftoa(lat))
		}
		if withData {
			tmp = append(tmp, string(p.Data))
		}
		data = append(data, tmp)
	}
	return w.WriteObjectsSlice(data)
}

func (s *Server) geoRange(name, key string, lat, long float64, radius float64, count int, any, withData bool) (pairs []Pair, err error) {
	limit := s.HardLimit
	if count > 0 && count < s.HardLimit {
		limit = count
	}

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
			lat, long = geohash.DecodeIntWithPrecision(uint64(bytesToFloat(buf)), 52)
		}

		center := geohash.EncodeIntWithPrecision(lat, long, bits)
		neigs := geohash.NeighborsIntWithPrecision(center, bits)
		rand.Shuffle(len(neigs), func(i, j int) { neigs[i], neigs[j] = neigs[j], neigs[i] })
		neigs = append([]uint64{center}, neigs...)

		h := &geoHeap{lat: lat, long: long}

		// iterate neighbours, starting at center (itself)
		for c := bk.Cursor(); len(neigs) > 0; neigs = neigs[1:] {
			start := neigs[0] << (52 - bits)
			startBuf := floatToBytes(float64(start))

			end := (neigs[0] + 1) << (52 - bits)
			endBuf := floatToBytes(float64(end))

			for k, v := c.Seek(startBuf); ; k, v = c.Next() {
				if len(k) >= 8 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) < 0 {
					if s := bytesToFloat(k[:8]); geoDistHash(lat, long, uint64(s)) <= radius {
						p := Pair{Key: string(k[8:]), Score: s}
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
				pairs = append(pairs, heap.Pop(h).(Pair)) // from farest to closest
			}
		}
		return nil
	})
	return
}

type geoHeap struct {
	lat, long float64
	p         []Pair
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
	h.p = append(h.p, x.(Pair))
}

func (h *geoHeap) Pop() interface{} {
	old := h.p
	n := len(old)
	x := old[n-1]
	h.p = old[0 : n-1]
	return x
}
