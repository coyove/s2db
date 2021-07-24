package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"sort"

	"github.com/mmcloughlin/geohash"
	"github.com/secmask/go-redisproto"
	"gitlab.litatom.com/zhangzezhong/zset/calc"
	"go.etcd.io/bbolt"
)

func errorWithPrecision(bits uint) (latErr, lngErr float64) {
	b := int(bits)
	latBits := b / 2
	lngBits := b - latBits
	latErr = math.Ldexp(180.0, -latBits)
	lngErr = math.Ldexp(360.0, -lngBits)
	return
}

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
	var p = math.Pi / 180
	var c = math.Cos
	lat1 *= p
	lat2 *= p
	lon1 *= p
	lon2 *= p
	var a = 0.5 - c(lat2-lat1)/2 + c(lat1)*c(lat2)*(1-c(lon2-lon1))/2
	return 12742000 * math.Asin(math.Sqrt(a)) // 2 * R; R = 6371 km
}

func (s *Server) runGeoRadius(w *redisproto.Writer, byMember bool, name string, h [2]uint64, wm int64, weak bool, command *redisproto.Command) error {
	var p []Pair
	var count = -1
	var any bool
	var withCoord, withDist, withHash, asc, desc bool
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
		p, err = (s.geoRange(name, key, lat, long, radius, count, any))
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

		if s.canUpdateCache(name, wm) {
			s.cache.Add(&CacheItem{Key: name, CmdHash: h, Data: p})
		}
		s.weakCache.AddWeight(name, p, int64(sizePairs(p)))
	}

	if withHash {
		data := []string{}
		for _, p := range p {
			data = append(data, p.Key, ftoa(p.Score))
		}
		return w.WriteBulkStrings(data)
	}

	if withDist {
		data := []string{}
		for _, p := range p {
			data = append(data, p.Key, ftoa(geoDistHash(lat, long, uint64(p.Score))))
		}
		return w.WriteBulkStrings(data)
	}

	if withCoord {
		data := []interface{}{}
		for _, p := range p {
			lat, long := geohash.DecodeIntWithPrecision(uint64(p.Score), 52)
			data = append(data, p.Key, []interface{}{ftoa(long), ftoa(lat)})
		}
		return w.WriteObjectsSlice(data)
	}

	data := []string{}
	for _, p := range p {
		data = append(data, p.Key)
	}
	return w.WriteBulkStrings(data)
}

func (s *Server) geoRange(name, key string, lat, long float64, radius float64, count int, any bool) (pairs []Pair, err error) {
	limit := s.HardLimit
	if count > 0 && count < s.HardLimit {
		limit = count
	}

	bits := bitsForBox(radius*2, radius*2)
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
			lat, long = geohash.DecodeIntWithPrecision(uint64(bytesToFloat(tx.Bucket([]byte("zset."+name)).Get([]byte(key)))), 52)
		}

		start := geohash.EncodeIntWithPrecision(lat, long, bits) - 1
		end := start + 2
		start <<= (52 - bits)
		end <<= (52 - bits)
		startBuf, endBuf := floatToBytes(float64(start)), floatToBytes(float64(end))

		c := bk.Cursor()
		k, _ := c.Seek(startBuf)

		if any {
			for i := 0; len(pairs) < limit; i++ {
				// s := bytesToFloat(k[:8])
				// lat2, long2 := geohash.DecodeIntWithPrecision(uint64(s), 52)
				// fmt.Println(k, startBuf, endBuf, geoDist(lat, long, lat2, long2))
				if len(k) >= 8 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) < 0 {
					s := bytesToFloat(k[:8])
					lat2, long2 := geohash.DecodeIntWithPrecision(uint64(s), 52)
					if geoDist(lat, long, lat2, long2) <= radius {
						p := Pair{
							Key:   string(k[8:]),
							Score: s,
						}
						pairs = append(pairs, p)
					}
				} else {
					break
				}
				k, _ = c.Next()
			}
		} else {
			h := &GeoHeap{lat: lat, long: long}
			for i := 0; len(pairs) < s.HardLimit; i++ {
				if len(k) >= 8 && bytes.Compare(k, startBuf) >= 0 && bytes.Compare(k, endBuf) < 0 {
					s := bytesToFloat(k[:8])
					lat2, long2 := geohash.DecodeIntWithPrecision(uint64(s), 52)
					if geoDist(lat, long, lat2, long2) <= radius {
						p := Pair{
							Key:   string(k[8:]),
							Score: s,
						}
						heap.Push(h, p)
						if h.Len() > limit {
							heap.Pop(h)
						}
					}
				} else {
					break
				}
				k, _ = c.Next()
			}

			for h.Len() > 0 {
				pairs = append(pairs, heap.Pop(h).(Pair))
			}
		}
		return nil
	})
	return
}

type GeoHeap struct {
	lat, long float64
	p         []Pair
}

func (h GeoHeap) Len() int {
	return len(h.p)
}

func (h GeoHeap) Less(i, j int) bool {
	return geoDistHash(h.lat, h.long, uint64(h.p[i].Score)) > geoDistHash(h.lat, h.long, uint64(h.p[j].Score))
}

func (h GeoHeap) Swap(i, j int) {
	h.p[i], h.p[j] = h.p[j], h.p[i]
}

func (h *GeoHeap) Push(x interface{}) {
	h.p = append(h.p, x.(Pair))
}

func (h *GeoHeap) Pop() interface{} {
	old := h.p
	n := len(old)
	x := old[n-1]
	h.p = old[0 : n-1]
	return x
}
