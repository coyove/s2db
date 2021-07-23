package main

import (
	"fmt"
	"math"

	"github.com/mmcloughlin/geohash"
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
	long := w / 6378000
	lat := h / 6356000
	b1 := math.Log2(360 / long)
	b2 := math.Log2(180 / lat)
	return uint(b1) + uint(b2)
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

func geoDist(lat1, lon1, lat2, lon2 float64) float64 {
	var p = 0.017453292519943295 // Math.PI / 180
	var c = math.Cos
	var a = 0.5 - c((lat2-lat1)*p)/2 + c(lat1*p)*c(lat2*p)*(1-c((lon2-lon1)*p))/2
	return 12742 * math.Asin(math.Sqrt(a)) // 2 * R; R = 6371 km
}

func (s *Server) geoRange(name string, lat, long float64, radius float64, count int) (pairs []Pair, err error) {
	//limit := s.HardLimit
	//if count > 0 && count < s.HardLimit {
	//	limit = count
	//}
	bits := bitsForBox(radius, radius)
	if bits > 52 {
		bits = 52
	}
	if bits == 0 {
		return nil, fmt.Errorf("radius too big")
	}
	h := geohash.EncodeIntWithPrecision(lat, long, bits)
	err = s.pick(name).View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("zset.score." + name))
		if bk == nil {
			return nil
		}
		return nil
	})
	return
}
