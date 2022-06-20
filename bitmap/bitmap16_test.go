package bitmap

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/golang/snappy"
)

func TestBitmapExistingMember(t *testing.T) {
	var e []byte
	e, _ = Add(e, 10)
	e, _ = Add(e, 10)
	res, _ := Decode(e)
	if len(res) != 1 {
		t.Fatal(res)
	}
}

func TestBitmap(t *testing.T) {
	rand.Seed(time.Now().Unix())

	check := func(buf []byte, res []uint16, m *roaring.Bitmap) {
		for _, v := range res {
			if !m.Contains(uint32(v)) {
				t.Fatal(v, m.ToArray())
			}
		}
		m.Iterate(func(v uint32) bool {
			if !Contains(buf, uint16(v)) {
				t.Fatal(v, m.ToArray())
			}
			return true
		})
		if len(res) != int(m.GetCardinality()) {
			dedup := map[uint16]bool{}
			for _, v := range res {
				if dedup[v] {
					fmt.Println(v)
				}
				dedup[v] = true
			}
			t.Fatal(len(res), m.GetCardinality())
		}
	}

	for i := 0; i < 1000; i++ {
		var v []uint16
		m := roaring.New()
		for i := 0; i < 2000; i++ {
			x := uint16(rand.Intn(2000))
			v = append(v, x)
			m.Add(uint32(x))
		}

		enc := Encode(nil, v...)
		res, _ := Decode(enc)

		check(enc, res, m)

		x := res[len(res)-1] + 1
		enc, _ = Add(enc, x)
		m.Add(uint32(x))

		res2, _ := Decode(enc)
		// fmt.Println(x, res[len(res)-10:], res2[len(res2)-10:])
		check(enc, res2, m)

		for i := 0; i < 2000; i++ {
			x := uint16(rand.Uint32())
			m.Add(uint32(x))
			enc, _ = Add(enc, x)
		}

		res3, _ := Decode(enc)
		check(enc, res3, m)

		m.Clear()
		enc = enc[:0]
		for i := 0; i < 20000; i++ {
			m.Add(uint32(i))
			enc, _ = Add(enc, uint16(i))
		}

		res4, _ := Decode(enc)
		check(enc, res4, m)
	}
}

func BenchmarkBitmap16(b *testing.B) {
	var buf []byte
	for i := 0; i < b.N; i++ {
		var v []uint16
		for i := 0; i < 2000; i++ {
			x := uint16(rand.Uint32())
			v = append(v, x)
		}
		buf = Encode(buf[:0], v...)
	}
}

func BenchmarkBitmap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := roaring.New()
		for i := 0; i < 2000; i++ {
			x := uint16(rand.Uint32())
			m.Add(uint32(x))
		}
	}
}

func BenchmarkBitmap16Add(b *testing.B) {
	var buf []byte
	var v []uint16
	for i := 0; i < 2000; i++ {
		x := uint16(rand.Uint32())
		v = append(v, x)
	}
	buf = Encode(buf[:0], v...)
	for i := 0; i < b.N; i++ {
		Add(buf, uint16(2000+i%1000))
	}
}

func BenchmarkBitmapAdd(b *testing.B) {
	m := roaring.New()
	for i := 0; i < 2000; i++ {
		x := uint16(rand.Uint32())
		m.Add(uint32(x))
	}
	buf, _ := m.MarshalBinary()
	for i := 0; i < b.N; i++ {
		m := roaring.New()
		m.UnmarshalBinary(buf)
		m.Add(uint32(2000 + i%1000))
		m.MarshalBinary()
	}
}

func TestPlay(t *testing.T) {
	var e []byte
	ts := uint16(time.Now().Unix() / 86400)
	m := roaring.New()
	for i := ts; i < ts+36; i++ {
		e, _ = Add(e, i)
		m.Add(uint32(i))
	}
	e = snappy.Encode(nil, e)
	fmt.Println(len(e), m.GetSizeInBytes())
}
