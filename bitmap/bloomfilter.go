package bitmap

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"sync"
	"unsafe"

	"github.com/AndreasBriese/bbloom"
)

//go:linkname strhash runtime.strhash
func strhash(p unsafe.Pointer, h uintptr) uintptr

type Bloom struct {
	wrapper struct {
		Mtx     sync.Mutex
		ElemNum uint64
		Bitset  []uint64
		SizeExp uint64
		Size    uint64
		SetLocs uint64
		Shift   uint64
	}
	tiny32 []uint32
	store  bbloom.Bloom
	size   int
	count  int
}

func NewBloomFilter(size int, fpr float64) *Bloom {
	if size <= 0 {
		panic("BloomFilter: invalid size")
	}
	return &Bloom{
		store: bbloom.New(float64(size), fpr),
		size:  size,
	}
}

func (b *Bloom) Add(m string) {
	var d []byte
	*(*[2]uintptr)(unsafe.Pointer(&d)) = *(*[2]uintptr)(unsafe.Pointer(&m))
	(*(*[3]int)(unsafe.Pointer(&d)))[2] = len(m)
	if !b.store.Has(d) {
		b.count++
	}
	if b.count < 250 {
		if len(b.tiny32) == 0 {
			b.tiny32 = append(b.tiny32, 0xffffffff)
		}
		b.tiny32 = append(b.tiny32, uint32(strhash(unsafe.Pointer(&m), 0)))
	} else {
		b.tiny32 = nil
	}
	b.store.Add(d)
}

func (b *Bloom) Contains(m string) bool {
	if len(b.tiny32) > 0 {
		h := uint32(strhash(unsafe.Pointer(&m), 0))
		if b.tiny32[0] == 0xffffffff {
			for _, v := range b.tiny32 {
				if v == h {
					return true
				}
			}
		} else {
			i := sort.Search(len(b.tiny32), func(i int) bool { return b.tiny32[i] >= h })
			return i < len(b.tiny32) && b.tiny32[i] == h
		}
		return false
	}
	var d []byte
	*(*[2]uintptr)(unsafe.Pointer(&d)) = *(*[2]uintptr)(unsafe.Pointer(&m))
	(*(*[3]int)(unsafe.Pointer(&d)))[2] = len(m)
	return b.store.Has(d)
}

func (b *Bloom) MarshalBinary() []byte {
	*(*bbloom.Bloom)(unsafe.Pointer(&b.wrapper)) = b.store
	p := &bytes.Buffer{}

	if b.tiny32 != nil {
		sort.Slice(b.tiny32, func(i, j int) bool { return b.tiny32[i] < b.tiny32[j] })
		p.WriteByte(32)
		var dummy []byte
		this := (*[3]uintptr)(unsafe.Pointer(&dummy))
		*this = *(*[3]uintptr)(unsafe.Pointer(&b.tiny32))
		(*this)[1] *= 4
		(*this)[2] *= 4
		p.Write(dummy)
		return p.Bytes()
	}

	var w io.Writer
	var c func() error
	if b.count < b.size/8 {
		p.WriteByte(1)
		gw := gzip.NewWriter(p)
		w, c = gw, gw.Close
	} else {
		p.WriteByte(0)
		w = p
	}

	binary.Write(w, binary.BigEndian, uint64(b.wrapper.SetLocs))

	var dummy []byte
	this := (*[3]uintptr)(unsafe.Pointer(&dummy))
	*this = *(*[3]uintptr)(unsafe.Pointer(&b.wrapper.Bitset))
	(*this)[1] *= 8
	(*this)[2] *= 8

	w.Write(dummy)
	if c != nil {
		c()

		// Bad compression, high entropy
		if p.Len() > len(dummy) {
			p.Reset()
			p.WriteByte(0)
			binary.Write(p, binary.BigEndian, uint64(b.wrapper.SetLocs))
			p.Write(dummy)
		}
	}
	return p.Bytes()
}

func BloomFilterUnmarshalBinary(b []byte) (*Bloom, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("BloomFilterUnmarshalBinary: invalid data")
	}
	switch b[0] {
	case 0:
		b = append([]byte{}, b[1:]...)
	case 1:
		rd, err := gzip.NewReader(bytes.NewReader(b[1:]))
		if err != nil {
			return nil, fmt.Errorf("BloomFilterUnmarshalBinary: failed to read data: %v", err)
		}
		b, err = ioutil.ReadAll(rd)
		if err != nil {
			return nil, fmt.Errorf("BloomFilterUnmarshalBinary: failed to read data: %v", err)
		}
		rd.Close()
	case 32:
		b := append([]byte{}, b[1:]...)
		dummy := &Bloom{}
		this := (*[3]uintptr)(unsafe.Pointer(&dummy.tiny32))
		*this = *(*[3]uintptr)(unsafe.Pointer(&b))
		(*this)[1] /= 4
		(*this)[2] /= 4
		return dummy, nil
	default:
		return nil, fmt.Errorf("BloomFilterUnmarshalBinary: invalid header: %c", b[0])
	}
	if len(b) < 8 {
		return nil, fmt.Errorf("BloomFilterUnmarshalBinary: failed to read setLocs")
	}
	setLocs := binary.BigEndian.Uint64(b)
	b = b[8:]
	return &Bloom{
		store: bbloom.NewWithBoolset(&b, setLocs),
	}, nil
}
