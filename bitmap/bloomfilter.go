package bitmap

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"unsafe"

	"github.com/AndreasBriese/bbloom"
)

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
	store bbloom.Bloom
	size  int
	count int
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
	b.store.Add(d)
}

func (b *Bloom) Contains(m string) bool {
	var d []byte
	*(*[2]uintptr)(unsafe.Pointer(&d)) = *(*[2]uintptr)(unsafe.Pointer(&m))
	(*(*[3]int)(unsafe.Pointer(&d)))[2] = len(m)
	return b.store.Has(d)
}

func (b *Bloom) MarshalBinary() []byte {
	*(*bbloom.Bloom)(unsafe.Pointer(&b.wrapper)) = b.store
	p := &bytes.Buffer{}

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
		b = b[1:]
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
