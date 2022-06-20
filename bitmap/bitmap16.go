package bitmap

import (
	"sort"
	"unsafe"
)

type Uint16Slice []uint16

func (x Uint16Slice) Len() int           { return len(x) }
func (x Uint16Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x Uint16Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func Encode(buf []byte, v ...uint16) []byte {
	sort.Sort(Uint16Slice(v))
	var run [32]byte
	var runCount int
	var curHigh byte
	for _, v := range v {
		hi, lo := byte(v>>8), byte(v)
		if hi != curHigh {
			if runCount > 0 {
				buf = append(buf, curHigh)
				buf = append(buf, run[:]...)
			}
			runCount = 0
			curHigh = hi
			run = [32]byte{}
		}
		bitset(&run, nil, int(lo))
		runCount++
	}
	if runCount > 0 {
		buf = append(buf, curHigh)
		buf = append(buf, run[:]...)
	}
	return buf
}

func bitset(run *[32]byte, run2 []byte, lo int) {
	if run != nil {
		(*run)[lo/8] = (*run)[lo/8] | (1 << (lo % 8))
		return
	}
	run2[lo/8] = run2[lo/8] | (1 << (lo % 8))
}

func Decode(buf []byte) (res []uint16, ok bool) {
	ok = Iterate(buf, func(v uint16) bool {
		res = append(res, v)
		return true
	})
	return res, ok
}

func Add(buf []byte, v uint16) (n []byte, ok bool) {
	if len(buf) == 0 {
		return Encode(nil, v), true
	}
	if len(buf)%33 != 0 {
		return nil, false
	}

	last := buf[len(buf)-33]
	hi, lo := byte(v>>8), byte(v)
	if last == hi {
		bitset(nil, buf[len(buf)-32:], int(lo))
		return buf, true
	}

	if last < hi {
		run := [32]byte{}
		bitset(&run, nil, int(lo))
		buf = append(buf, hi)
		buf = append(buf, run[:]...)
		return buf, true
	}

	idx := sort.Search(len(buf)/33, func(i int) bool { return buf[i*33] >= hi })
	if idx < len(buf)/33 && buf[idx*33] == hi {
		bitset(nil, buf[idx*33+1:idx*33+33], int(lo))
		return buf, true
	}

	// x, _ := Decode(buf)
	// x = append(x, v)
	// return Encode(nil, x...), true

	run := [32]byte{}
	bitset(&run, nil, int(lo))
	buf = append(buf, "012345678901234567890123456789012"...)
	copy(buf[idx*33+33:], buf[idx*33:])
	_ = append(append(buf[:idx*33], hi), run[:]...)
	return buf, true
}

func Iterate(buf []byte, f func(uint16) bool) (ok bool) {
	if len(buf)%33 != 0 {
		return false
	}
	for i := 0; i < len(buf); i += 33 {
		hi := buf[i]
		run := buf[i+1 : i+33]
		for i := 0; i < 256; i++ {
			if run[i/8]&(1<<(i%8)) > 0 {
				if !f(uint16(hi)<<8 + uint16(i)) {
					return false
				}
			}
		}
	}
	return true
}

func StringContains(buf string, v uint16) (ok bool) {
	if len(buf)%33 != 0 {
		return false
	}
	hi, lo := byte(v>>8), byte(v)
	idx := sort.Search(len(buf)/33, func(i int) bool { return buf[i*33] >= hi })
	if idx < len(buf)/33 && buf[idx*33] == hi {
		run := buf[idx*33+1 : idx*33+33]
		return run[lo/8]&(1<<(lo%8)) > 0
	}
	return false
}

func Contains(buf []byte, v uint16) (ok bool) {
	return StringContains(*(*string)(unsafe.Pointer(&buf)), v)
}
