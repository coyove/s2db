package main

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"regexp"
	"testing"
	"time"

	s2pkg "github.com/coyove/s2db/s2pkg"
)

func TestCommandJoinSplit(t *testing.T) {
	rand.Seed(time.Now().Unix())

	for c := 0; c < 1e4; c++ {
		b := [][]byte{}
		for i := 0; i < 10; i++ {
			x := make([]byte, rand.Intn(200)+200)
			for i := range x {
				x[i] = byte(rand.Int())
			}
			b = append(b, x)
		}
		x := joinCommand(b...)
		sum32 := x[len(x)-4:]
		h := crc32.NewIEEE()
		h.Write(x[5 : len(x)-4])
		if !bytes.Equal(h.Sum(nil), sum32) {
			t.Fatal(sum32)
		}
		cmd2, _ := splitCommand(x[5 : len(x)-4])
		for i := 0; i < cmd2.ArgCount(); i++ {
			if !bytes.Equal(cmd2.At(i), b[i]) {
				t.FailNow()
			}
		}
	}
}

func TestFloatBytesComparison(t *testing.T) {
	rand.Seed(time.Now().Unix())

	do := func(k float64) {
		for i := 0; i < 1e6; i++ {
			a := rand.Float64() * k
			b := rand.Float64() * k
			s := bytes.Compare(s2pkg.FloatToBytes(a), s2pkg.FloatToBytes(b))
			if a > b && s == 1 {
			} else if a < b && s == -1 {
			} else {
				t.Fatal(a, b, s)
			}
		}

		for i := 0; i < 1e6; i++ {
			a := rand.Float64() * k
			b := -rand.Float64() * k
			s := bytes.Compare(s2pkg.FloatToBytes(a), s2pkg.FloatToBytes(b))
			if s != 1 {
				t.Fatal(a, b, s2pkg.FloatToBytes(a), s2pkg.FloatToBytes(b))
			}
		}

		for i := 0; i < 1e6; i++ {
			a := -rand.Float64() * k
			b := -rand.Float64() * k
			s := bytes.Compare(s2pkg.FloatToBytes(a), s2pkg.FloatToBytes(b))
			if a > b && s == 1 {
			} else if a < b && s == -1 {
			} else {
				t.Fatal(a, b, s)
			}
		}

		for i := 0; i < 1e6; i++ {
			a := -rand.Float64() * k
			if x := s2pkg.BytesToFloat(s2pkg.FloatToBytes(a)); math.Abs((x-a)/a) > 1e-6 {
				t.Fatal(a, x)
			}
		}
	}

	do(1)
	do(2)
	do(math.Float64frombits(0x7FEFFFFFFFFFFFFF)) // max float64 below +inf

	for i := 0; i < 1e6; i++ {
		a := rand.Float64()
		b := rand.Float64()*math.Float64frombits(0x7FEFFFFFFFFFFFFF) + 1
		if s := bytes.Compare(s2pkg.FloatToBytes(a), s2pkg.FloatToBytes(b)); s != -1 {
			t.Fatal(a, b, s)
		}
	}

	fmt.Println(s2pkg.FloatToBytes(math.Inf(1)))
	fmt.Println(s2pkg.FloatToBytes(math.Float64frombits(0x7FEFFFFFFFFFFFFF)))
	fmt.Println(s2pkg.FloatToBytes(math.Inf(-1)))
	fmt.Println(s2pkg.FloatToBytes(math.Float64frombits(1 << 63)))
	fmt.Println(s2pkg.FloatToBytes(0))

	if !bytes.Equal(s2pkg.FloatToBytes(0), s2pkg.FloatToBytes(math.Float64frombits(1<<63))) {
		t.FailNow()
	}
}

func BenchmarkGlobMatch(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s2pkg.Match("ab*e", "abccccccd")
	}
}

func BenchmarkRegexpMatch(b *testing.B) {
	b.StopTimer()
	rx := regexp.MustCompile("ab.*e")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		rx.MatchString("abccccccd")
	}
}
