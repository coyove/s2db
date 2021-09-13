package main

import (
	"bytes"
	"compress/flate"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/coyove/s2db/redisproto"
	"go.etcd.io/bbolt"
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
		if x[0] == 1 {
			t.FailNow()
		}
		cmd2, _ := splitCommand(string(x))
		for i := 0; i < cmd2.ArgCount(); i++ {
			if !bytes.Equal(cmd2.At(i), b[i]) {
				t.FailNow()
			}
		}
	}

	for c := 0; c < 1e4; c++ {
		b := [][]byte{}
		s := []string{}
		for i := 0; i < 10; i++ {
			x := make([]byte, rand.Intn(200)+200)
			for i := range x {
				x[i] = byte(rand.Int())
			}
			b = append(b, x)
			s = append(s, string(x))
		}
		cmd1 := string(joinCommand(b...))
		cmd2 := string(joinCommandString(s...))
		if cmd1 != cmd2 {
			t.Fatal(cmd1, cmd2)
		}
	}
}

func TestBBolt(t *testing.T) {
	c := dumpCommand(&redisproto.Command{
		Argv: [][]byte{
			[]byte("QAPPEND 'love130421010922294:love131163602688147' '4SjC3qQOJSfE_0FJF-O9Jv7wldWydVAScbX2_4YekwhodpJKYHo_Idm2TpfEgleC8jLq4JTXkqO5bBC0jzFgkLYECL3qeyPVN7EP393AKZ7f8YOgn0WABZLT6nrBKaEdEoxzsqq6Vcmhx9CidR8Kkqco8SPABuYDLBI4YTLAVKZYKng75bMZgaFiJhrbGfaHGnJxJ5CbLk4nYXRN6Brl7vtJFrUkVqPPsZ7BRd0MAbTB0_oYWtb8EufI3OGxsCzirZ1udZ9BwSjqRp352tc=' '10000'"),
		},
	})
	p := &bytes.Buffer{}
	w, _ := flate.NewWriter(p, flate.HuffmanOnly)
	w.Write(c)
	w.Flush()
	fmt.Println(p.Len(), len(c))
	return

	data := func() []byte {
		n := rand.Intn(32) + 32
		return make([]byte, n)
	}
	const N = 1e5
	const B = 1000

	{
		os.Remove("s.db")
		db, _ := bbolt.Open("s.db", 0666, bboltOptions)
		for z := 0; z < N; z += B {
			db.Update(func(tx *bbolt.Tx) error {
				off := 1000
				bk, _ := tx.CreateBucketIfNotExists([]byte("a"))
				bk.FillPercent = 0.9
				for i := 0; i < B; i++ {
					bk.Put(intToBytes(uint64(z+i)), data())
					bk.Delete(intToBytes(uint64(z + i - off)))
				}
				return nil
			})
		}
		db.Close()
		fi, _ := os.Stat("s.db")
		fmt.Println("0.9 seq", fi.Size())
	}
}

func TestFloatBytesComparison(t *testing.T) {
	rand.Seed(time.Now().Unix())

	do := func(k float64) {
		for i := 0; i < 1e6; i++ {
			a := rand.Float64() * k
			b := rand.Float64() * k
			s := bytes.Compare(floatToBytes(a), floatToBytes(b))
			if a > b && s == 1 {
			} else if a < b && s == -1 {
			} else {
				t.Fatal(a, b, s)
			}
		}

		for i := 0; i < 1e6; i++ {
			a := rand.Float64() * k
			b := -rand.Float64() * k
			s := bytes.Compare(floatToBytes(a), floatToBytes(b))
			if s != 1 {
				t.Fatal(a, b, floatToBytes(a), floatToBytes(b))
			}
		}

		for i := 0; i < 1e6; i++ {
			a := -rand.Float64() * k
			b := -rand.Float64() * k
			s := bytes.Compare(floatToBytes(a), floatToBytes(b))
			if a > b && s == 1 {
			} else if a < b && s == -1 {
			} else {
				t.Fatal(a, b, s)
			}
		}

		for i := 0; i < 1e6; i++ {
			a := -rand.Float64() * k
			if x := bytesToFloat(floatToBytes(a)); math.Abs((x-a)/a) > 1e-6 {
				t.Fatal(a, x)
			}
		}
	}

	do(1)
	do(2)
	do(math.Float64frombits(0x7FEFFFFFFFFFFFFF)) // max float64 below +inf

	fmt.Println(floatToBytes(math.Inf(1)))
	fmt.Println(floatToBytes(math.Float64frombits(0x7FEFFFFFFFFFFFFF)))
	fmt.Println(floatToBytes(math.Inf(-1)))
	fmt.Println(floatToBytes(math.Float64frombits(1 << 63)))
	fmt.Println(floatToBytes(0))

	if !bytes.Equal(floatToBytes(0), floatToBytes(math.Float64frombits(1<<63))) {
		t.FailNow()
	}
}
