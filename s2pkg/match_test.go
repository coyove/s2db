package s2pkg

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"testing"

	"github.com/coyove/s2db/bitmap"
	// proto "github.com/golang/protobuf/proto"
)

func TestMatch(t *testing.T) {
	if !Match("\\not{ab}abc", "abc") {
		t.Fatal()
	}
	if Match("\\not{abc}", "abc") {
		t.Fatal()
	}
	if !Match("\\not{\\suffix{ab}}", "xyzaba") {
		t.Fatal()
	}
	if Match("\\not{\\prefix{\"a}\"}}", "a}c") {
		t.Fatal()
	}
	if !Match("\\json:a.b{1}", `{"a":{"b":1}}`) {
		t.Fatal()
	}
	if Match("\\not{\\json:a.b{1}}", `{"a":{"b":1}}`) {
		t.Fatal()
	}
	if Match("\\json:a.b{\\not{\\term{bzz}} }", `{"a":{"b":"abzzzc"}}`) {
		t.Fatal()
	}
	if !Match("\\json:a.b{\\term{bzz}}", `{"a":{"b":"abzzzc"}}`) {
		t.Fatal()
	}
	if !Match("\\json:a.b{\\ge{4}}", `{"a":{"b":4}}`) {
		t.Fatal()
	}
	if Match("\\json:a.b{\\lt{3}}", `{"a":{"b":4}}`) {
		t.Fatal()
	}
	if !Match("\\or{\\prefix{ab}\\prefix{ba}}", `abc`) {
		t.Fatal()
	}
	if Match("\\or{\\prefix{ab}\\prefix{ba}}", `xyz`) {
		t.Fatal()
	}

	buf := string(bitmap.Encode(nil, 1, 103, 2005, 30007, 50009))
	if !Match("\\bm16{102,103,104,105}", string(buf)) {
		t.Fatal()
	}
	if !Match("\\bm16{50009,100000}", string(buf)) {
		t.Fatal()
	}
	if Match("\\bm16{50000}", string(buf)) {
		t.Fatal()
	}
	if Match("\\not{\\bm16{30007}}", string(buf)) {
		t.Fatal()
	}
}

func TestProtobuf(t *testing.T) {
	//s := &GoTest{}
	//s.RequiredField = &GoTestField{
	//	Label: proto.String("a"),
	//}
	//s.F_BoolRequired = proto.Bool(true)
	//s.F_Int32Required = proto.Int32(-11)
	//s.F_Int64Required = proto.Int64(-12)
	//s.F_Uint32Required = proto.Uint32(15)
	//s.F_Uint64Required = proto.Uint64(16)
	//s.F_FloatRequired = proto.Float32(17)
	//s.F_DoubleRequired = proto.Float64(18)
	//s.F_StringRequired = proto.String("19")
	//s.F_BytesRequired = []byte("101")
	//s.F_Sint32Required = proto.Int32(102)
	//s.F_Sint64Required = proto.Int64(103)
	//s.F_Sfixed32Required = proto.Int32(-104)
	//s.F_Sfixed64Required = proto.Int64(105)
	//buf, _ := proto.Marshal(s)
	//fmt.Printf("%x", buf)
	{
		buf, _ := hex.DecodeString("22030a0161500158f5ffffffffffffffff0160f4ffffffffffffffff01780f8001108d0100008841910100000000000032409a01023139aa0603313031b006cc01b806ce01c50698ffffffc9066900000000000000")
		fmt.Println(buf)
		fmt.Println(ReadProtobuf(buf, "11i"))
		fmt.Println(ReadProtobuf(buf, "15u"))
	}
}

func BenchmarkTermMatch(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if !Match(`\term{123}`, "123") {
			b.Fatal()
		}
	}
}

func BenchmarkTermMatchRegexp(b *testing.B) {
	re := regexp.MustCompile(`123`)
	for i := 0; i < b.N; i++ {
		if !re.MatchString("123") {
			b.Fatal()
		}
	}
}

func BenchmarkOrMatch(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if !Match(`\or{\match{1}\match{2}\match{??3}}`, "123") {
			b.Fatal()
		}
	}
}

func BenchmarkOrMatchRegexp(b *testing.B) {
	re := regexp.MustCompile(`(1|2|..3)`)
	for i := 0; i < b.N; i++ {
		if !re.MatchString("123") {
			b.Fatal()
		}
	}
}
