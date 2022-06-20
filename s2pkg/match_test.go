package s2pkg

import (
	"regexp"
	"testing"

	"github.com/coyove/s2db/bitmap"
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
