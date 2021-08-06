package calc

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/mmcloughlin/geohash"
)

func TestEval(t *testing.T) {
	assert := func(a, b float64) {
		if a != b {
			t.Fatal(a, b)
		}
	}

	EvalZero := func(in string, args ...float64) float64 {
		v, _ := Eval(in, args...)
		return v
	}

	now := time.Now().UTC()

	assert(EvalZero("1"), 1)
	assert(EvalZero("+1"), 1)
	assert(EvalZero("1+2"), 3)
	assert(EvalZero("1+2*3"), 7)
	assert(EvalZero("(1+2)*3"), 9)
	assert(EvalZero("-1"), -1)
	assert(EvalZero("-inf"), math.Inf(-1))
	assert(EvalZero("-Inf"), math.Inf(-1))
	assert(EvalZero("+inf"), math.Inf(1))
	assert(EvalZero("+Inf"), math.Inf(1))
	assert(EvalZero("inf"), math.Inf(1))
	assert(EvalZero("Inf"), math.Inf(1))
	assert(EvalZero("coord(10,a*2)+N", 'a', 20, 'N', 1), float64(geohash.EncodeIntWithPrecision(40, 10, 52))+1)
	assert(EvalZero("int(now) != now"), 1)
	assert(EvalZero("int(now) == int(now. / 1000)"), 1)
	assert(EvalZero("DOM * 2\n==\n "+strconv.Itoa(now.Day()*2)), 1)
	assert(EvalZero("in(2, 1, 2, 3)"), 1)
	assert(EvalZero("nin(0, 1, 2, 3)"), 1)
	assert(EvalZero("!nin(2, 1, 2, 3)"), 1)
	assert(EvalZero("s == 1", 's', 1), 1)
	assert(EvalZero("if(s == 20, s + 1, s -1", 's', 20), 21)
	assert(EvalZero("hr(10+8)"), 18)
	assert(EvalZero("hr(18+8)"), 2)
	assert(EvalZero("hr(2-10)"), 16)

	fmt.Println(Eval("(day.)+1"))
	fmt.Println(Eval("now-hour"))
	fmt.Println(Eval("coord(1,2)"))
	fmt.Println(Eval("MIN == 20 && HOUR == 9"))
}

func BenchmarkEval(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Eval("HOUR == a", 'a', 1)
	}
}
