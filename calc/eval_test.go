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

	EvalZero := func(in string) float64 {
		v, _ := Eval(in)
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
	assert(EvalZero("coord(10,20*2)+1"), float64(geohash.EncodeIntWithPrecision(40, 10, 52))+1)
	assert(EvalZero("int(now) != now"), 1)
	assert(EvalZero("int(now) == int(now. / 1000)"), 1)
	assert(EvalZero("DOM * 2\n==\n "+strconv.Itoa(now.Day()*2)), 1)
	assert(EvalZero("in(2, 1, 2, 3)"), 1)
	assert(EvalZero("nin(0, 1, 2, 3)"), 1)

	fmt.Println(Eval("(day.)+1"))
	fmt.Println(Eval("now-hour"))
	fmt.Println(Eval("coord(1,2)"))
	fmt.Println(Eval("MIN == 20 && HOUR == 9"))
}

func BenchmarkEval(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Eval("HOUR == 1")
	}
}
