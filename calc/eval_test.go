package calc

import (
	"fmt"
	"math"
	"testing"
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

	fmt.Println(Eval("(day.)+1"))
	fmt.Println(Eval("now-hour"))
}
