package calc

import (
	"testing"
)

func TestEval(t *testing.T) {
	assert := func(a, b float64) {
		if a != b {
			t.Fatal(a, b)
		}
	}

	assert(EvalZero("1"), 1)
	assert(EvalZero("+1"), 1)
	assert(EvalZero("1+2"), 3)
	assert(EvalZero("1+2*3"), 7)
	assert(EvalZero("-1"), -1)
}
