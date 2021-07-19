package calc

import (
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"strconv"
)

func EvalZero(in string) float64 {
	f, err := parser.ParseExpr(in)
	if err != nil {
		return 0
	}
	return evalBinary(f)
}

func Eval(in string) (float64, error) {
	f, err := parser.ParseExpr(in)
	if err != nil {
		return 0, err
	}
	return evalBinary(f), err
}

func evalBinary(in ast.Expr) float64 {
	switch in := in.(type) {
	case *ast.BinaryExpr:
		switch in.Op {
		case token.ADD:
			return evalBinary(in.X) + evalBinary(in.Y)
		case token.SUB:
			return evalBinary(in.X) - evalBinary(in.Y)
		case token.MUL:
			return evalBinary(in.X) * evalBinary(in.Y)
		case token.QUO:
			return evalBinary(in.X) / evalBinary(in.Y)
		case token.REM:
			return math.Remainder(evalBinary(in.X), evalBinary(in.Y))
		}
	case *ast.UnaryExpr:
		switch in.Op {
		case token.ADD:
			return evalBinary(in.X)
		case token.SUB:
			return -evalBinary(in.X)
		}
	case *ast.BasicLit:
		v, _ := strconv.ParseFloat(in.Value, 64)
		return v
	}
	return 0
}
