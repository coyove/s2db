package calc

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/mmcloughlin/geohash"
)

func Eval(in string) (float64, error) {
	v, err := strconv.ParseFloat(in, 64)
	if err == nil {
		return v, nil
	}

	old := in
	if strings.Contains(in, "now") {
		now := time.Now()
		in = strings.Replace(in, "now.", strconv.FormatInt(now.UnixNano()/1e6, 10), -1)
		in = strings.Replace(in, "now", strconv.FormatFloat(float64(now.UnixNano())/1e9, 'f', -1, 64), -1)
	}
	in = strings.Replace(in, "month.", "2592000000", -1)
	in = strings.Replace(in, "month", "2592000", -1)
	in = strings.Replace(in, "week.", "604800000", -1)
	in = strings.Replace(in, "week", "604800", -1)
	in = strings.Replace(in, "day.", "86400000", -1)
	in = strings.Replace(in, "day", "86400", -1)
	in = strings.Replace(in, "hour.", "3600000", -1)
	in = strings.Replace(in, "hour", "3600", -1)

	f, err := parser.ParseExpr(in)
	if err != nil {
		return 0, fmt.Errorf("invalid expression: %q, parser reports: %v", in, err)
	}

	v = evalBinary(f)
	if math.IsNaN(v) {
		return v, fmt.Errorf("invalid expression: %q", old)
	}
	return v, nil
}

const (
	geoHashFunc      = 0xffffffffffff0001
	geoHashLossyFunc = 0xffffffffffff0002
)

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
	case *ast.ParenExpr:
		return evalBinary(in.X)
	case *ast.Ident:
		switch in.Name {
		case "coord":
			return math.Float64frombits(geoHashFunc)
		case "coordLossy":
			return math.Float64frombits(geoHashLossyFunc)
		default:
		}
	case *ast.CallExpr:
		switch math.Float64bits(evalBinary(in.Fun)) {
		case geoHashFunc:
			if len(in.Args) == 2 {
				long := evalBinary(in.Args[0])
				lat := evalBinary(in.Args[1])
				return float64(geohash.EncodeIntWithPrecision(lat, long, 52))
			}
		case geoHashLossyFunc:
			if len(in.Args) == 2 {
				h := uint64(evalBinary(in.Args[0]))
				i := int(evalBinary(in.Args[1]))
				lat, long := geohash.DecodeIntWithPrecision(h>>i, uint(52-i))
				return float64(geohash.EncodeIntWithPrecision(lat, long, 52))
			}
		}
	}
	// fmt.Printf("%T", in)
	return math.NaN()
}
