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

	in = strings.Replace(in, "\n", "", -1)
	now := time.Now().UTC()
	if strings.Contains(in, "now") {
		in = strings.Replace(in, "now.", strconv.FormatInt(now.UnixNano()/1e6, 10), -1)
		in = strings.Replace(in, "now", strconv.FormatFloat(float64(now.UnixNano())/1e9, 'f', -1, 64), -1)
	}

	in = replaceInt(in, "month.", 2592000000)
	in = replaceInt(in, "month", 2592000)
	in = replaceInt(in, "week.", 604800000)
	in = replaceInt(in, "week", 604800)
	in = replaceInt(in, "day.", 86400000)
	in = replaceInt(in, "day", 86400)
	in = replaceInt(in, "hour", 3600000)
	in = replaceInt(in, "hour", 3600)

	in = replaceInt(in, "MIN", now.Minute())
	in = replaceInt(in, "HOUR", now.Hour())
	in = replaceInt(in, "DOW", int(now.Weekday()))
	in = replaceInt(in, "DOY", int(now.YearDay()))
	in = replaceInt(in, "DOM", now.Day())
	in = replaceInt(in, "MON", int(now.Month()))

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
	intFunc = 0xffffffffffff0001 + iota
	geoHashFunc
	geoHashLossyFunc
	inFunc
	ninFunc
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
			x, y := evalBinary(in.X), evalBinary(in.Y)
			if float64(int64(x)) == x && float64(int64(y)) == y {
				return float64(int64(x) % int64(y))
			}
			return math.Remainder(x, y)
		case token.EQL:
			return bton(evalBinary(in.X) == evalBinary(in.Y))
		case token.NEQ:
			return bton(evalBinary(in.X) != evalBinary(in.Y))
		case token.GTR:
			return bton(evalBinary(in.X) > evalBinary(in.Y))
		case token.LSS:
			return bton(evalBinary(in.X) < evalBinary(in.Y))
		case token.GEQ:
			return bton(evalBinary(in.X) >= evalBinary(in.Y))
		case token.LEQ:
			return bton(evalBinary(in.X) <= evalBinary(in.Y))
		case token.LAND:
			return bton(evalBinary(in.X) != 0 && evalBinary(in.Y) != 0)
		case token.LOR:
			return bton(evalBinary(in.X) != 0 || evalBinary(in.Y) != 0)
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
		case "int":
			return math.Float64frombits(intFunc)
		case "in":
			return math.Float64frombits(inFunc)
		case "nin":
			return math.Float64frombits(ninFunc)
		default:
		}
	case *ast.CallExpr:
		switch n := math.Float64bits(evalBinary(in.Fun)); n {
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
		case intFunc:
			if len(in.Args) == 1 {
				return float64(int64(evalBinary(in.Args[0])))
			}
		case inFunc, ninFunc:
			if len(in.Args) > 2 {
				a := evalBinary(in.Args[0])
				for i := 1; i < len(in.Args); i++ {
					if a == evalBinary(in.Args[i]) {
						return bton(n == inFunc)
					}
				}
				return bton(n == ninFunc)
			}
		}
	}
	// fmt.Printf("%T", in)
	return math.NaN()
}

func bton(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func replaceInt(s, old string, new int) string {
	if strings.Contains(s, old) {
		return strings.Replace(s, old, strconv.Itoa(new), -1)
	}
	return s
}
