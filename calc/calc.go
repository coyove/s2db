package calc

import (
	"bytes"
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

func Eval(in string, args ...float64) (float64, error) {
	v, err := strconv.ParseFloat(in, 64)
	if err == nil {
		return v, nil
	}

	old := in

	var depth int
	var buf bytes.Buffer
	for i := 0; i < len(in); i++ {
		r := in[i]
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case '\n', '\r':
			continue
		}
		if strings.ContainsRune("kdhms", rune(in[i])) && i > 0 && in[i-1] >= '0' && in[i-1] <= '9' {
			switch r {
			case 'k':
				buf.WriteString("(1000)")
			case 'd':
				buf.WriteString("(86400)")
			case 'h':
				buf.WriteString("(3600)")
			case 'm':
				if i < len(in)-1 && in[i+1] == 's' {
					buf.WriteString("(0.001)")
					i++
				} else {
					buf.WriteString("(60)")
				}
			}
			if i < len(in)-1 && in[i+1] >= '0' && in[i+1] <= '9' {
				buf.WriteString("&")
			}
			continue
		}
		buf.WriteByte(r)
	}
	for ; depth > 0; depth-- {
		buf.WriteByte(')')
	}
	in = buf.String()
	// fmt.Println(in)

	f, err := parser.ParseExpr(in)
	if err != nil {
		return 0, fmt.Errorf("invalid expression: %q, parser reports: %v", in, err)
	}

	var r runner
	if len(args) > 0 {
		if len(args)%2 != 0 {
			return 0, fmt.Errorf("invalid arguments")
		}
		for i := 0; i < len(args); i += 2 {
			if args[i] > 64 && args[i] < 128 {
				r.args[byte(args[i])-64] = args[i+1]
				continue
			}
			return 0, fmt.Errorf("invalid arguments")
		}
	}
	r.now = time.Now().UTC()

	v = r.evalBinary(f)
	if math.IsNaN(v) {
		return v, fmt.Errorf("invalid expression: %q", old)
	}
	return v, nil
}

const (
	intFunc = 0xffffffffffff0001 + iota
	geoHashFunc
	geoHashLossyFunc
	utcAddFunc
	inFunc
	minFunc
	maxFunc
	ninFunc
	ifFunc
	hrFunc
)

type runner struct {
	now  time.Time
	args [64]float64
}

func (r *runner) evalBinary(in ast.Expr) float64 {
	// fmt.Printf("%T", in)
	switch in := in.(type) {
	case *ast.BinaryExpr:
		switch in.Op {
		case token.ADD, token.AND:
			return r.evalBinary(in.X) + r.evalBinary(in.Y)
		case token.SUB:
			return r.evalBinary(in.X) - r.evalBinary(in.Y)
		case token.MUL:
			return r.evalBinary(in.X) * r.evalBinary(in.Y)
		case token.QUO:
			return r.evalBinary(in.X) / r.evalBinary(in.Y)
		case token.XOR:
			return math.Pow(r.evalBinary(in.X), r.evalBinary(in.Y))
		case token.REM:
			x, y := r.evalBinary(in.X), r.evalBinary(in.Y)
			if float64(int64(x)) == x && float64(int64(y)) == y {
				return float64(int64(x) % int64(y))
			}
			return math.Remainder(x, y)
		case token.EQL:
			return bton(r.evalBinary(in.X) == r.evalBinary(in.Y))
		case token.NEQ:
			return bton(r.evalBinary(in.X) != r.evalBinary(in.Y))
		case token.GTR:
			return bton(r.evalBinary(in.X) > r.evalBinary(in.Y))
		case token.LSS:
			return bton(r.evalBinary(in.X) < r.evalBinary(in.Y))
		case token.GEQ:
			return bton(r.evalBinary(in.X) >= r.evalBinary(in.Y))
		case token.LEQ:
			return bton(r.evalBinary(in.X) <= r.evalBinary(in.Y))
		case token.LAND:
			return bton(r.evalBinary(in.X) != 0 && r.evalBinary(in.Y) != 0)
		case token.LOR:
			return bton(r.evalBinary(in.X) != 0 || r.evalBinary(in.Y) != 0)
		}
	case *ast.UnaryExpr:
		switch in.Op {
		case token.ADD:
			return r.evalBinary(in.X)
		case token.SUB:
			return -r.evalBinary(in.X)
		case token.NOT:
			return bton(r.evalBinary(in.X) == 0)
		}
	case *ast.BasicLit:
		v, err := strconv.ParseFloat(in.Value, 64)
		if err != nil {
			return math.NaN()
		}
		return v
	case *ast.ParenExpr:
		return r.evalBinary(in.X)
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
		case "when":
			return math.Float64frombits(ifFunc)
		case "hr":
			return math.Float64frombits(hrFunc)
		case "min":
			return math.Float64frombits(minFunc)
		case "max":
			return math.Float64frombits(maxFunc)
		case "now", "NOW":
			return float64(r.now.UnixNano()/1e6) / 1e3
		case "MIN":
			return float64(r.now.Minute())
		case "HOUR":
			return float64(r.now.Hour())
		case "DOW":
			return float64(r.now.Weekday())
		case "DOY":
			return float64(r.now.YearDay())
		case "DOM":
			return float64(r.now.Day())
		case "WOY":
			_, w := r.now.ISOWeek()
			return float64(w)
		case "MON":
			return float64(r.now.Month())
		default:
			if len(in.Name) == 1 && in.Name[0] > 64 && in.Name[0] < 128 {
				return r.args[in.Name[0]-64]
			}
		}
	case *ast.CallExpr:
		x := r.evalBinary(in.Fun)
		switch n := math.Float64bits(x); n {
		case geoHashFunc:
			if len(in.Args) == 2 {
				long := r.evalBinary(in.Args[0])
				lat := r.evalBinary(in.Args[1])
				return float64(geohash.EncodeIntWithPrecision(lat, long, 52))
			}
		case geoHashLossyFunc:
			if len(in.Args) == 2 {
				h := uint64(r.evalBinary(in.Args[0]))
				i := int(r.evalBinary(in.Args[1]))
				lat, long := geohash.DecodeIntWithPrecision(h>>i, uint(52-i))
				return float64(geohash.EncodeIntWithPrecision(lat, long, 52))
			}
		case intFunc:
			if len(in.Args) == 1 {
				return float64(int64(r.evalBinary(in.Args[0])))
			}
		case ifFunc:
			if len(in.Args) == 3 {
				if r.evalBinary(in.Args[0]) == 0 {
					return r.evalBinary(in.Args[2])
				}
				return r.evalBinary(in.Args[1])
			}
		case inFunc, ninFunc:
			if len(in.Args) > 2 {
				a := r.evalBinary(in.Args[0])
				for i := 1; i < len(in.Args); i++ {
					if a == r.evalBinary(in.Args[i]) {
						return bton(n == inFunc)
					}
				}
				return bton(n == ninFunc)
			}
		case hrFunc:
			if len(in.Args) == 1 {
				return float64((int64(r.evalBinary(in.Args[0])) + 24) % 24)
			}
		case minFunc, maxFunc:
			if len(in.Args) >= 1 {
				x := r.evalBinary(in.Args[0])
				for i := 1; i < len(in.Args); i++ {
					if n == minFunc {
						x = math.Min(x, r.evalBinary(in.Args[i]))
					} else {
						x = math.Max(x, r.evalBinary(in.Args[i]))
					}
				}
				return x
			}
		default:
			if len(in.Args) == 1 {
				return x * r.evalBinary(in.Args[0])
			} else if len(in.Args) == 0 {
				return x
			}
		}
	}
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
