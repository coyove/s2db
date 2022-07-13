package wire

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/coyove/s2db/ranges"
	"github.com/coyove/s2db/s2pkg"
)

var (
	ExpectNumber   = &ProtocolError{"Expect Number"}
	ExpectNewLine  = &ProtocolError{"Expect Newline"}
	ExpectTypeChar = &ProtocolError{"Expect TypeChar"}

	ErrInvalidNumArg    = errors.New("too many arguments")
	ErrInvalidBulkSize  = errors.New("invalid bulk size")
	ErrLineTooLong      = errors.New("line too long")
	ErrUnknownCommand   = errors.New("unknown command")
	ErrServerReadonly   = errors.New("server is readonly")
	ErrRejectedByMaster = errors.New("rejected by master")
	ErrNoAuth           = errors.New("NOAUTH")

	ReadBufferInitSize = 1 << 16
	MaxNumArg          = 20000
	MaxBulkSize        = 1 << 20
	MaxTelnetLine      = 1 << 20
	spaceSlice         = []byte{' '}
	emptyBulk          = [0]byte{}
)

type ProtocolError struct {
	message string
}

func (p *ProtocolError) Error() string {
	return p.message
}

type Command struct {
	Argv [][]byte
	last bool
}

func (c *Command) At(index int) []byte {
	if index >= 0 && index < len(c.Argv) {
		return c.Argv[index]
	} else {
		return nil
	}
}

func (c *Command) Get(index int) string {
	return string(c.At(index))
}

func (c *Command) Int64(index int) int64 {
	return s2pkg.MustParseInt64(string(c.At(index)))
}

func (c *Command) Float64(index int) float64 {
	return s2pkg.MustParseFloatBytes(c.At(index))
}

func (c *Command) Bytes(index int) []byte {
	return append([]byte{}, c.At(index)...)
}

func (c *Command) EqualFold(index int, v string) bool {
	buf := c.At(index)
	return strings.EqualFold(*(*string)(unsafe.Pointer(&buf)), v)
}

func (c *Command) ArgCount() int {
	return len(c.Argv)
}

func (c *Command) IsLast() bool {
	return c.last
}

func (c *Command) Args() []interface{} {
	a := make([]interface{}, len(c.Argv))
	for i := range c.Argv {
		a[i] = c.Argv[i]
	}
	return a
}

func (c *Command) String() string {
	if len(c.Argv) == 0 {
		return "NOP"
	}
	buf := bytes.NewBufferString(c.Get(0))
	for i := 1; i < c.ArgCount(); i++ {
		msg := c.At(i)
		if i == 1 {
			buf.WriteString("\t")
		} else {
			buf.WriteString(" ")
		}
		if utf8.Valid(msg) {
			x := *(*string)(unsafe.Pointer(&msg))
			if _, err := strconv.ParseFloat(x, 64); err == nil {
				buf.WriteString(x)
			} else {
				buf.Write(strconv.AppendQuote(nil, x))
			}
		} else {
			w := base64.NewEncoder(base64.URLEncoding, buf)
			w.Write(msg)
			w.Close()
		}
	}
	return buf.String()
}

type Parser struct {
	reader        io.Reader
	buffer        []byte
	parsePosition int
	writeIndex    int
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func NewParser(reader io.Reader) *Parser {
	return &Parser{reader: reader, buffer: make([]byte, ReadBufferInitSize)}
}

// ensure that we have enough space for writing 'req' byte
func (r *Parser) requestSpace(req int) {
	ccap := cap(r.buffer)
	if r.writeIndex+req > ccap {
		newbuff := make([]byte, max(ccap*2, ccap+req+ReadBufferInitSize))
		copy(newbuff, r.buffer)
		r.buffer = newbuff
	}
}

func (r *Parser) readSome(min int) error {
	r.requestSpace(min)
	nr, err := io.ReadAtLeast(r.reader, r.buffer[r.writeIndex:], min)
	if err != nil {
		return err
	}
	r.writeIndex += nr
	return nil
}

// check for at least 'num' byte available in buffer to use, wait if need
func (r *Parser) requireNBytes(num int) error {
	a := r.writeIndex - r.parsePosition
	if a >= num {
		return nil
	}
	if err := r.readSome(num - a); err != nil {
		return err
	}
	return nil
}

func (r *Parser) readNumber() (int, error) {
	var neg bool = false
	err := r.requireNBytes(1)
	if err != nil {
		return 0, err
	}
	switch r.buffer[r.parsePosition] {
	case '-':
		neg = true
		r.parsePosition++
	case '+':
		neg = false
		r.parsePosition++
	}
	var num uint64 = 0
	var startpos int = r.parsePosition
OUTTER:
	for {
		for i := r.parsePosition; i < r.writeIndex; i++ {
			c := r.buffer[r.parsePosition]
			if c >= '0' && c <= '9' {
				num = num*10 + uint64(c-'0')
				r.parsePosition++
			} else {
				break OUTTER
			}
		}
		if r.parsePosition == r.writeIndex {
			if e := r.readSome(1); e != nil {
				return 0, e
			}
		}
	}
	if r.parsePosition == startpos {
		return 0, ExpectNumber
	}
	if neg {
		return -int(num), nil
	} else {
		return int(num), nil
	}

}
func (r *Parser) discardNewLine() error {
	if e := r.requireNBytes(2); e != nil {
		return e
	}
	if r.buffer[r.parsePosition] == '\r' && r.buffer[r.parsePosition+1] == '\n' {
		r.parsePosition += 2
		return nil
	}
	return ExpectNewLine
}

func (r *Parser) parseBinary() (*Command, error) {
	r.parsePosition++
	numArg, err := r.readNumber()
	if err != nil {
		return nil, err
	}
	var e error
	if e = r.discardNewLine(); e != nil {
		return nil, e
	}
	switch {
	case numArg == -1:
		return nil, r.discardNewLine() // null array
	case numArg < -1:
		return nil, ErrInvalidNumArg
	case numArg > MaxNumArg:
		return nil, ErrInvalidNumArg
	}
	Argv := make([][]byte, 0, numArg)
	for i := 0; i < numArg; i++ {
		if e = r.requireNBytes(1); e != nil {
			return nil, e
		}
		if r.buffer[r.parsePosition] != '$' {
			return nil, ExpectTypeChar
		}
		r.parsePosition++
		var plen int
		if plen, e = r.readNumber(); e != nil {
			return nil, e
		}
		if e = r.discardNewLine(); e != nil {
			return nil, e
		}
		switch {
		case plen == -1:
			Argv = append(Argv, nil) // null bulk
		case plen == 0:
			Argv = append(Argv, emptyBulk[:]) // empty bulk
		case plen > 0 && plen <= MaxBulkSize:
			if e = r.requireNBytes(plen); e != nil {
				return nil, e
			}
			Argv = append(Argv, r.buffer[r.parsePosition:(r.parsePosition+plen)])
			r.parsePosition += plen
		default:
			return nil, ErrInvalidBulkSize
		}
		if e = r.discardNewLine(); e != nil {
			return nil, e
		}
	}
	return &Command{Argv: Argv}, nil
}

func (r *Parser) parseTelnet() (*Command, error) {
	nlPos := -1
	for {
		nlPos = bytes.IndexByte(r.buffer, '\n')
		if nlPos == -1 {
			if e := r.readSome(1); e != nil {
				return nil, e
			}
		} else {
			break
		}
		if r.writeIndex > MaxTelnetLine {
			return nil, ErrLineTooLong
		}
	}
	r.parsePosition = r.writeIndex // we don't support pipeline in telnet mode
	return &Command{Argv: bytes.Split(r.buffer[:nlPos-1], spaceSlice)}, nil
}

func (r *Parser) reset() {
	r.writeIndex = 0
	r.parsePosition = 0
	//r.buffer = make([]byte, len(r.buffer))
}

func (r *Parser) ReadCommand() (*Command, error) {
	// if the buffer is empty, try to fetch some
	if r.parsePosition >= r.writeIndex {
		if err := r.readSome(1); err != nil {
			return nil, err
		}
	}

	var cmd *Command
	var err error
	if r.buffer[r.parsePosition] == '*' {
		cmd, err = r.parseBinary()
	} else {
		cmd, err = r.parseTelnet()
	}
	if r.parsePosition >= r.writeIndex {
		if cmd != nil {
			cmd.last = true
		}
		r.reset()
	}
	return cmd, err
}

func (r *Parser) Commands() <-chan *Command {
	cmds := make(chan *Command)
	go func() {
		for cmd, err := r.ReadCommand(); err == nil; cmd, err = r.ReadCommand() {
			cmds <- cmd
		}
		close(cmds)

	}()
	return cmds
}

type Flags struct {
	Match   string
	TwoHops struct {
		Member  string
		KeyFunc func(string) string
	}
	Union        []string
	Intersect    map[string]bool
	Limit        int
	ILimit       *float64
	Count        int
	WithData     bool
	WithScores   bool
	MemberBitmap bool
	Timeout      time.Duration
}

func (f *Flags) IsSpecial() bool {
	return f.TwoHops.Member != "" || len(f.Intersect) > 0
}

func (c Command) Flags(start int) (f Flags) {
	f.Limit = ranges.HardLimit
	f.Count = ranges.HardLimit
	f.Timeout = time.Second
	if start == -1 {
		return
	}
	for i := start; i < c.ArgCount(); i++ {
		if c.EqualFold(i, "COUNT") {
			f.Count = s2pkg.MustParseInt(c.Get(i + 1))
			if f.Count > ranges.HardLimit {
				f.Count = ranges.HardLimit
			}
			i++
		} else if c.EqualFold(i, "LIMIT") {
			if c.Get(i+1) != "0" {
				panic("non-zero limit offset not supported")
			}
			f.Limit = s2pkg.MustParseInt(c.Get(i + 2))
			if f.Limit > ranges.HardLimit {
				f.Limit = ranges.HardLimit
			}
			i += 2
		} else if c.EqualFold(i, "MATCH") {
			f.Match = c.Get(i + 1)
			i++
		} else if c.EqualFold(i, "INTERSECT") {
			if f.Intersect == nil {
				f.Intersect = map[string]bool{}
			}
			f.Intersect[c.Get(i+1)] = true
			i++
		} else if c.EqualFold(i, "NOTINTERSECT") {
			if f.Intersect == nil {
				f.Intersect = map[string]bool{}
			}
			f.Intersect[c.Get(i+1)] = false
			i++
		} else if c.EqualFold(i, "TWOHOPS") {
			f.TwoHops.Member = c.Get(i + 1)
			f.TwoHops.KeyFunc = func(in string) string { return in }
			i++
			if c.EqualFold(i+1, "CONCATKEY") {
				i++
				prefix := c.Get(i + 1)
				start, end := int(c.Int64(i+2)), int(c.Int64(i+3))
				suffix := c.Get(i + 4)
				f.TwoHops.KeyFunc = func(in string) string {
					s := (start + len(in)) % len(in)
					e := (end + len(in)) % len(in)
					return prefix + in[s:e+1] + suffix
				}
				i += 4
			}
		} else if c.EqualFold(i, "UNION") {
			f.Union = append(f.Union, c.Get(i+1))
			i++
		} else if c.EqualFold(i, "TIMEOUT") {
			f.Timeout, _ = time.ParseDuration(c.Get(i + 1))
			i++
		} else {
			f.WithData = f.WithData || c.EqualFold(i, "WITHDATA")
			f.WithScores = f.WithScores || c.EqualFold(i, "WITHSCORES")
			f.MemberBitmap = f.MemberBitmap || c.EqualFold(i, "MBRBITMAP")
			if f.MemberBitmap {
				f.WithData = true
			}
		}
	}
	return
}
