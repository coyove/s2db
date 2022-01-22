package redisproto

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/coyove/nj"
	"github.com/coyove/nj/bas"
	"github.com/coyove/s2db/s2pkg"
)

var (
	ExpectNumber   = &ProtocolError{"Expect Number"}
	ExpectNewLine  = &ProtocolError{"Expect Newline"}
	ExpectTypeChar = &ProtocolError{"Expect TypeChar"}

	InvalidNumArg   = errors.New("TooManyArg")
	InvalidBulkSize = errors.New("Invalid bulk size")
	LineTooLong     = errors.New("LineTooLong")

	ReadBufferInitSize = 1 << 16
	MaxNumArg          = 20
	MaxBulkSize        = 1 << 16
	MaxTelnetLine      = 1 << 10
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
		return "<empty command>"
	}
	buf := bytes.NewBufferString(c.Get(0))
	for i := 1; i < c.ArgCount(); i++ {
		msg := c.At(i)
		if utf8.Valid(msg) {
			buf.WriteString(" '")
			buf.Write(msg)
			buf.WriteString("'")
		} else {
			buf.WriteString(" 0x")
			hex.NewEncoder(buf).Write(msg)
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
		break
	case '+':
		neg = false
		r.parsePosition++
		break
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
		return nil, InvalidNumArg
	case numArg > MaxNumArg:
		return nil, InvalidNumArg
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
			return nil, InvalidBulkSize
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
			return nil, LineTooLong
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
	Command
	MATCH     string
	MATCHDATA string
	TWOHOPS   struct {
		ENDPOINT []byte
		KEYMAP   bas.Value
	}
	MERGE struct {
		ENDPOINTS []string
		FUNC      bas.Value
	}
	INTERSECT   map[string]bas.Value
	LIMIT       int
	COUNT       int
	SHARD       int
	ANY         bool
	ASC         bool
	DESC        bool
	WITHDATA    bool
	WITHSCORES  bool
	WITHCOORD   bool
	WITHDIST    bool
	WITHHASH    bool
	WITHINDEXES bool
	TIMEOUT     time.Duration
}

func (c Command) Flags(start int) (f Flags) {
	f.Command = c
	f.LIMIT = s2pkg.RangeHardLimit
	f.COUNT = s2pkg.RangeHardLimit
	f.SHARD = -1
	f.TIMEOUT = time.Second
	for i := start; i < c.ArgCount(); i++ {
		if c.EqualFold(i, "COUNT") {
			f.COUNT = s2pkg.MustParseInt(c.Get(i + 1))
			i++
		} else if c.EqualFold(i, "SHARD") {
			f.SHARD = s2pkg.MustParseInt(c.Get(i + 1))
			i++
		} else if c.EqualFold(i, "LIMIT") {
			if c.Get(i+1) != "0" {
				panic("non-zero limit offset not supported")
			}
			f.LIMIT = s2pkg.MustParseInt(c.Get(i + 2))
			i += 2
		} else if c.EqualFold(i, "MATCH") {
			f.MATCH = c.Get(i + 1)
			i++
		} else if c.EqualFold(i, "MATCHDATA") {
			f.MATCHDATA = c.Get(i + 1)
			i++
		} else if c.EqualFold(i, "INTERSECT") {
			if f.INTERSECT == nil {
				f.INTERSECT = map[string]bas.Value{}
			}
			key, fun := splitCode(c, c.Get(i+1))
			f.INTERSECT[key] = fun
			i++
		} else if c.EqualFold(i, "TWOHOPS") {
			key, fun := splitCode(c, c.Get(i+1))
			f.TWOHOPS.ENDPOINT = []byte(key)
			f.TWOHOPS.KEYMAP = fun
			i++
		} else if c.EqualFold(i, "MERGE") {
			key, fun := splitCode(c, c.Get(i+1))
			f.MERGE.ENDPOINTS = strings.Split(key, ",")
			f.MERGE.FUNC = fun
			i++
		} else if c.EqualFold(i, "TIMEOUT") {
			f.TIMEOUT, _ = time.ParseDuration(c.Get(i + 1))
			i++
		} else {
			f.WITHSCORES = f.WITHCOORD || c.EqualFold(i, "WITHSCORES")
			f.ANY = f.ANY || c.EqualFold(i, "ANY")
			f.ASC = f.ASC || c.EqualFold(i, "ASC")
			f.DESC = f.DESC || c.EqualFold(i, "DESC")
			f.WITHDATA = f.WITHDATA || c.EqualFold(i, "WITHDATA")
			f.WITHSCORES = f.WITHSCORES || c.EqualFold(i, "WITHSCORES")
			f.WITHCOORD = f.WITHCOORD || c.EqualFold(i, "WITHCOORD")
			f.WITHDIST = f.WITHDIST || c.EqualFold(i, "WITHDIST")
			f.WITHHASH = f.WITHHASH || c.EqualFold(i, "WITHHASH")
			f.WITHINDEXES = f.WITHINDEXES || c.EqualFold(i, "WITHINDEXES")
		}
	}
	return
}

func splitCode(c Command, key string) (string, bas.Value) {
	if idx := strings.Index(key, "{"); idx > 0 && strings.HasSuffix(key, "}") {
		key2 := key[:idx]
		code, err := nj.LoadString(key[idx+1:len(key)-1], &bas.Environment{
			Globals: bas.NewObject(2).SetProp("left", bas.Str(c.Get(1))).SetProp("right", bas.Str(key2)),
		})
		s2pkg.PanicErr(err)
		res, err := code.Run()
		s2pkg.PanicErr(err)
		return key2, res
	}
	return key, bas.Nil
}

func (in Command) HashCode() (h [2]uint64) {
	h = [2]uint64{0, 5381}
	for _, buf := range in.Argv {
		for _, b := range buf {
			old := h[1]
			h[1] = h[1]*33 + uint64(b)
			if h[1] < old {
				h[0]++
			}
		}
		h[1]++
	}
	return h
}
