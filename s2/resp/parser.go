package resp

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unsafe"

	"github.com/coyove/s2db/s2"
)

var (
	ExpectNumber   = &ProtocolError{"Expect Number"}
	ExpectNewLine  = &ProtocolError{"Expect Newline"}
	ExpectTypeChar = &ProtocolError{"Expect TypeChar"}

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
	Argv   [][]byte
	last   bool
	itf    bool
	values []any
}

func (c *Command) Bytes(index int) []byte {
	return append([]byte{}, c.BytesRef(index)...)
}

func (c *Command) BytesRef(index int) []byte {
	if index >= 0 && index < len(c.Argv) {
		return c.Argv[index]
	}
	return nil
}

func (c *Command) Str(index int) string {
	return string(c.BytesRef(index))
}

func (c *Command) StrRef(index int) (s string) {
	buf := c.BytesRef(index)
	*(*[2]uintptr)(unsafe.Pointer(&s)) = *(*[2]uintptr)(unsafe.Pointer(&buf))
	return
}

func (c *Command) Int(index int) int {
	return int(c.Int64(index))
}

func (c *Command) Int64(index int) int64 {
	i, err := strconv.ParseInt(c.StrRef(index), 10, 64)
	if err != nil {
		panic(fmt.Errorf("invalid number %q at #%d: %v", c.BytesRef(index), index, err))
	}
	return i
}

func (c *Command) StrEqFold(index int, v string) bool {
	return strings.EqualFold(c.StrRef(index), v)
}

func (c *Command) ArgCount() int {
	return len(c.Argv)
}

func (c *Command) IsLast() bool {
	return c.last
}

func (c *Command) String() string {
	if len(c.Argv) == 0 {
		return "NOP"
	}
	buf := bytes.ToUpper(c.BytesRef(0))
	for i := 1; i < c.ArgCount(); i++ {
		buf = append(buf, ' ')
		msg := c.StrRef(i)
		if _, err := strconv.ParseFloat(msg, 64); err == nil {
			buf = append(buf, msg...)
		} else {
			buf = strconv.AppendQuote(buf, msg)
		}
	}
	return string(buf)
}

func (K *Command) GetAppendOptions(start int) (data, ids [][]byte, opts s2.AppendOptions) {
	data = [][]byte{K.Bytes(2)}
	opts.NoSync = true
	for i := start; i < K.ArgCount(); i++ {
		if K.StrEqFold(i, "and") {
			data = append(data, K.Bytes(i+1))
			i++
		} else if K.StrEqFold(i, "setid") {
			for _, id := range K.Argv[i+1 : i+1+len(data)] {
				ids = append(ids, s2.Bytes(id))
			}
			i += len(data)
		} else if K.StrEqFold(i, "dp") {
			opts.DPLen = byte(K.Int64(i + 1))
			i++
		} else if K.StrEqFold(i, "sync") {
			opts.NoSync = false
		}
		opts.Effect = opts.Effect || K.StrEqFold(i, "effect")
		opts.NoExpire = opts.NoExpire || K.StrEqFold(i, "noexp")
		opts.Defer = opts.Defer || K.StrEqFold(i, "defer")
	}
	return
}

func (K *Command) GetSelectOptions() (n int, flag s2.SelectOptions) {
	// SELECT key start n [...]
	n = K.Int(3)
	for i := 4; i < K.ArgCount(); i++ {
		flag.Desc = flag.Desc || K.StrEqFold(i, "desc")
		flag.Raw = flag.Raw || K.StrEqFold(i, "raw")
		flag.Async = flag.Async || K.StrEqFold(i, "async")
		flag.LeftOpen = flag.LeftOpen || K.StrEqFold(i, "leftopen")
		flag.NoData = flag.NoData || K.StrEqFold(i, "nodata")

		if K.StrEqFold(i, "union") {
			flag.Unions = append(flag.Unions, K.Str(i+1))
			i++
		}
	}
	return
}

func (K *Command) GetScanOptions() (index, local bool, count int) {
	for i := 2; i < K.ArgCount(); i++ {
		if K.StrEqFold(i, "count") {
			count = K.Int(i + 1)
			i++
		} else {
			index = index || K.StrEqFold(i, "index")
			local = local || K.StrEqFold(i, "local")
		}
	}
	if count > 65536 {
		count = 65536
	}
	return
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
		return nil, s2.ErrInvalidNumArg
	case numArg > MaxNumArg:
		return nil, s2.ErrInvalidNumArg
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
			return nil, s2.ErrInvalidBulkSize
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
			return nil, s2.ErrLineTooLong
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