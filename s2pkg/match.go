package s2pkg

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unsafe"

	"github.com/coyove/s2db/bitmap"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

func MatchMemberOrData(pattern, member string, data []byte) bool {
	if strings.HasPrefix(pattern, "\\member{}") {
		return Match(pattern[9:], member)
	}
	if strings.HasPrefix(pattern, "\\data{}") {
		return MatchBinary(pattern[7:], data)
	}
	return Match(pattern, member) || MatchBinary(pattern, data)
}

func MatchBinary(pattern string, buf []byte) bool {
	return Match(pattern, *(*string)(unsafe.Pointer(&buf)))
}

func runSubMatch(key, value, rest string, text string) bool {
	switch key {
	case "not":
		return !Match(value, text)
	case "or":
		for {
			k, v, r := ExtractEscape(value)
			if v == "" {
				break
			}
			if runSubMatch(k, v, r, text) {
				return true
			}
			value = r
		}
		return false
	case "re":
		rx, err := regexp.Compile(value)
		if err != nil {
			logrus.Errorf("Match: invalid regex pattern `%s`: %v", value, err)
			return false
		}
		return rx.MatchString(text)
	case "match":
		m, err := filepath.Match(value, text)
		if err != nil {
			logrus.Errorf("Match: invalid \"not\" pattern `%s`: %v", value, err)
			return false
		}
		return m
	case "term":
		return strings.Contains(text, value)
	case "prefix":
		return strings.HasPrefix(text, value)
	case "suffix":
		return strings.HasSuffix(text, value)
	case "gt":
		return ParseFloat(text) > ParseFloat(value)
	case "lt":
		return ParseFloat(text) < ParseFloat(value)
	case "ge":
		return ParseFloat(text) >= ParseFloat(value)
	case "le":
		return ParseFloat(text) <= ParseFloat(value)
	case "eq":
		return ParseFloat(text) == ParseFloat(value)
	case "ne":
		return ParseFloat(text) != ParseFloat(value)
	case "bm16":
		for len(value) > 0 {
			idx := strings.IndexByte(value, ',')
			var v int
			if idx == -1 {
				v, _ = strconv.Atoi(value)
				value = ""
			} else {
				v, _ = strconv.Atoi(value[:idx])
				value = value[idx+1:]
			}
			if bitmap.StringContains(text, uint16(v)) {
				return true
			}
		}
	}

	if strings.HasPrefix(key, "json:") {
		return Match(value, gjson.Parse(text).Get(key[5:]).String())
	}
	if strings.HasPrefix(key, "pb:") {
		var a = struct {
			s string
			i int
		}{text, len(text)}
		v, err := ReadProtobuf(*(*[]byte)(unsafe.Pointer(&a)), key[3:])
		if err != nil {
			logrus.Errorf("Match: failed to read protobuf: %v", err)
			return false
		}
		return Match(value, fmt.Sprint(v))
	}
	return false
}

func Match(pattern string, text string) bool {
	key, value, rest := ExtractEscape(pattern)
	if value != "" {
		if !runSubMatch(key, value, rest, text) {
			return false
		}
		if rest == "" {
			return true
		}
		return Match(rest, text)
	}
	m, err := filepath.Match(rest, text)
	if err != nil {
		logrus.Errorf("Match: invalid pattern `%s`: %v", pattern, err)
		return false
	}
	return m
}

func ExtractEscape(text string) (key, value, rest string) {
	if strings.HasPrefix(text, "\\") {
		if idx := strings.Index(text, "{"); idx > -1 {
			key = text[1:idx]
			text = text[idx:]
			for i, q, b := 1, false, 0; i < len(text); i++ {
				switch text[i] {
				case '"':
					if q && text[i-1] == '\\' {
						continue
					}
					q = !q
				case '{':
					if q {
						continue
					}
					b++
				case '}':
					if q {
						continue
					}
					if b == 0 {
						value = text[1:i]
						if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
							value, _ = strconv.Unquote(value)
						}
						return key, value, strings.TrimLeftFunc(text[i+1:], unicode.IsSpace)
					}
					b--
				}
			}
		}
	}
	if strings.HasPrefix(text, "\\\\") {
		text = text[1:]
	}
	return "", "", text
}

func readVarint(buf *[]byte) (int64, uint64, error) {
	v, n := binary.Varint(*buf)
	if n <= 0 {
		return 0, 0, fmt.Errorf("invalid varint encoding: %q", *buf)
	}
	u, _ := binary.Uvarint(*buf)
	*buf = (*buf)[n:]
	return v, u, nil
}

func readField(buf []byte, idx int, typ byte) (interface{}, error) {
READ:
	if len(buf) == 0 {
		return nil, fmt.Errorf("field index %d not found", idx)
	}

	_, v, err := readVarint(&buf)
	if err != nil {
		return nil, err
	}

	fidx, wtyp := v>>3, v&7

	switch wtyp {
	case 0:
		v, u, err := readVarint(&buf)
		if err != nil {
			return nil, err
		}
		if int(fidx) != idx {
			goto READ
		}
		switch typ {
		case 'i':
			return v, nil
		case 'u':
			return u, nil
		case 'b':
			return u == 1, nil
		}
	case 1:
		if len(buf) < 8 {
			return nil, fmt.Errorf("invalid fixed64 buffer")
		}
		if int(fidx) != idx {
			buf = buf[8:]
			goto READ
		}
		switch typ {
		case 'i':
			return int64(binary.LittleEndian.Uint64(buf)), nil
		case 'u':
			return binary.LittleEndian.Uint64(buf), nil
		}
	case 2:
		_, length, err := readVarint(&buf)
		if err != nil {
			return nil, fmt.Errorf("invalid length-delimited data: %v", err)
		}
		if len(buf) < int(length) {
			return nil, fmt.Errorf("invalid length-delimited data")
		}
		if int(fidx) != idx {
			buf = buf[length:]
			goto READ
		}
		if typ == 's' {
			return string(buf[:length]), nil
		}
		return buf[:length], nil
	case 3, 4:
		goto READ
	case 5:
		if len(buf) < 4 {
			return nil, fmt.Errorf("invalid fixed32 buffer")
		}
		if int(fidx) != idx {
			buf = buf[4:]
			goto READ
		}
		switch typ {
		case 'i':
			return int32(binary.LittleEndian.Uint32(buf)), nil
		case 'u':
			return binary.LittleEndian.Uint32(buf), nil
		}
	}
	return nil, fmt.Errorf("unknown wire type %d or user type %c", wtyp, typ)
}

func ReadProtobuf(buf []byte, idx string) (interface{}, error) {
	for len(idx) > 0 {
		var fidx int
		var typ byte

		sep := strings.IndexByte(idx, ',')
		if sep == -1 {
			fmt.Sscanf(idx, "%d%c", &fidx, &typ)
			idx = ""
		} else {
			fmt.Sscanf(idx[:sep], "%d%c", &fidx, &typ)
			idx = idx[sep+1:]
		}

		f, err := readField(buf, fidx, typ)
		if len(idx) == 0 {
			return f, err
		}
		if err != nil {
			return nil, err
		}
		if _, ok := f.([]byte); !ok {
			return nil, fmt.Errorf("expected length-delimited data, got: %v", f)
		}
		buf = f.([]byte)
	}
	return nil, nil
}
