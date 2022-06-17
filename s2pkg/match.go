package s2pkg

import (
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unsafe"

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
	}

	if strings.HasPrefix(key, "json:") {
		return Match(value, gjson.Parse(text).Get(key[5:]).String())
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
