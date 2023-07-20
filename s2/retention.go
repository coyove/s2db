package s2

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type RetentionConfig struct {
	Prefix string
	Value  int64
}

// Form: prefix0:config0,prefix1:config1...
func ParseRetentionTable(in string) (dtl, keep []RetentionConfig, err error) {
	for len(in) > 0 {
		idx := strings.IndexByte(in, ',')
		next := idx + 1
		if idx == -1 {
			idx = len(in)
			next = idx
		}
		if idx < 3 {
			break
		}
		p := in[:idx]
		in = in[next:]

		idx = strings.IndexByte(p, ':')
		if idx < 1 {
			return nil, nil, fmt.Errorf("invalid config: %q", p)
		}

		c := RetentionConfig{}
		c.Prefix = p[:idx]

		x := p[idx+1:]
		if strings.HasSuffix(x, "d") {
			d, err := strconv.ParseInt(x[:len(x)-1], 10, 64)
			if err != nil {
				return nil, nil, err
			}
			c.Value = d
			dtl = append(dtl, c)
		} else if strings.HasPrefix(x, "+") {
			d, err := strconv.ParseInt(x[1:], 10, 64)
			if err != nil {
				return nil, nil, err
			}
			c.Value = d
			keep = append(keep, c)
		} else {
			return nil, nil, fmt.Errorf("invalid config: %q", p)
		}
	}
	s := func(res []RetentionConfig) {
		sort.Slice(res, func(i, j int) bool {
			if len(res[i].Prefix) == len(res[j].Prefix) {
				return res[i].Prefix < res[j].Prefix
			}
			return len(res[i].Prefix) > len(res[j].Prefix)
		})
	}
	s(dtl)
	s(keep)
	return
}
