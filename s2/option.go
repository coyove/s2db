package s2

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type AppendOptions struct {
	// Distinct prefix length.
	DPLen byte

	// Data will not be synced to other peers.
	NoSync bool

	// Wait for proper cause-effect.
	Effect bool

	// Data will not expire.
	NoExpire bool

	// The operation will be deferred and executed sometime in the future.
	// Returned IDs do not indicate the success/effect of this operation.
	Defer bool
}

type SelectOptions struct {
	// Select data in desc order.
	Desc bool

	// Select local data, merge peers asynchronously.
	Async bool

	// Select local data only, including special DB markers.
	Raw bool
}

func (o SelectOptions) ToInt() (v int64) {
	if o.Desc {
		v |= 1
	}
	if o.Async {
		v |= 2
	}
	if o.Raw {
		v |= 4
	}
	return
}

func ParseSelectOptions(v int64) (o SelectOptions) {
	o.Desc = v&1 > 0
	o.Async = v&2 > 0
	o.Raw = v&4 > 0
	return o
}

var SO_Desc = &SelectOptions{Desc: true}

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
