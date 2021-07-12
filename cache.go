package main

type CacheState struct {
	Command string
	CmdHash [2]uint64
	Key     string
	Lower   RangeLimit
	Upper   RangeLimit
	Data    []Pair
}

func NewCacheState(cmd, key string, h [2]uint64, pairs []Pair) *CacheState {
	cs := &CacheState{
		Command: cmd,
		CmdHash: h,
		Key:     key,
		Data:    pairs,
	}
	switch cmd {
	case "ZRANGEBYLEX", "ZREVRANGEBYLEX":
		cs.Lower.Value = pairs[0].Key
		cs.Upper.Value = pairs[len(pairs)-1].Key
		if cs.Lower.Value > cs.Upper.Value {
			cs.Lower, cs.Upper = cs.Upper, cs.Lower
		}
	case "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
		cs.Lower.Float = pairs[0].Score
		cs.Upper.Float = pairs[len(pairs)-1].Score
		if cs.Lower.Float > cs.Upper.Float {
			cs.Lower, cs.Upper = cs.Upper, cs.Lower
		}
	}
	return cs
}

func (cs *CacheState) Intersect(p []Pair) bool {
	if len(p) == 0 {
		return false
	}
	switch cs.Command {
	case "ZRANGEBYLEX", "ZREVRANGEBYLEX":
		lo, hi := p[0].Key, p[len(p)-1].Key
		if lo > hi {
			lo, hi = hi, lo
		}
		if (lo >= cs.Lower.Value && lo <= cs.Upper.Value) || (hi >= cs.Lower.Value && hi <= cs.Upper.Value) {
			return true
		}
		if lo < cs.Lower.Value && hi > cs.Upper.Value {
			return true
		}
	case "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
		lo, hi := p[0].Score, p[len(p)-1].Score
		if lo > hi {
			lo, hi = hi, lo
		}
		if (lo >= cs.Lower.Float && lo <= cs.Upper.Float) || (hi >= cs.Lower.Float && hi <= cs.Upper.Float) {
			return true
		}
		if lo < cs.Lower.Float && hi > cs.Upper.Float {
			return true
		}
	}
	return false
}
