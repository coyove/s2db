package main

type CacheState struct {
	Command string
	Key     string
	Lower   RangeLimit
	Upper   RangeLimit
}

func NewCacheState(cmd, key string, pairs []Pair) *CacheState {
	cs := &CacheState{
		Command: cmd,
		Key:     key,
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
	for _, p := range p {
		switch cs.Command {
		case "ZRANGEBYLEX", "ZREVRANGEBYLEX":
			if p.Key >= cs.Lower.Value && p.Key <= cs.Upper.Value {
				return true
			}
		case "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
			if p.Score >= cs.Lower.Float && p.Score <= cs.Upper.Float {
				return true
			}
		}
	}
	return false
}
