package s2

import (
	"errors"
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

	// Selected range will be left-opened.
	LeftOpen bool

	// Omit data upon returning the result.
	NoData bool

	// Union keys.
	Unions []string
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
	if o.LeftOpen {
		v |= 8
	}
	if o.NoData {
		v |= 16
	}
	return
}

func ParseSelectOptions(v int64) (o SelectOptions) {
	o.Desc = v&1 > 0
	o.Async = v&2 > 0
	o.Raw = v&4 > 0
	o.LeftOpen = v&8 > 0
	o.NoData = v&16 > 0
	return o
}

var SO_Desc = &SelectOptions{Desc: true}

var (
	ErrInvalidNumArg   = errors.New("too many arguments")
	ErrInvalidBulkSize = errors.New("invalid bulk size")
	ErrLineTooLong     = errors.New("line too long")
	ErrUnknownCommand  = errors.New("unknown command")
	ErrServerReadonly  = errors.New("server is readonly")
	ErrNoAuth          = errors.New("authentication required")
	ErrPeerTimeout     = errors.New("peer timed out")
)
