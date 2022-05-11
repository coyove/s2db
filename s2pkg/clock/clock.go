package clock

import (
	_ "runtime"
	"sync"
	"time"
	_ "unsafe"

	"github.com/sirupsen/logrus"
)

//go:linkname runtimeNano runtime.nanotime
func runtimeNano() int64

var (
	startupNano     int64
	startupWallNano int64

	idCounter uint32
	idLastSec int64
	idMutex   sync.Mutex
)

func init() {
	startupNano = runtimeNano()
	startupWallNano = time.Now().UnixNano()
	go func() {
		time.Sleep(time.Millisecond * 100)
		if runtimeNano() <= startupNano {
			logrus.Error("invalid monotonic clock")
			logrus.Exit(-1)
		}
	}()
}

func UnixNano() int64 {
	return runtimeNano() - startupNano + startupWallNano
}

func Now() time.Time {
	return time.Unix(0, UnixNano())
}

// 35 bits seconds timestamp, 28 bits counter
func Id() (id uint64) {
	idMutex.Lock()
	defer idMutex.Unlock()

	sec := UnixNano() / 1e9
	if sec < idLastSec {
		panic("bad clock skew")
	}
	if sec != idLastSec {
		idCounter = 0
	}
	idLastSec = sec
	idCounter++
	if idCounter >= 0x0fffffff {
		panic("too many calls in one second")
	}
	id = uint64(sec)<<28 | uint64(idCounter&0x0fffffff)
	return
}

func IdNano(id uint64) int64 {
	return int64(id >> 28)
}

func IdBeforeSeconds(id uint64, seconds int) uint64 {
	idsec := int64(id >> 28)
	if idsec <= int64(seconds) {
		return 1<<28 + 1
	}
	return uint64(idsec-int64(seconds))<<28 + 1
}
