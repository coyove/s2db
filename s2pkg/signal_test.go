package s2pkg

import (
	"fmt"
	"testing"
	"time"
)

func TestLocker(t *testing.T) {
	var lk Locker
	for i := 0; i < 10; i++ {
		go func(i int) {
			lk.Wait(nil)
			fmt.Println("done", i)
		}(i)
	}
	lk.Lock()
	time.Sleep(time.Second)
	lk.Unlock()
}
