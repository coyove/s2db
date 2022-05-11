package clock

import "testing"

func BenchmarkClockId(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Id()
	}
}
