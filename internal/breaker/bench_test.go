package breaker

import (
	"testing"
	"time"
)

func BenchmarkAllowSuccessParallel(b *testing.B) {
	br := New("b", 5, time.Second)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if br.Allow() {
				br.Success()
			}
		}
	})
}
