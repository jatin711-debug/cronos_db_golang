package consumer

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

// Completion keys are stored, so building them without fmt must give exactly
// the bytes fmt gave: records written before would not be found otherwise.
func TestCompletionKeysMatchTheirFormat(t *testing.T) {
	groups := []string{"", "g", "soak-slow", "groupe-é", strings.Repeat("x", 300), "a:b:c"}
	partitions := []int32{0, 1, 7, 1023, math.MaxInt32, -1}
	offsets := []int64{0, 1, 9, 10, 12345, 1<<40 + 7, math.MaxInt64, -1, -12345, math.MinInt64}
	var buf []byte
	for _, group := range groups {
		for _, partition := range partitions {
			wantPrefix := fmt.Sprintf("done:%d:%s:%d:", len(group), group, partition)
			if got := completionPrefix(group, partition); got != wantPrefix {
				t.Fatalf("completionPrefix(%q, %d) = %q, want %q", group, partition, got, wantPrefix)
			}
			for _, offset := range offsets {
				want := fmt.Sprintf("done:%d:%s:%d:%020d", len(group), group, partition, offset)
				if got := completionKey(group, partition, offset); got != want {
					t.Fatalf("completionKey(%q, %d, %d) = %q, want %q", group, partition, offset, got, want)
				}
				buf = appendCompletionKey(buf[:0], group, partition, offset)
				if string(buf) != want {
					t.Fatalf("appendCompletionKey(%q, %d, %d) = %q, want %q", group, partition, offset, buf, want)
				}
			}
		}
	}
}

func BenchmarkCompletionKey(b *testing.B) {
	b.Run("format", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = []byte(fmt.Sprintf("done:%d:%s:%d:%020d", len("soak-slow"), "soak-slow", 3, int64(i)))
		}
	})
	b.Run("append", func(b *testing.B) {
		b.ReportAllocs()
		var key []byte
		for i := 0; i < b.N; i++ {
			key = appendCompletionKey(key[:0], "soak-slow", 3, int64(i))
		}
	})
}
