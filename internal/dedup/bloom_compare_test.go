package dedup

import (
	"fmt"
	"testing"
)

// The server uses the Rust bloom filter when it is built with cgo (the Docker
// image) and the Go one otherwise (the release binaries). These benchmarks
// compare them through the BloomFilter interface the server calls, so the
// cost of calling into Rust is counted. Rust cases are skipped in a build
// without cgo:
//
//	CGO_ENABLED=1 go test -run XXX -bench BloomCompare ./internal/dedup/

const (
	compareCapacity = 1_000_000 // what the soak runs with (--bloom-capacity)
	compareFPR      = 0.01      // what the partition manager asks for
)

type bloomImpl struct {
	name string
	make func() BloomFilter
}

func bloomImpls() []bloomImpl {
	return []bloomImpl{
		{"go", func() BloomFilter { return NewGoBloomFilter(compareCapacity, compareFPR) }},
		{"rust", func() BloomFilter {
			if bf := NewRustBloomFilter(compareCapacity, compareFPR); bf != nil {
				return bf
			}
			return nil
		}},
	}
}

// compareKeys returns n dedup keys in the server's format. short ones look
// like the soak's IDs; long ones like a UUID on a longer topic.
func compareKeys(prefix string, n int, long bool) []string {
	keys := make([]string, n)
	for i := range keys {
		if long {
			keys[i] = Key("orders.payments", fmt.Sprintf("%s-%08x-4b1d-9c2e-%012x", prefix, i, i*2654435761))
		} else {
			keys[i] = Key("soak", fmt.Sprintf("%s.w%02d.%d", prefix, i%48, i))
		}
	}
	return keys
}

func BenchmarkBloomCompare(b *testing.B) {
	for _, keyKind := range []struct {
		name string
		long bool
	}{{"short-key", false}, {"long-key", true}} {
		present := compareKeys("present", compareCapacity/2, keyKind.long)
		absent := compareKeys("absent", compareCapacity/2, keyKind.long)
		for _, impl := range bloomImpls() {
			prefix := impl.name + "/" + keyKind.name
			filled := impl.make()
			if filled == nil {
				b.Run(prefix, func(b *testing.B) { b.Skip("no Rust filter in this build (cgo off)") })
				continue
			}
			for _, key := range present {
				filled.Add(key)
			}

			b.Run(prefix+"/add", func(b *testing.B) {
				bf := impl.make()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					bf.Add(present[i%len(present)])
				}
			})
			b.Run(prefix+"/check-present", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if !filled.MayContain(present[i%len(present)]) {
						b.Fatal("a key that was added is missing")
					}
				}
			})
			b.Run(prefix+"/check-absent", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					filled.MayContain(absent[i%len(absent)])
				}
			})
			for _, size := range []int{64, 1024} {
				b.Run(fmt.Sprintf("%s/check-batch-%d", prefix, size), func(b *testing.B) {
					batches := len(absent) / size
					for i := 0; i < b.N; i++ {
						at := (i % batches) * size
						filled.MayContainBatch(absent[at : at+size])
					}
					b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/key")
				})
			}
			b.Run(prefix+"/parallel-90check-10add", func(b *testing.B) {
				bf := impl.make()
				for _, key := range present[:len(present)/2] {
					bf.Add(key)
				}
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						if i%10 == 0 {
							bf.Add(absent[i%len(absent)])
						} else {
							bf.MayContain(present[i%len(present)])
						}
						i++
					}
				})
			})
		}
	}
}

// BenchmarkBloomCompareCallOverhead times the cheapest call each filter has.
// For Rust it is the cost of crossing from Go into Rust and back.
func BenchmarkBloomCompareCallOverhead(b *testing.B) {
	for _, impl := range bloomImpls() {
		bf := impl.make()
		b.Run(impl.name, func(b *testing.B) {
			if bf == nil {
				b.Skip("no Rust filter in this build (cgo off)")
			}
			for i := 0; i < b.N; i++ {
				_ = bf.Count()
			}
		})
	}
}

// BenchmarkBloomCompareAccuracy fills each filter to its capacity and reports
// the false positive rate among keys never added, and the filter's memory.
func BenchmarkBloomCompareAccuracy(b *testing.B) {
	added := compareKeys("present", compareCapacity, false)
	probes := compareKeys("absent", compareCapacity, false)
	for _, impl := range bloomImpls() {
		b.Run(impl.name, func(b *testing.B) {
			bf := impl.make()
			if bf == nil {
				b.Skip("no Rust filter in this build (cgo off)")
			}
			for _, key := range added {
				bf.Add(key)
			}
			falsePositives := 0
			for _, key := range probes {
				if bf.MayContain(key) {
					falsePositives++
				}
			}
			for i := 0; i < b.N; i++ {
				_ = bf.Count() // the work is above; this keeps the harness happy
			}
			b.ReportMetric(100*float64(falsePositives)/float64(len(probes)), "fp-%")
			b.ReportMetric(float64(bf.MemoryUsageBytes())/(1<<20), "MiB")
		})
	}
}
