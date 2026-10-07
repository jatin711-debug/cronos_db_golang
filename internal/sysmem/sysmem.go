// Package sysmem says how much memory this process may use and how much of
// it it holds.
//
// The numbers are for deciding when to stop taking work. They are about this
// process and its own limit: a node in a container with a 4 GiB limit is out
// of memory at 4 GiB however much the machine under it has free, and what
// the machine's other processes use is not this node's to give back.
package sysmem

import (
	"bytes"
	"os"
	"runtime/metrics"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/v3/mem"
)

// Paths are variables so that tests can point them at files of their own.
var (
	cgroupV2Limit = "/sys/fs/cgroup/memory.max"
	cgroupV1Limit = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	procStatus    = "/proc/self/status"
)

// unlimited is above any real limit. cgroup v1 reports "no limit" as a number
// near the top of the address space.
const unlimited = uint64(1) << 60

// Limit returns the memory this process may use, in bytes, and where the
// number comes from: "configured", "cgroup" or "machine". It is 0 with
// source "unknown" when nothing can be found out.
func Limit(configured uint64) (uint64, string) {
	if configured > 0 {
		return configured, "configured"
	}
	machine := uint64(0)
	if vm, err := mem.VirtualMemory(); err == nil {
		machine = vm.Total
	}
	for _, path := range []string{cgroupV2Limit, cgroupV1Limit} {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		limit, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		if err != nil || limit == 0 || limit >= unlimited {
			continue // "max", or no limit set
		}
		if machine == 0 || limit < machine {
			return limit, "cgroup"
		}
	}
	if machine > 0 {
		return machine, "machine"
	}
	return 0, "unknown"
}

// Sample is what the process holds at one moment, in bytes.
type Sample struct {
	// Held is what it holds and cannot hand back.
	Held uint64
	// Outside is the part of Held that is not the Go runtime's: what linked
	// libraries allocated. It is 0 where the system does not say.
	Outside uint64
}

// Used returns the memory this process holds and cannot hand back, in bytes.
func Used() uint64 { return Measure().Held }

// Measure returns what this process holds.
//
// On Linux that is its anonymous resident memory: the Go heap, and what the
// libraries it links allocate outside it. File pages are left out; the kernel
// takes them back when it needs to. Elsewhere it is what the Go runtime has
// mapped, which misses allocations made outside the runtime.
//
// Memory the Go runtime keeps for reuse after a collection is not counted
// either. The runtime gives it back or uses it again, and counting it would
// make a node that was busy a minute ago look full now.
func Measure() Sample {
	samples := []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/memory/classes/heap/free:bytes"},
	}
	metrics.Read(samples)
	value := func(i int) uint64 {
		if samples[i].Value.Kind() != metrics.KindUint64 {
			return 0
		}
		return samples[i].Value.Uint64()
	}
	mapped, released, kept := value(0), value(1), value(2)

	runtime := mapped - min(released, mapped)
	held, outside := runtime, uint64(0)
	if anonymous, ok := anonymousResident(); ok {
		held, outside = anonymous, anonymous-min(runtime, anonymous)
	}
	return Sample{Held: held - min(kept, held), Outside: outside}
}

// anonymousResident reads RssAnon from /proc/self/status.
func anonymousResident() (uint64, bool) {
	data, err := os.ReadFile(procStatus)
	if err != nil {
		return 0, false
	}
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		rest, found := bytes.CutPrefix(line, []byte("RssAnon:"))
		if !found {
			continue
		}
		fields := strings.Fields(string(rest))
		if len(fields) == 0 {
			return 0, false
		}
		kilobytes, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return 0, false
		}
		return kilobytes * 1024, true
	}
	return 0, false
}
