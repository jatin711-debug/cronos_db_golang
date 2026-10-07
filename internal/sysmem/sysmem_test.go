package sysmem

import (
	"os"
	"path/filepath"
	"testing"
)

func file(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "value")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func point(t *testing.T, v2, v1, status string) {
	t.Helper()
	oldV2, oldV1, oldStatus := cgroupV2Limit, cgroupV1Limit, procStatus
	cgroupV2Limit, cgroupV1Limit, procStatus = v2, v1, status
	t.Cleanup(func() { cgroupV2Limit, cgroupV1Limit, procStatus = oldV2, oldV1, oldStatus })
}

// A container's limit is the limit, not the memory of the machine under it.
// The guard that refuses publishes when memory runs short compared the whole
// machine's use with a percentage, and on a large machine a container was
// killed long before that percentage was reached.
func TestLimit_IsTheContainersWhenThereIsOne(t *testing.T) {
	none := filepath.Join(t.TempDir(), "absent")
	const fourGiB = uint64(4) << 30

	point(t, file(t, "4294967296\n"), none, none)
	if limit, source := Limit(0); limit != fourGiB || source != "cgroup" {
		t.Fatalf("with a cgroup v2 limit of 4 GiB: %d from %q", limit, source)
	}
	point(t, none, file(t, "4294967296\n"), none)
	if limit, source := Limit(0); limit != fourGiB || source != "cgroup" {
		t.Fatalf("with a cgroup v1 limit of 4 GiB: %d from %q", limit, source)
	}

	// No limit set: v2 says "max", v1 a number near the top of the address space.
	point(t, file(t, "max\n"), file(t, "9223372036854771712\n"), none)
	limit, source := Limit(0)
	if source != "machine" || limit == 0 {
		t.Fatalf("without a cgroup limit: %d from %q, want the machine's memory", limit, source)
	}
	machine := limit

	// A limit above what the machine has is no limit.
	point(t, file(t, "1152921504606846975\n"), none, none)
	if limit, source := Limit(0); limit != machine || source != "machine" {
		t.Fatalf("with a cgroup limit above the machine's memory: %d from %q", limit, source)
	}

	// What the operator says wins.
	if limit, source := Limit(123); limit != 123 || source != "configured" {
		t.Fatalf("with a configured limit: %d from %q", limit, source)
	}
}

func TestUsed_ReadsAnonymousMemoryWhereTheSystemReportsIt(t *testing.T) {
	none := filepath.Join(t.TempDir(), "absent")
	point(t, none, none, file(t, "Name:\tcronos-api\nVmRSS:\t  900000 kB\nRssAnon:\t  500000 kB\nRssFile:\t  400000 kB\n"))
	anonymous, ok := anonymousResident()
	if !ok || anonymous != 500000*1024 {
		t.Fatalf("anonymous memory read as %d (%v), want 512000000", anonymous, ok)
	}
	if used := Used(); used == 0 || used > anonymous {
		t.Fatalf("used %d, want above zero and at most the anonymous memory %d", used, anonymous)
	}

	point(t, none, none, none)
	if _, ok := anonymousResident(); ok {
		t.Fatal("anonymous memory reported without a status file")
	}
	if used := Used(); used == 0 {
		t.Fatal("no memory in use reported where the system says nothing")
	}
}
