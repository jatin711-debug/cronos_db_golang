//go:build acceptance && linux

package acceptance

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Power cuts, simulated with LazyFS (https://github.com/dsrhaslab/lazyfs), a
// FUSE filesystem that keeps written data in its own page cache until it is
// synced and can be told to drop what was not: the data a machine loses when
// its power goes. Killing a process, which the other tests do, loses nothing
// the process wrote, synced or not, because the kernel still writes it out.
// Jepsen found with LazyFS that a power cut of one node, or of all of them
// at once, loses acknowledged writes in systems whose kill tests pass.
//
// Each node keeps its data on a LazyFS mount. A cut kills a node and drops
// its unsynced writes; the last round cuts all three at once. With
// every_event fsync, an event is acknowledged only once a majority of the
// replicas has synced it, so what must hold is what holds for the other
// faults: nothing acknowledged is lost, nothing is delivered early, and the
// replicas end with the same log.
//
// It needs Linux, FUSE and a LazyFS build, and is opt-in:
//
//	CRONOS_LAZYFS_BIN=/path/to/lazyfs/lazyfs/build/lazyfs \
//	  go test -tags acceptance -run '^TestPowerLoss$' -count=1 -v -timeout 40m ./tests/acceptance/
//
// Settings (environment):
//
//	CRONOS_LAZYFS_BIN          the lazyfs executable (required)
//	CRONOS_LAZYFS_CONFIG       a LazyFS configuration to use instead of the one
//	                           written here, with {{FIFO}} where the fault FIFO goes
//	CRONOS_LAZYFS_UNMOUNT      the unmount command (fusermount3 -u, else fusermount -u)
//	CRONOS_POWERLOSS_ROUNDS    single-node cuts before the cut of all nodes (6)
//	CRONOS_POWERLOSS_FSYNC     the nodes' --fsync-mode (every_event); batch is the
//	                           production default, and periodic, which acknowledges
//	                           before it syncs, should fail: a check of the test itself
//
// The configuration written here follows LazyFS 0.2's default.toml; another
// version may name its settings differently, so give it CRONOS_LAZYFS_CONFIG.
func TestPowerLoss(t *testing.T) {
	bin := os.Getenv("CRONOS_LAZYFS_BIN")
	if bin == "" {
		t.Skip("set CRONOS_LAZYFS_BIN to a LazyFS executable to run the power-loss test")
	}
	rounds := 6
	if raw := os.Getenv("CRONOS_POWERLOSS_ROUNDS"); raw != "" {
		if _, err := fmt.Sscan(raw, &rounds); err != nil || rounds < 1 {
			t.Fatalf("CRONOS_POWERLOSS_ROUNDS=%q", raw)
		}
	}
	fsync := "every_event"
	if mode := os.Getenv("CRONOS_POWERLOSS_FSYNC"); mode != "" {
		fsync = mode
	}
	t.Logf("nodes sync with --fsync-mode=%s", fsync)
	c := newCluster(t, "--fsync-mode="+fsync)
	mounts := make(map[string]*lazyMount, len(c.nodes))
	for _, n := range c.nodes {
		m := mountLazyFS(t, bin, filepath.Join(c.dir, "lazyfs-"+n.id))
		mounts[n.id] = m
		n.dataDir = m.dir
	}
	// Cleanups run last first: the nodes stop before their mounts go.
	t.Cleanup(func() {
		for _, n := range c.nodes {
			c.kill(n)
		}
	})

	c.startAll()
	w := startWorkload(t, c, "powerloss", "powerloss-workers", 2, 20, 2)
	accepted := func() int64 { return w.ledger.accepted.Load() }
	delivered := func() int64 { return w.ledger.delivered.Load() }
	w.waitProgress(accepted, 100, 60*time.Second, "publishes before the first power cut")
	w.waitProgress(delivered, 50, 60*time.Second, "deliveries before the first power cut")

	// cut kills nodes and drops whatever they had not synced, as a power cut
	// does, then starts them again.
	cut := func(nodes ...*node) {
		t.Helper()
		for _, n := range nodes {
			c.kill(n)
		}
		for _, n := range nodes {
			mounts[n.id].dropUnsynced(t)
			c.note(n, "power cut: unsynced writes dropped")
		}
		for _, n := range nodes {
			c.start(n)
		}
		c.waitReady(c.nodes...)
		w.waitProgress(accepted, 50, 90*time.Second, "publishes after the power cut")
		w.waitProgress(delivered, 50, 90*time.Second, "deliveries after the power cut")
	}

	observer := c.dial()
	for round := 0; round < rounds; round++ {
		// Leaders first: a leader's unsynced tail is what a careless design
		// acknowledges.
		victim := c.nodes[round%len(c.nodes)]
		if round%2 == 0 {
			victim = c.leaderOf(observer, int32(round%partitionCount))
		}
		step(t, "power cut %d: %s", round+1, victim.id)
		cut(victim)
	}
	step(t, "power cut of every node at once")
	cut(c.nodes...)

	step(t, "power cuts done; waiting for the remaining deliveries")
	w.stopPublishers()
	checkDelivery(t, w)
	w.stopConsumers()
	checkLogs(t, c, w.topic, w.ledger, false)
}

// lazyMount is a LazyFS mount: dir is where the node keeps its data, root is
// where LazyFS stores it, and fifo takes LazyFS's fault commands.
type lazyMount struct {
	dir, root, fifo string
	cmd             *exec.Cmd
	out             *bytes.Buffer
}

func mountLazyFS(t *testing.T, bin, base string) *lazyMount {
	t.Helper()
	m := &lazyMount{
		dir:  filepath.Join(base, "mnt"),
		root: filepath.Join(base, "root"),
		fifo: filepath.Join(base, "faults.fifo"),
		out:  new(bytes.Buffer),
	}
	for _, dir := range []string{m.dir, m.root} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	config := filepath.Join(base, "lazyfs.toml")
	template := lazyFSConfig
	if path := os.Getenv("CRONOS_LAZYFS_CONFIG"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		template = string(raw)
	}
	if err := os.WriteFile(config, []byte(strings.ReplaceAll(template, "{{FIFO}}", m.fifo)), 0o644); err != nil {
		t.Fatal(err)
	}
	m.cmd = exec.Command(bin, m.dir, "--config-path", config, "-o", "modules=subdir", "-o", "subdir="+m.root, "-f")
	m.cmd.Stdout, m.cmd.Stderr = m.out, m.out
	if err := m.cmd.Start(); err != nil {
		t.Fatalf("start LazyFS: %v", err)
	}
	t.Cleanup(func() { m.unmount(t) })
	deadline := time.Now().Add(30 * time.Second)
	for !mounted(m.dir) {
		if time.Now().After(deadline) {
			t.Fatalf("LazyFS did not mount %s:\n%s", m.dir, m.out.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	return m
}

// dropUnsynced tells LazyFS to forget what was written to the mount and not
// synced. The node must have stopped.
func (m *lazyMount) dropUnsynced(t *testing.T) {
	t.Helper()
	f, err := os.OpenFile(m.fifo, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open LazyFS fault FIFO: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteString("lazyfs::clear-cache\n"); err != nil {
		t.Fatalf("drop unsynced writes: %v", err)
	}
	time.Sleep(500 * time.Millisecond) // LazyFS applies the command asynchronously
}

func (m *lazyMount) unmount(t *testing.T) {
	command := []string{"fusermount3", "-u", m.dir}
	if raw := os.Getenv("CRONOS_LAZYFS_UNMOUNT"); raw != "" {
		command = append(strings.Fields(raw), m.dir)
	} else if _, err := exec.LookPath("fusermount3"); err != nil {
		command = []string{"fusermount", "-u", m.dir}
	}
	if out, err := exec.Command(command[0], command[1:]...).CombinedOutput(); err != nil && mounted(m.dir) {
		t.Errorf("unmount %s: %v: %s", m.dir, err, out)
	}
	done := make(chan struct{})
	go func() { _ = m.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = m.cmd.Process.Kill()
	}
}

// mounted reports whether dir is a mount point.
func mounted(dir string) bool {
	raw, err := os.ReadFile("/proc/self/mounts")
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if fields := strings.Fields(line); len(fields) > 1 && fields[1] == dir {
			return true
		}
	}
	return false
}

// lazyFSConfig follows LazyFS 0.2's config/default.toml, with the fault FIFO
// at {{FIFO}}. The cache must hold what the nodes write between syncs.
const lazyFSConfig = `[faults]
fifo_path="{{FIFO}}"

[cache]
apply_eviction=false

[cache.simple]
custom_size="1gb"
blocks_per_page=1

[filesystem]
log_all_operations=false
logfile=""
`
