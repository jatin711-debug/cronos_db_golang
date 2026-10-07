//go:build acceptance

package acceptance

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A cluster can also run as three containers of the image that is shipped,
// on a network of their own. That is slower to set up than three processes
// and needs Docker, and it is the only way here to do to the network what
// networks do: lose what two nodes send each other while both keep running
// and everybody else still reaches them. Processes on one machine cannot be
// cut off from one another.

// firewallImage holds the tool that drops packets. The node image does not,
// and should not; the rules are set from a second container that shares the
// node's network namespace.
const firewallImage = "cronos-netfault:local"

// docker runs the docker command and returns what it printed.
func docker(args ...string) (string, error) {
	out, err := exec.Command("docker", args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func (c *cluster) docker(args ...string) string {
	c.t.Helper()
	out, err := docker(args...)
	if err != nil {
		c.t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// containerImage returns the image the nodes run. The tests that need
// containers are skipped where none is named or there is no Docker.
func containerImage(t *testing.T) string {
	t.Helper()
	image := os.Getenv("CRONOS_IMAGE")
	if image == "" {
		t.Skip("CRONOS_IMAGE names no image; this test runs the nodes as containers of one")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	if out, err := docker("image", "inspect", "--format", "{{.Id}}", image); err != nil {
		t.Fatalf("the image %s is not on this machine: %v\n%s", image, err, out)
	}
	return image
}

// ensureFirewallImage builds the image the packet rules are set from, once.
func ensureFirewallImage(t *testing.T) {
	t.Helper()
	if _, err := docker("image", "inspect", "--format", "{{.Id}}", firewallImage); err == nil {
		return
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM alpine:3.20\nRUN apk add --no-cache iptables\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := docker("build", "-q", "-t", firewallImage, dir); err != nil {
		t.Fatalf("build %s: %v\n%s", firewallImage, err, out)
	}
}

// newContainerCluster prepares three nodes as containers without starting
// them. They reach each other by name on a network of their own, and this
// machine reaches each through two published ports. The containers and the
// network are removed when the test ends, and the node logs are kept as for
// a cluster of processes.
func newContainerCluster(t *testing.T, extraArgs ...string) *cluster {
	t.Helper()
	image := containerImage(t)
	ensureFirewallImage(t)

	run := fmt.Sprintf("cronos-acc-%d", time.Now().UnixNano()%1_000_000_000)
	c := &cluster{t: t, dir: t.TempDir(), extraArgs: extraArgs, image: image, network: run}
	c.certDir = filepath.Join(c.dir, "certs")
	for _, dir := range []string{filepath.Join(c.dir, "logs"), c.certDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	ports := freePorts(t, 6)
	names := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node%d", i+1)
		name := run + "-" + id
		names = append(names, name)
		c.nodes = append(c.nodes, &node{
			index:       i,
			id:          id,
			container:   name,
			logPath:     filepath.Join(c.dir, "logs", id+".log"),
			grpcAddr:    fmt.Sprintf("127.0.0.1:%d", ports[i*2]),
			httpAddr:    fmt.Sprintf("127.0.0.1:%d", ports[i*2+1]),
			gossipAddr:  name + ":7946",
			clusterAddr: name + ":7947",
			raftAddr:    name + ":7948",
		})
	}
	// The image runs as a user of its own, which has to be able to read what
	// is mounted into it.
	c.caFile, c.certFile, c.keyFile = nodeCertificates(t, c.certDir, names...)
	for _, path := range []string{c.certDir, c.caFile, c.certFile, c.keyFile} {
		mode := os.FileMode(0o644)
		if path == c.certDir {
			mode = 0o755
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
	}

	c.docker("network", "create", c.network)
	t.Cleanup(func() {
		for _, n := range c.nodes {
			c.removeContainer(n)
		}
		_, _ = docker("network", "rm", c.network)
		c.keepLogs()
	})
	return c
}

// startContainer runs the node's container, or starts it again with the data
// it had when it was killed.
func (c *cluster) startContainer(n *node) {
	c.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.up {
		c.t.Fatalf("%s is already running", n.id)
	}
	c.note(n, "starting")
	if n.created {
		c.docker("start", n.container)
	} else {
		args := []string{"run", "-d", "--name", n.container, "--network", c.network,
			"-p", n.grpcAddr + ":9000", "-p", n.httpAddr + ":8080",
			"-v", c.certDir + ":/certs:ro", c.image}
		c.docker(append(args, c.args(n)...)...)
		n.created = true
	}
	n.up, n.frozen = true, false
}

// killContainer ends the node at once. Its data stays in the container.
func (c *cluster) killContainer(n *node) {
	n.mu.Lock()
	up := n.up
	n.up, n.frozen = false, false
	n.mu.Unlock()
	if !up {
		return
	}
	if out, err := docker("kill", n.container); err != nil {
		c.t.Errorf("kill %s: %v\n%s", n.id, err, out)
	}
	c.note(n, "killed")
}

// removeContainer removes the node's container with everything it stored,
// after saving what it logged.
func (c *cluster) removeContainer(n *node) {
	n.mu.Lock()
	created := n.created
	n.created, n.up, n.frozen = false, false, false
	n.mu.Unlock()
	if !created {
		return
	}
	if logs, err := docker("logs", n.container); err == nil {
		if f, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			_, _ = f.WriteString(logs + "\n")
			_ = f.Close()
		}
	}
	_, _ = docker("rm", "-f", "-v", n.container)
}

// address returns the node's address on the cluster's network.
func (c *cluster) address(n *node) string {
	c.t.Helper()
	return c.docker("inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", n.container)
}

// firewall runs a command in the node's network namespace.
func (c *cluster) firewall(n *node, command ...string) {
	c.t.Helper()
	c.docker(append([]string{"run", "--rm", "--network", "container:" + n.container, "--cap-add", "NET_ADMIN", firewallImage}, command...)...)
}

// cut makes the two nodes lose everything they send each other, in both
// directions and without a word, as a failed link does: connections that are
// open stay open and carry nothing, and new ones are never answered. The
// packets are dropped where they arrive, so the sender is told nothing.
// Everybody else still reaches both.
func (c *cluster) cut(a, b *node) {
	c.t.Helper()
	c.firewall(a, "iptables", "-A", "INPUT", "-s", c.address(b), "-j", "DROP")
	c.firewall(b, "iptables", "-A", "INPUT", "-s", c.address(a), "-j", "DROP")
	c.note(a, "link to %s cut", b.id)
	c.note(b, "link to %s cut", a.id)
}

// heal restores every link that was cut.
func (c *cluster) heal() {
	c.t.Helper()
	for _, n := range c.nodes {
		if n.running() {
			c.firewall(n, "iptables", "-F", "INPUT")
			c.note(n, "links restored")
		}
	}
}

// others returns the nodes that are not n.
func (c *cluster) others(n *node) []*node {
	var rest []*node
	for _, other := range c.nodes {
		if other != n {
			rest = append(rest, other)
		}
	}
	return rest
}
