//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	partitionCount = 4
	// readyTimeout bounds how long a node may take to serve again after it was
	// started, which after a crash includes replaying its logs and rejoining.
	readyTimeout = 90 * time.Second
)

var (
	binaryOnce sync.Once
	binaryPath string
	binaryDir  string
	binaryErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if binaryDir != "" {
		_ = os.RemoveAll(binaryDir)
	}
	os.Exit(code)
}

// serverBinary returns the server to run: the one CRONOS_BINARY names, or one
// built from this checkout.
func serverBinary(t *testing.T) string {
	t.Helper()
	binaryOnce.Do(func() {
		if path := os.Getenv("CRONOS_BINARY"); path != "" {
			binaryPath, binaryErr = filepath.Abs(path)
			return
		}
		root, err := repoRoot()
		if err != nil {
			binaryErr = err
			return
		}
		if binaryDir, err = os.MkdirTemp("", "cronos-acceptance-bin-"); err != nil {
			binaryErr = err
			return
		}
		binaryPath = filepath.Join(binaryDir, "cronos-api")
		if runtime.GOOS == "windows" {
			binaryPath += ".exe"
		}
		build := exec.Command("go", "build", "-o", binaryPath, "./cmd/api")
		build.Dir = root
		if out, err := build.CombinedOutput(); err != nil {
			binaryErr = fmt.Errorf("build the server: %v\n%s", err, out)
		}
	})
	if binaryErr != nil {
		t.Fatal(binaryErr)
	}
	return binaryPath
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// node is one server process and the addresses it was given.
type node struct {
	index   int
	id      string
	dataDir string
	logPath string

	grpcAddr    string
	httpAddr    string
	gossipAddr  string
	clusterAddr string
	raftAddr    string

	mu     sync.Mutex
	cmd    *exec.Cmd
	exited chan struct{}
	frozen bool
}

func (n *node) running() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.cmd != nil && !n.frozen
}

// cluster is a three-node CronosDB cluster running as local processes.
type cluster struct {
	t      *testing.T
	binary string
	dir    string
	nodes  []*node
	// extraArgs are appended to every node's command line.
	extraArgs []string
}

// newCluster prepares three nodes without starting them. Whatever is still
// running when the test ends is killed, and the node logs are shown if the
// test failed.
func newCluster(t *testing.T, extraArgs ...string) *cluster {
	t.Helper()
	c := &cluster{t: t, binary: serverBinary(t), dir: t.TempDir(), extraArgs: extraArgs}
	ports := freePorts(t, 15)
	if err := os.MkdirAll(filepath.Join(c.dir, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		addr := func(k int) string { return fmt.Sprintf("127.0.0.1:%d", ports[i*5+k]) }
		id := fmt.Sprintf("node%d", i+1)
		c.nodes = append(c.nodes, &node{
			index:       i,
			id:          id,
			dataDir:     filepath.Join(c.dir, id),
			logPath:     filepath.Join(c.dir, "logs", id+".log"),
			grpcAddr:    addr(0),
			httpAddr:    addr(1),
			gossipAddr:  addr(2),
			clusterAddr: addr(3),
			raftAddr:    addr(4),
		})
	}
	// Registered after TempDir, so it runs before the directory is removed.
	t.Cleanup(func() {
		for _, n := range c.nodes {
			c.kill(n)
		}
		c.keepLogs()
	})
	return c
}

func freePorts(t *testing.T, count int) []int {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	ports := make([]int, 0, count)
	for i := 0; i < count; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	return ports
}

// args is the command line of a node. It follows the layout of the production
// chart: the first node names no seeds and creates the cluster when it has no
// state, the others name every node.
func (c *cluster) args(n *node) []string {
	args := []string{
		"--dev",
		"--node-id=" + n.id,
		"--auth-enabled=false",
		"--cluster",
		fmt.Sprintf("--partition-count=%d", partitionCount),
		"--replication-factor=3",
		"--min-insync-replicas=2",
		"--cluster-expected-nodes=3",
		"--fsync-mode=batch",
		"--segment-size=1048576",
		"--bloom-capacity=200000",
		"--follower-reads=true",
		"--ack-timeout=5s",
		"--data-dir=" + n.dataDir,
		"--grpc-addr=" + n.grpcAddr,
		"--http-addr=" + n.httpAddr,
		"--cluster-gossip-addr=" + n.gossipAddr,
		"--cluster-grpc-addr=" + n.clusterAddr,
		"--cluster-raft-addr=" + n.raftAddr,
	}
	if n.index > 0 {
		seeds := make([]string, len(c.nodes))
		for i, other := range c.nodes {
			seeds[i] = other.gossipAddr
		}
		args = append(args, "--cluster-seeds="+strings.Join(seeds, ","))
	}
	return append(args, c.extraArgs...)
}

func (c *cluster) note(n *node, format string, a ...any) {
	line := fmt.Sprintf("=== harness %s: %s ===\n", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, a...))
	if f, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
		_, _ = f.WriteString(line)
		_ = f.Close()
	}
}

// start runs the node's process. Its output is appended to the node's log.
func (c *cluster) start(n *node) {
	c.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd != nil {
		c.t.Fatalf("%s is already running", n.id)
	}
	c.note(n, "starting")
	logFile, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		c.t.Fatal(err)
	}
	cmd := exec.Command(c.binary, c.args(n)...)
	cmd.Dir = c.dir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		c.t.Fatalf("start %s: %v", n.id, err)
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		_ = logFile.Close()
		close(exited)
	}()
	n.cmd, n.exited, n.frozen = cmd, exited, false
}

// kill ends the node's process at once, as a crash or a lost machine does:
// nothing is flushed and nobody is told.
func (c *cluster) kill(n *node) {
	n.mu.Lock()
	cmd, exited := n.cmd, n.exited
	n.cmd, n.exited, n.frozen = nil, nil, false
	n.mu.Unlock()
	if cmd == nil {
		return
	}
	_ = thawProcess(cmd.Process) // a frozen process cannot die on every platform
	_ = cmd.Process.Kill()
	select {
	case <-exited:
	case <-time.After(30 * time.Second):
		c.t.Errorf("%s did not exit after being killed", n.id)
	}
	c.note(n, "killed")
}

// wipe removes everything the node has stored, as losing its disk does. The
// node must not be running.
func (c *cluster) wipe(n *node) {
	c.t.Helper()
	if n.running() {
		c.t.Fatalf("%s is running", n.id)
	}
	if err := os.RemoveAll(n.dataDir); err != nil {
		c.t.Fatalf("remove the data of %s: %v", n.id, err)
	}
	c.note(n, "data removed")
}

// freeze stops the node's process from running until thaw.
func (c *cluster) freeze(n *node) {
	c.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil {
		c.t.Fatalf("%s is not running", n.id)
	}
	if err := freezeProcess(n.cmd.Process); err != nil {
		c.t.Fatalf("freeze %s: %v", n.id, err)
	}
	n.frozen = true
	c.note(n, "frozen")
}

func (c *cluster) thaw(n *node) {
	c.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil {
		c.t.Fatalf("%s is not running", n.id)
	}
	if err := thawProcess(n.cmd.Process); err != nil {
		c.t.Fatalf("thaw %s: %v", n.id, err)
	}
	n.frozen = false
	c.note(n, "thawed")
}

// ready reports whether the node answers its readiness check: it is part of
// the cluster and every partition it should serve has a leader and is loaded.
func (c *cluster) ready(n *node) (bool, string) {
	httpClient := http.Client{Timeout: 2 * time.Second}
	resp, err := httpClient.Get("http://" + n.httpAddr + "/health/ready")
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return resp.StatusCode == http.StatusOK, strings.TrimSpace(string(body))
}

// waitReady waits until every given node is ready.
func (c *cluster) waitReady(nodes ...*node) {
	c.t.Helper()
	deadline := time.Now().Add(readyTimeout)
	for _, n := range nodes {
		for {
			ok, detail := c.ready(n)
			if ok {
				break
			}
			select {
			case <-n.exitedChan():
				c.t.Fatalf("%s exited while it was expected to become ready", n.id)
			default:
			}
			if time.Now().After(deadline) {
				c.t.Fatalf("%s is not ready after %s: %s", n.id, readyTimeout, detail)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}

// exitedChan is closed when the node's current process has ended. It blocks
// forever for a node that is not running.
func (n *node) exitedChan() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.exited
}

// startAll starts every node and waits until the cluster serves.
func (c *cluster) startAll() {
	c.t.Helper()
	for _, n := range c.nodes {
		c.start(n)
	}
	c.waitReady(c.nodes...)
}

func (c *cluster) addresses() []string {
	addrs := make([]string, len(c.nodes))
	for i, n := range c.nodes {
		addrs[i] = n.grpcAddr
	}
	return addrs
}

func (c *cluster) nodeByID(id string) *node {
	for _, n := range c.nodes {
		if n.id == id {
			return n
		}
	}
	return nil
}

// dial connects a client to the cluster by its bootstrap addresses alone, as
// an application does.
func (c *cluster) dial() *client.Client {
	c.t.Helper()
	cfg := client.DefaultConfig(c.addresses()...)
	// Short enough that a publish has time to go on to another node when the
	// first one it tries does not answer.
	cfg.RequestTimeout = 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := client.Dial(ctx, cfg)
	if err != nil {
		c.t.Fatalf("connect to the cluster: %v", err)
	}
	c.t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// leaderOf returns the running node that leads the partition, waiting for one
// to be assigned.
func (c *cluster) leaderOf(observer *client.Client, partitionID int32) *node {
	c.t.Helper()
	deadline := time.Now().Add(readyTimeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := observer.ForceMetadataRefresh(ctx)
		cancel()
		if err == nil {
			for _, meta := range observer.PartitionMetadata() {
				if meta.PartitionID != partitionID {
					continue
				}
				if n := c.nodeByID(meta.LeaderID); n != nil && n.running() {
					return n
				}
			}
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("partition %d has no running leader after %s (last error: %v)", partitionID, readyTimeout, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// readLog returns the node's whole log of a partition, read from that node
// whether or not it leads the partition.
func (c *cluster) readLog(n *node, partitionID int32, topic string) ([]*types.Event, error) {
	conn, err := grpc.NewClient(n.grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	events := types.NewEventServiceClient(conn)

	var log []*types.Event
	next := int64(0)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		stream, err := events.Replay(ctx, &types.ReplayRequest{Topic: topic, PartitionId: partitionID, StartOffset: next, Count: 1000})
		if err != nil {
			cancel()
			return nil, err
		}
		read := 0
		for {
			item, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				cancel()
				return nil, err
			}
			log = append(log, item.GetEvent())
			read++
		}
		cancel()
		if read == 0 {
			return log, nil
		}
		next = log[len(log)-1].GetOffset() + 1
	}
}

// keepLogs copies the node logs to CRONOS_ACCEPTANCE_ARTIFACTS and, when the
// test failed, prints the end of each.
func (c *cluster) keepLogs() {
	artifacts := os.Getenv("CRONOS_ACCEPTANCE_ARTIFACTS")
	for _, n := range c.nodes {
		data, err := os.ReadFile(n.logPath)
		if err != nil {
			continue
		}
		if artifacts != "" {
			dir := filepath.Join(artifacts, strings.ReplaceAll(c.t.Name(), "/", "_"))
			if err := os.MkdirAll(dir, 0o755); err == nil {
				_ = os.WriteFile(filepath.Join(dir, n.id+".log"), data, 0o644)
			}
		}
		if c.t.Failed() {
			const tail = 12000
			if len(data) > tail {
				data = data[len(data)-tail:]
			}
			c.t.Logf("---- end of the log of %s ----\n%s", n.id, data)
		}
	}
}
