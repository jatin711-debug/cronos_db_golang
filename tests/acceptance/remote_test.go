//go:build acceptance

package acceptance

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The soak on separate hosts. Everything else in this package starts the
// nodes as local processes or containers. With CRONOS_SOAK_REMOTE naming a
// JSON file, TestSoak drives a cluster whose nodes run elsewhere: it keeps
// its ledger from this machine, sends its publishes and consumes from here,
// and does its disruptions with commands the file gives for each node, for
// example over ssh. The file:
//
//	{
//	  "ca": "certs/ca.crt", "cert": "certs/client.crt", "key": "certs/client.key",
//	  "encryption_key_file": "certs/wal.key",
//	  "cdc_listen": "0.0.0.0:18555", "cdc_url": "http://10.0.0.9:18555/cdc",
//	  "nodes": [
//	    {"id": "node1", "grpc": "10.0.0.1:9000", "http": "10.0.0.1:8080", "cluster": "10.0.0.1:7947",
//	     "start": "ssh n1 sudo systemctl start cronos",
//	     "kill": "ssh n1 sudo systemctl kill -s KILL cronos",
//	     "freeze": "ssh n1 sudo systemctl kill -s STOP cronos",
//	     "thaw": "ssh n1 sudo systemctl kill -s CONT cronos",
//	     "wipe": "ssh n1 sudo sh -c 'rm -rf /var/lib/cronos/*'",
//	     "disk": "ssh n1 du -sb /var/lib/cronos | cut -f1",
//	     "logs": "ssh n1 journalctl -u cronos --no-pager -n 2000"},
//	    ...
//	  ]
//	}
//
// The nodes must run with the settings TestSoak gives local nodes (see
// docs/FAULT_TESTING.md): the replication certificates whose CA is "ca",
// encryption with the key in "encryption_key_file", "--auth-enabled=false",
// and CRONOS_CDC_WEBHOOK_URL set to "cdc_url". "cert" and "key" are a
// certificate of that CA that this machine reads the replicas' logs with.
// Commands run with sh -c (cmd /c on Windows); "disk" prints the bytes the
// node's data takes, and "logs" what to keep of its log. Relative paths are
// relative to the file.

// remoteNode is one node of a remote cluster: its addresses and the commands
// that act on it.
type remoteNode struct {
	ID      string `json:"id"`
	GRPC    string `json:"grpc"`
	HTTP    string `json:"http"`
	Cluster string `json:"cluster"`
	Start   string `json:"start"`
	Kill    string `json:"kill"`
	Freeze  string `json:"freeze"`
	Thaw    string `json:"thaw"`
	Wipe    string `json:"wipe"`
	Disk    string `json:"disk"`
	Logs    string `json:"logs"`
}

type remoteConfig struct {
	CA                string       `json:"ca"`
	Cert              string       `json:"cert"`
	Key               string       `json:"key"`
	EncryptionKeyFile string       `json:"encryption_key_file"`
	CDCListen         string       `json:"cdc_listen"`
	CDCURL            string       `json:"cdc_url"`
	Nodes             []remoteNode `json:"nodes"`
}

// loadRemote reads CRONOS_SOAK_REMOTE, or returns nil without it.
func loadRemote(t *testing.T) *remoteConfig {
	t.Helper()
	path := os.Getenv("CRONOS_SOAK_REMOTE")
	if path == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rc remoteConfig
	if err := json.Unmarshal(raw, &rc); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	base := filepath.Dir(path)
	for _, p := range []*string{&rc.CA, &rc.Cert, &rc.Key, &rc.EncryptionKeyFile} {
		if *p != "" && !filepath.IsAbs(*p) {
			*p = filepath.Join(base, *p)
		}
	}
	if len(rc.Nodes) != 3 {
		t.Fatalf("%s names %d nodes; the soak runs three", path, len(rc.Nodes))
	}
	for _, n := range rc.Nodes {
		if n.ID == "" || n.GRPC == "" || n.HTTP == "" || n.Cluster == "" || n.Start == "" || n.Kill == "" {
			t.Fatalf("%s: node %+v needs id, grpc, http, cluster, start and kill", path, n)
		}
	}
	if rc.CA == "" || rc.Cert == "" || rc.Key == "" || rc.CDCListen == "" || rc.CDCURL == "" {
		t.Fatalf("%s needs ca, cert, key, cdc_listen and cdc_url", path)
	}
	return &rc
}

// newRemoteCluster is a cluster of nodes that already run, or are started by
// their start commands, elsewhere.
func newRemoteCluster(t *testing.T, rc *remoteConfig) *cluster {
	t.Helper()
	c := &cluster{t: t, dir: t.TempDir(), caFile: rc.CA, certFile: rc.Cert, keyFile: rc.Key}
	if err := os.MkdirAll(filepath.Join(c.dir, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if rc.EncryptionKeyFile != "" {
		key, err := os.ReadFile(rc.EncryptionKeyFile)
		if err != nil {
			t.Fatal(err)
		}
		c.encryptionKey = []byte(strings.TrimSpace(string(key)))
	}
	for i := range rc.Nodes {
		rn := rc.Nodes[i]
		c.nodes = append(c.nodes, &node{
			index:           i,
			id:              rn.ID,
			logPath:         filepath.Join(c.dir, "logs", rn.ID+".log"),
			grpcAddr:        rn.GRPC,
			httpAddr:        rn.HTTP,
			clusterAddr:     rn.Cluster,
			clusterDialAddr: rn.Cluster,
			remote:          &rn,
		})
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			if n.remote.Logs != "" {
				if out, err := runRemote(n.remote.Logs); err == nil {
					_ = os.WriteFile(n.logPath, []byte(out), 0o644)
				}
			}
		}
		c.keepLogs()
	})
	return c
}

// runRemote runs a node command and returns what it printed.
func runRemote(command string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	shell, flag := "sh", "-c"
	if runtime.GOOS == "windows" {
		shell, flag = "cmd", "/c"
	}
	out, err := exec.CommandContext(ctx, shell, flag, command).CombinedOutput()
	return string(out), err
}

// remote runs the command of a remote node for what, and fails the test if
// it fails. A command that is not configured fails the test too: the soak
// asked for something it cannot do on this cluster.
func (c *cluster) remote(n *node, what, command string) {
	c.t.Helper()
	if command == "" {
		c.t.Fatalf("%s: no %q command for remote node %s", what, what, n.id)
	}
	out, err := runRemote(command)
	if err != nil {
		c.t.Fatalf("%s %s: %v\n%s", what, n.id, err, out)
	}
	c.note(n, "%s (remote)", what)
}

// remoteDiskUse returns the bytes a remote node's data takes, from its disk
// command, or 0 without one.
func remoteDiskUse(n *node) int64 {
	if n.remote.Disk == "" {
		return 0
	}
	out, err := runRemote(n.remote.Disk)
	if err != nil {
		return 0
	}
	fields := strings.Fields(out)
	if len(fields) == 0 {
		return 0
	}
	v, _ := strconv.ParseInt(fields[0], 10, 64)
	return v
}
