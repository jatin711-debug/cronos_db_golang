package cluster

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/replication"
)

// nodeKeys is a CA and one certificate it signed, as the files a node is
// configured with.
type nodeKeys struct {
	ca, cert, key string
}

// newNodeKeys creates a CA and a certificate that is valid for the given
// hosts, for use by a server and by a client.
func newNodeKeys(t *testing.T, hosts ...string) nodeKeys {
	t.Helper()
	dir := t.TempDir()
	writePEM := func(name, kind string, der []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test cluster CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test node"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, host)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caTemplate, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return nodeKeys{
		ca:   writePEM("ca.crt", "CERTIFICATE", caDER),
		cert: writePEM("tls.crt", "CERTIFICATE", der),
		key:  writePEM("tls.key", "EC PRIVATE KEY", keyDER),
	}
}

// configs builds what a node listens and dials with, the way the server does
// from its --replication-tls-* settings.
func (k nodeKeys) configs(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	settings := &replication.MTLSConfig{Enabled: true, CAFile: k.ca, CertFile: k.cert, KeyFile: k.key}
	server, err := replication.BuildServerTLSConfig(settings)
	if err != nil {
		t.Fatal(err)
	}
	client, err = replication.BuildClientTLSConfig(settings)
	if err != nil {
		t.Fatal(err)
	}
	return server, client
}

func securedMember(t *testing.T, keys nodeKeys, id, addr string, seeds []string) *Membership {
	t.Helper()
	server, client := keys.configs(t)
	m, err := NewMembership(&ClusterConfig{
		NodeID:            id,
		BindAddr:          addr,
		GRPCAddr:          "grpc-of-" + id,
		RaftAddr:          "raft-of-" + id,
		SeedNodes:         seeds,
		HeartbeatInterval: 40 * time.Millisecond,
		ServerTLS:         server,
		ClientTLS:         client,
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// askToJoin does what a node does first: it connects to the membership port
// at addr, with the TLS configuration given or without TLS, asks to join as
// the node "intruder", and returns the answer.
func askToJoin(t *testing.T, addr string, clientTLS *tls.Config) (map[string]any, error) {
	t.Helper()
	var conn net.Conn
	var err error
	if clientTLS != nil {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, clientTLS)
	} else {
		conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
	}
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	request := &GossipMessage{Type: "join", NodeID: "intruder", Address: "intruder:1", GossipAddr: "intruder:2", RaftAddr: "intruder:3"}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return nil, err
	}
	var answer map[string]any
	if err := json.NewDecoder(conn).Decode(&answer); err != nil {
		return nil, err
	}
	return answer, nil
}

// Nodes that hold certificates of the cluster's CA find each other over
// mutual TLS, and their heartbeats keep arriving over it: a node that the
// other was made to count as dead is counted as alive again.
func TestTransportTLS_MembersJoinAndHearEachOther(t *testing.T) {
	keys := newNodeKeys(t, "127.0.0.1")
	addrA, addrB := freeAddr(t), freeAddr(t)
	a := securedMember(t, keys, "node-a", addrA, nil)
	b := securedMember(t, keys, "node-b", addrB, []string{addrA})
	startMember(t, a)
	startMember(t, b)
	eventually(t, "the two nodes know each other", func() bool { return alive(a, "node-b") && alive(b, "node-a") })

	if err := a.MarkDead("node-b"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a heartbeat from node-b reaches node-a", func() bool { return alive(a, "node-b") })
}

// Whoever can merely reach the membership port is not let in. The port used
// to be plain TCP even with replication TLS on, and a join request from
// anyone made the sender a member, which the Raft leader then added as a
// voter.
func TestTransportTLS_MembershipRefusesCallersWithoutTheClustersCertificate(t *testing.T) {
	keys := newNodeKeys(t, "127.0.0.1")
	addr := freeAddr(t)
	a := securedMember(t, keys, "node-a", addr, nil)
	startMember(t, a)

	caPEM, err := os.ReadFile(keys.ca)
	if err != nil {
		t.Fatal(err)
	}
	clusterCA := x509.NewCertPool()
	clusterCA.AppendCertsFromPEM(caPEM)
	_, member := keys.configs(t)
	_, stranger := newNodeKeys(t, "127.0.0.1").configs(t)
	// The stranger trusts the cluster's CA, so that what is tested is what
	// the node makes of the stranger and not the other way round.
	stranger.RootCAs = clusterCA

	callers := []struct {
		name      string
		clientTLS *tls.Config
	}{
		{"without TLS", nil},
		{"with TLS and no certificate", &tls.Config{RootCAs: clusterCA, MinVersion: tls.VersionTLS12}},
		{"with a certificate of another CA", stranger},
	}
	for _, caller := range callers {
		if answer, err := askToJoin(t, addr, caller.clientTLS); err == nil {
			t.Errorf("a caller %s was answered: %v", caller.name, answer)
		}
	}
	if knows(a, "intruder") {
		t.Fatal("a caller without the cluster's certificate became a member")
	}

	// The same request with a certificate of the cluster's CA is answered,
	// so the refusals above are about the certificate and nothing else.
	if answer, err := askToJoin(t, addr, member); err != nil || answer["success"] != true {
		t.Fatalf("a caller with the cluster's certificate was refused: %v %v", answer, err)
	}
	if !knows(a, "intruder") {
		t.Fatal("the caller with the cluster's certificate did not become a member")
	}
}

// A node checks that the certificate it is shown belongs to the host it
// called. A certificate of the cluster's CA that is valid for another host
// does not pass for this one.
func TestTransportTLS_MemberChecksTheNameOfTheNodeItCalls(t *testing.T) {
	elsewhere := newNodeKeys(t, "node.elsewhere.test")
	addrA, addrB := freeAddr(t), freeAddr(t)
	a := securedMember(t, elsewhere, "node-a", addrA, nil)
	b := securedMember(t, elsewhere, "node-b", addrB, nil)
	startMember(t, a)
	startMember(t, b)

	if _, err := b.joinViaNode(t.Context(), addrA); err == nil {
		t.Fatal("a node joined through a peer whose certificate is not valid for the peer's address")
	}
	if knows(a, "node-b") {
		t.Fatal("the join went through although the certificate was not valid for the address")
	}
}

func securedRaft(t *testing.T, keys nodeKeys, id string) *RaftNode {
	t.Helper()
	server, client := keys.configs(t)
	node, err := NewRaftNode(&ClusterConfig{
		NodeID:            id,
		RaftAddr:          freeAddr(t),
		RaftDataDir:       t.TempDir(),
		HeartbeatInterval: 500 * time.Millisecond,
		ElectionTimeout:   time.Second,
		ServerTLS:         server,
		ClientTLS:         client,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })
	return node
}

// Raft runs over mutual TLS: a second node is added, and what the leader
// commits reaches it.
func TestTransportTLS_RaftReplicatesOverTLS(t *testing.T) {
	keys := newNodeKeys(t, "127.0.0.1")
	first, second := securedRaft(t, keys, "node-a"), securedRaft(t, keys, "node-b")
	if err := first.Bootstrap(); err != nil {
		t.Fatal(err)
	}
	if err := first.WaitForLeader(15 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := first.Join("node-b", second.config.RaftAddr); err != nil {
		t.Fatal(err)
	}
	if err := first.ProposePartition(&PartitionInfo{ID: 7, LeaderID: "node-a", Replicas: []string{"node-a", "node-b"}}, true); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the second node has applied what the first committed", func() bool {
		info, ok := second.Partition(7)
		return ok && info.LeaderID == "node-a"
	})
}

// The Raft port demands the cluster's certificate before it reads a request.
func TestTransportTLS_RaftRefusesCallersWithoutTheClustersCertificate(t *testing.T) {
	keys := newNodeKeys(t, "127.0.0.1")
	node := securedRaft(t, keys, "node-a")
	addr := node.config.RaftAddr

	caPEM, err := os.ReadFile(keys.ca)
	if err != nil {
		t.Fatal(err)
	}
	clusterCA := x509.NewCertPool()
	clusterCA.AppendCertsFromPEM(caPEM)

	// A request for a vote, as Raft's wire format has it: the kind of request
	// and an empty body. A Raft port without TLS answers it.
	requestVote := []byte{1, 0x80}
	ask := func(conn net.Conn, err error) error {
		if err != nil {
			return err
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		// Padded to the length of a TLS record header, which the port reads
		// before it decides anything.
		if _, err := conn.Write(append(requestVote, make([]byte, 6)...)); err != nil {
			return err
		}
		_, err = conn.Read(make([]byte, 1))
		return err
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	plain, plainErr := dialer.Dial("tcp", addr)
	// In TLS 1.3 the refusal of a caller without a certificate reaches it
	// with the first thing it reads.
	bare, bareErr := tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{RootCAs: clusterCA, MinVersion: tls.VersionTLS12})
	for name, err := range map[string]error{
		"without TLS":                 ask(plain, plainErr),
		"with TLS and no certificate": ask(bare, bareErr),
	} {
		if err == nil {
			t.Fatalf("the Raft port answered a caller %s", name)
		}
		if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			t.Fatalf("the Raft port neither answered nor refused a caller %s: %v", name, err)
		}
	}

	// With the cluster's certificate the same port accepts the connection.
	_, member := keys.configs(t)
	secured, err := dialPeer(t.Context(), addr, 5*time.Second, member)
	if err != nil {
		t.Fatalf("the Raft port refused a caller with the cluster's certificate: %v", err)
	}
	_ = secured.Close()
}

// A configuration that secures one direction only is refused.
func TestTransportTLS_NeedsBothDirections(t *testing.T) {
	server, _ := newNodeKeys(t, "127.0.0.1").configs(t)
	m := NewManager(&Config{NodeID: "half", GossipAddr: freeAddr(t), ServerTLS: server})
	defer m.cancel()
	if err := m.Start(); err == nil {
		t.Fatal("a node started that listens with TLS and dials without it")
	}
}
