package tlsfiles

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// authority is a CA that issues certificates for a test.
type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

var serials struct {
	mu   sync.Mutex
	next int64
}

func nextSerial() *big.Int {
	serials.mu.Lock()
	defer serials.mu.Unlock()
	serials.next++
	return big.NewInt(serials.next)
}

func newAuthority(t *testing.T, name string) *authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          nextSerial(),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &authority{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue returns a certificate for 127.0.0.1 and its key, both as PEM, and the
// certificate's serial number.
func (a *authority) issue(t *testing.T) (certPEM, keyPEM []byte, serial *big.Int) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial = nextSerial()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "node"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), serial
}

// clock is the time the files are looked at by, which a test moves on.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *clock { return &clock{now: time.Now()} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// pass lets enough time go by for the files to be looked at again.
func (c *clock) pass() {
	c.mu.Lock()
	c.now = c.now.Add(checkEvery + time.Second)
	c.mu.Unlock()
}

// replace writes a file as whatever issues certificates does: beside it, and
// then moved into its place. Its time is set ahead of the file it replaces,
// since some file systems keep times to the second.
func replace(t *testing.T, path string, content []byte) {
	t.Helper()
	ahead := time.Now().Add(time.Minute)
	if info, err := os.Stat(path); err == nil && !info.ModTime().Before(ahead) {
		ahead = info.ModTime().Add(time.Minute)
	}
	tmp := path + ".new"
	if err := os.WriteFile(tmp, content, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(tmp, ahead, ahead); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func serialOf(t *testing.T, cert *tls.Certificate) *big.Int {
	t.Helper()
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return leaf.SerialNumber
}

func TestKeyPair_FollowsItsFiles(t *testing.T) {
	ca := newAuthority(t, "ca")
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	cert1, key1, serial1 := ca.issue(t)
	replace(t, certFile, cert1)
	replace(t, keyFile, key1)

	pair, err := LoadKeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	clk := newClock()
	pair.now = clk.Now
	if got := serialOf(t, pair.Certificate()); got.Cmp(serial1) != 0 {
		t.Fatalf("serves certificate %v, want %v", got, serial1)
	}

	// The certificate is replaced and its key is not yet: the two do not
	// belong together, and what was read before stays in use.
	cert2, key2, serial2 := ca.issue(t)
	replace(t, certFile, cert2)
	if got := serialOf(t, pair.Certificate()); got.Cmp(serial1) != 0 {
		t.Fatalf("serves certificate %v before the files were looked at again, want %v", got, serial1)
	}
	clk.pass()
	if got := serialOf(t, pair.Certificate()); got.Cmp(serial1) != 0 {
		t.Fatalf("serves certificate %v with the key of another, want %v", got, serial1)
	}

	// The key follows, and the new pair is served.
	replace(t, keyFile, key2)
	clk.pass()
	if got := serialOf(t, pair.Certificate()); got.Cmp(serial2) != 0 {
		t.Fatalf("serves certificate %v after both files were replaced, want %v", got, serial2)
	}

	// Files that are gone for a moment change nothing.
	if err := os.Remove(certFile); err != nil {
		t.Fatal(err)
	}
	clk.pass()
	if got := serialOf(t, pair.Certificate()); got.Cmp(serial2) != 0 {
		t.Fatalf("serves certificate %v while its file is gone, want %v", got, serial2)
	}
}

func TestLoad_FailsWithoutUsableFiles(t *testing.T) {
	ca := newAuthority(t, "ca")
	dir := t.TempDir()
	cert, key, _ := ca.issue(t)
	_, otherKey, _ := ca.issue(t)
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	replace(t, certFile, cert)

	if _, err := LoadKeyPair(certFile, keyFile); err == nil {
		t.Fatal("a certificate without its key was loaded")
	}
	replace(t, keyFile, otherKey)
	if _, err := LoadKeyPair(certFile, keyFile); err == nil {
		t.Fatal("a certificate with the key of another was loaded")
	}
	replace(t, keyFile, key)
	if _, err := LoadKeyPair(certFile, keyFile); err != nil {
		t.Fatalf("a certificate with its key was refused: %v", err)
	}

	caFile := filepath.Join(dir, "ca.crt")
	if _, err := LoadPool(caFile); err == nil {
		t.Fatal("a CA file that does not exist was loaded")
	}
	replace(t, caFile, []byte("not a certificate"))
	if _, err := LoadPool(caFile); err == nil {
		t.Fatal("a CA file without a certificate was loaded")
	}
}

// node is one end of a connection between two nodes, set up as the server and
// the client of the replication channel are.
type node struct {
	certFile, keyFile, caFile string
	pair                      *KeyPair
	pool                      *Pool
}

func newNode(t *testing.T, clk *clock, ca *authority) (*node, *big.Int) {
	t.Helper()
	dir := t.TempDir()
	n := &node{certFile: filepath.Join(dir, "tls.crt"), keyFile: filepath.Join(dir, "tls.key"), caFile: filepath.Join(dir, "ca.crt")}
	cert, key, serial := ca.issue(t)
	replace(t, n.certFile, cert)
	replace(t, n.keyFile, key)
	replace(t, n.caFile, ca.pem)
	var err error
	if n.pair, err = LoadKeyPair(n.certFile, n.keyFile); err != nil {
		t.Fatal(err)
	}
	if n.pool, err = LoadPool(n.caFile); err != nil {
		t.Fatal(err)
	}
	n.pair.now, n.pool.now = clk.Now, clk.Now
	return n, serial
}

// reissue gives the node a new certificate from ca and returns its serial.
func (n *node) reissue(t *testing.T, ca *authority) *big.Int {
	t.Helper()
	cert, key, serial := ca.issue(t)
	replace(t, n.certFile, cert)
	replace(t, n.keyFile, key)
	return serial
}

func (n *node) server() *tls.Config {
	cfg := &tls.Config{GetCertificate: n.pair.GetCertificate, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}}
	FollowClientCAs(cfg, n.pool)
	return cfg
}

func (n *node) client() *Client {
	return NewClient(&tls.Config{GetClientCertificate: n.pair.GetClientCertificate, MinVersion: tls.VersionTLS12}, n.pool)
}

// listen serves TLS connections with cfg until the test ends. It returns the
// address, and a channel with the serial of the certificate each accepted
// client presented, or nil for a client that was refused.
func listen(t *testing.T, cfg *tls.Config) (string, <-chan *big.Int) {
	t.Helper()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	clients := make(chan *big.Int, 16)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				tlsConn := conn.(*tls.Conn)
				if err := tlsConn.Handshake(); err != nil {
					clients <- nil
					return
				}
				clients <- tlsConn.ConnectionState().PeerCertificates[0].SerialNumber
				_, _ = conn.Write([]byte("ok"))
			}()
		}
	}()
	return listener.Addr().String(), clients
}

// connect makes one connection and returns the serial of the certificate the
// server presented.
func connect(addr string, cfg *tls.Config) (*big.Int, error) {
	conn, err := (&tls.Dialer{Config: cfg}).DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tlsConn := conn.(*tls.Conn)
	// The server reads the client's certificate after the client has finished
	// its part; reading the reply makes sure the server accepted it.
	reply := make([]byte, 2)
	if _, err := tlsConn.Read(reply); err != nil {
		return nil, err
	}
	return tlsConn.ConnectionState().PeerCertificates[0].SerialNumber, nil
}

// TestNodes_ReplaceCertificatesAndTheirCAWhileRunning takes two nodes through
// what an operator does to replace certificates, and then the CA that issues
// them, with one listener and one client configuration from start to end.
func TestNodes_ReplaceCertificatesAndTheirCAWhileRunning(t *testing.T) {
	clk := newClock()
	oldCA := newAuthority(t, "old ca")
	a, aSerial := newNode(t, clk, oldCA)
	b, bSerial := newNode(t, clk, oldCA)
	addr, presented := listen(t, a.server())
	client := b.client()

	expect := func(when string, wantServer, wantClient *big.Int) {
		t.Helper()
		got, err := connect(addr, client.Config())
		if err != nil {
			t.Fatalf("%s: %v", when, err)
		}
		if got.Cmp(wantServer) != 0 {
			t.Fatalf("%s: the server presented certificate %v, want %v", when, got, wantServer)
		}
		if got := <-presented; got == nil || got.Cmp(wantClient) != 0 {
			t.Fatalf("%s: the server saw the client present certificate %v, want %v", when, got, wantClient)
		}
	}
	expect("at the start", aSerial, bSerial)

	// Both get new certificates from the same CA.
	aSerial, bSerial = a.reissue(t, oldCA), b.reissue(t, oldCA)
	clk.pass()
	expect("after new certificates", aSerial, bSerial)

	// The CA is replaced. First every node is told of the new one as well.
	newCA := newAuthority(t, "new ca")
	both := append(append([]byte{}, oldCA.pem...), newCA.pem...)
	replace(t, a.caFile, both)
	replace(t, b.caFile, both)
	clk.pass()
	expect("with both CAs known", aSerial, bSerial)

	// Then the nodes get certificates of the new CA, one after the other.
	aSerial = a.reissue(t, newCA)
	clk.pass()
	expect("with the server on the new CA", aSerial, bSerial)
	bSerial = b.reissue(t, newCA)
	clk.pass()
	expect("with both on the new CA", aSerial, bSerial)

	// Last, the old CA is forgotten. A node that still has a certificate of
	// it is refused from then on.
	replace(t, a.caFile, newCA.pem)
	replace(t, b.caFile, newCA.pem)
	clk.pass()
	expect("with only the new CA known", aSerial, bSerial)

	late, _ := newNode(t, clk, oldCA)
	replace(t, late.caFile, newCA.pem)
	clk.pass()
	if _, err := connect(addr, late.client().Config()); err == nil {
		t.Fatal("a client with a certificate of the CA that was removed was accepted")
	}
	if got := <-presented; got != nil {
		t.Fatalf("the server accepted the certificate %v of the CA that was removed", got)
	}
}

// TestCredentials_VerifyAgainstTheCAOfTheMoment checks the gRPC credentials:
// one set of them, kept as a gRPC client keeps it, goes on connecting after
// the CA it verifies servers against has been replaced.
func TestCredentials_VerifyAgainstTheCAOfTheMoment(t *testing.T) {
	clk := newClock()
	oldCA := newAuthority(t, "old ca")
	a, _ := newNode(t, clk, oldCA)
	b, _ := newNode(t, clk, oldCA)
	addr, presented := listen(t, a.server())
	creds := b.client().Credentials()

	handshake := func() error {
		raw, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer raw.Close()
		conn, _, err := creds.ClientHandshake(context.Background(), addr, raw)
		if err != nil {
			return err
		}
		reply := make([]byte, 2)
		_, err = conn.Read(reply)
		return err
	}
	if err := handshake(); err != nil {
		t.Fatalf("at the start: %v", err)
	}
	<-presented

	newCA := newAuthority(t, "new ca")
	both := append(append([]byte{}, oldCA.pem...), newCA.pem...)
	replace(t, a.caFile, both)
	replace(t, b.caFile, both)
	a.reissue(t, newCA)
	clk.pass()
	if err := handshake(); err != nil {
		t.Fatalf("after the server moved to a new CA that the client was told of: %v", err)
	}
	<-presented

	// A server whose CA the client has never been told of is still refused.
	stranger, _ := newNode(t, clk, newAuthority(t, "another ca"))
	replace(t, stranger.caFile, both)
	clk.pass()
	strangerAddr, _ := listen(t, stranger.server())
	raw, err := net.Dial("tcp", strangerAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, _, err := creds.ClientHandshake(context.Background(), strangerAddr, raw); err == nil {
		t.Fatal("a server with a certificate of an unknown CA was accepted")
	}
}
