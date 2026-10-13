//go:build acceptance

package acceptance

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// authority is a CA that issues the certificates of a cluster under test.
type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte // the CA certificate, as a node's CA file holds it
}

func newAuthority(t *testing.T, name string) *authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
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

// issue returns a certificate that is valid for the given hosts, for a server
// and for a client, and its key, both as a node's files hold them.
func (a *authority) issue(t *testing.T, hosts ...string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	node := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "acceptance node"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			node.IPAddresses = append(node.IPAddresses, ip)
		} else {
			node.DNSNames = append(node.DNSNames, host)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, node, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// replaceFile puts content in the place of a file the way whatever issues
// certificates does: written beside it and moved over it, so that a reader
// sees the old file or the new one and never a part of either.
func replaceFile(t *testing.T, path string, content []byte) {
	t.Helper()
	tmp := path + ".new"
	if err := os.WriteFile(tmp, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// nodeCertificates creates a CA and a certificate that is valid for the given
// hosts, and returns the CA and the three files a node is given for its
// traffic with other nodes. The nodes of the cluster under test share them,
// as the pods of a release share one secret.
func nodeCertificates(t *testing.T, dir string, hosts ...string) (ca *authority, caFile, certFile, keyFile string) {
	t.Helper()
	ca = newAuthority(t, "acceptance cluster CA")
	certPEM, keyPEM := ca.issue(t, hosts...)
	write := func(name string, content []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	return ca, write("ca.crt", ca.pem), write("tls.crt", certPEM), write("tls.key", keyPEM)
}
