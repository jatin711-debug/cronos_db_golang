// Package tlsfiles serves a certificate, its key and a CA bundle from files,
// and reads them again when the files change.
//
// Certificates are replaced while a node runs: they expire, and whatever
// issues them writes new ones to the same paths. A node that reads its
// certificate once, when it starts, goes on presenting the old one until it is
// restarted, and when the old one expires no other node accepts it. With this
// package a handshake is made with what the files hold at the time.
//
// Nothing watches the files. They are looked at when a handshake asks for
// them, at most once in checkEvery, and read when their time or size differs
// from when they were read last. A file that cannot be read, or a certificate
// and key that do not belong together, as happens for a moment when the two
// are written one after the other, leaves what was read before in use.
package tlsfiles

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// checkEvery is how long the files are trusted to be as they were found.
const checkEvery = 2 * time.Second

// stamp is what tells a file that was replaced from one that was not.
type stamp struct {
	modified time.Time
	size     int64
}

func stampOf(path string) (stamp, error) {
	info, err := os.Stat(path)
	if err != nil {
		return stamp{}, err
	}
	return stamp{modified: info.ModTime(), size: info.Size()}, nil
}

// watched is the part a KeyPair and a Pool share: when the files were last
// looked at, and what they looked like when they were last read.
type watched struct {
	mu      sync.Mutex
	paths   []string
	stamps  []stamp
	checked time.Time
	// failed is the stamps of an attempt to read that did not work, so that
	// one bad write is reported once and not at every handshake.
	failed []stamp
	now    func() time.Time
}

// changed reports whether the files differ from when they were read, looking
// at them at most once in checkEvery. The caller holds w.mu.
func (w *watched) changed() ([]stamp, bool) {
	now := w.now()
	if now.Sub(w.checked) < checkEvery {
		return nil, false
	}
	w.checked = now
	current := make([]stamp, len(w.paths))
	for i, path := range w.paths {
		s, err := stampOf(path)
		if err != nil {
			// Gone for the moment, as during a replacement. Looked at again later.
			return nil, false
		}
		current[i] = s
	}
	if equalStamps(current, w.stamps) || equalStamps(current, w.failed) {
		return nil, false
	}
	return current, true
}

func equalStamps(a, b []stamp) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].modified.Equal(b[i].modified) || a[i].size != b[i].size {
			return false
		}
	}
	return true
}

// KeyPair is a certificate and its private key, read from two files.
type KeyPair struct {
	watched
	current atomic.Pointer[tls.Certificate]
}

// LoadKeyPair reads a certificate and its key. It fails when they cannot be
// read or do not belong together: a node must not start without them.
func LoadKeyPair(certFile, keyFile string) (*KeyPair, error) {
	k := &KeyPair{watched: watched{paths: []string{certFile, keyFile}, now: time.Now}}
	cert, stamps, err := k.read()
	if err != nil {
		return nil, err
	}
	k.current.Store(cert)
	k.stamps, k.checked = stamps, k.now()
	return k, nil
}

func (k *KeyPair) read() (*tls.Certificate, []stamp, error) {
	stamps := make([]stamp, len(k.paths))
	for i, path := range k.paths {
		s, err := stampOf(path)
		if err != nil {
			return nil, nil, err
		}
		stamps[i] = s
	}
	cert, err := tls.LoadX509KeyPair(k.paths[0], k.paths[1])
	if err != nil {
		return nil, nil, err
	}
	return &cert, stamps, nil
}

// Certificate returns the certificate as its files hold it now.
func (k *KeyPair) Certificate() *tls.Certificate {
	k.mu.Lock()
	defer k.mu.Unlock()
	if seen, changed := k.changed(); changed {
		cert, stamps, err := k.read()
		if err != nil {
			k.failed = seen
			slog.Warn("A TLS certificate was replaced and cannot be read; the one read before stays in use",
				"cert_file", k.paths[0], "key_file", k.paths[1], "error", err)
		} else {
			k.current.Store(cert)
			k.stamps, k.failed = stamps, nil
			attrs := []any{"cert_file", k.paths[0]}
			if leaf, err := x509.ParseCertificate(cert.Certificate[0]); err == nil {
				attrs = append(attrs, "not_after", leaf.NotAfter.UTC().Format(time.RFC3339))
			}
			slog.Info("A TLS certificate was replaced and read again", attrs...)
		}
	}
	return k.current.Load()
}

// GetCertificate is for tls.Config.GetCertificate: a server presents the
// certificate its files hold when a client connects.
func (k *KeyPair) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return k.Certificate(), nil
}

// GetClientCertificate is for tls.Config.GetClientCertificate: a client
// presents the certificate its files hold when a server asks for one.
func (k *KeyPair) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return k.Certificate(), nil
}

// Pool is a bundle of CA certificates, read from one file.
type Pool struct {
	watched
	current atomic.Pointer[x509.CertPool]
}

// LoadPool reads a CA bundle. It fails when the file cannot be read or holds
// no certificate.
func LoadPool(caFile string) (*Pool, error) {
	p := &Pool{watched: watched{paths: []string{caFile}, now: time.Now}}
	pool, stamps, err := p.read()
	if err != nil {
		return nil, err
	}
	p.current.Store(pool)
	p.stamps, p.checked = stamps, p.now()
	return p, nil
}

func (p *Pool) read() (*x509.CertPool, []stamp, error) {
	s, err := stampOf(p.paths[0])
	if err != nil {
		return nil, nil, err
	}
	pem, err := os.ReadFile(p.paths[0])
	if err != nil {
		return nil, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, nil, fmt.Errorf("%s holds no certificate", p.paths[0])
	}
	return pool, []stamp{s}, nil
}

// Get returns the bundle as its file holds it now. It is the same pool until
// the file changes, so a caller can tell by comparing.
func (p *Pool) Get() *x509.CertPool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if seen, changed := p.changed(); changed {
		pool, stamps, err := p.read()
		if err != nil {
			p.failed = seen
			slog.Warn("A TLS CA bundle was replaced and cannot be read; the one read before stays in use",
				"ca_file", p.paths[0], "error", err)
		} else {
			p.current.Store(pool)
			p.stamps, p.failed = stamps, nil
			slog.Info("A TLS CA bundle was replaced and read again", "ca_file", p.paths[0])
		}
	}
	return p.current.Load()
}

// FollowClientCAs makes a server's configuration verify the certificates of
// its clients against pool as its file holds it at each handshake. The
// configuration must not be changed afterwards.
func FollowClientCAs(server *tls.Config, pool *Pool) {
	server.ClientCAs = pool.Get()
	var derived atomic.Pointer[tls.Config]
	server.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		now := pool.Get()
		if now == server.ClientCAs {
			return nil, nil // as configured
		}
		if d := derived.Load(); d != nil && d.ClientCAs == now {
			return d, nil
		}
		d := server.Clone()
		d.GetConfigForClient = nil
		d.ClientCAs = now
		derived.Store(d)
		return d, nil
	}
}

// Client is a client's TLS configuration whose CA bundle follows its file.
// The certificate the client presents follows its files by itself, through
// KeyPair.GetClientCertificate; the bundle it verifies servers against is a
// field of the configuration, so a connection has to be made with the
// configuration as it is then.
type Client struct {
	base    *tls.Config
	pool    *Pool // nil when the base configuration does not verify against a file
	derived atomic.Pointer[tls.Config]
}

// NewClient returns a client configuration that verifies servers against pool
// as its file holds it when a connection is made. With a nil pool it is base.
func NewClient(base *tls.Config, pool *Pool) *Client {
	if pool != nil {
		base.RootCAs = pool.Get()
	}
	return &Client{base: base, pool: pool}
}

// Config returns the configuration to make a connection with now.
func (c *Client) Config() *tls.Config {
	if c.pool == nil {
		return c.base
	}
	now := c.pool.Get()
	if now == c.base.RootCAs {
		return c.base
	}
	if d := c.derived.Load(); d != nil && d.RootCAs == now {
		return d
	}
	d := c.base.Clone()
	d.RootCAs = now
	c.derived.Store(d)
	return d
}
