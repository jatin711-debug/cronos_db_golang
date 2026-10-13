package tlsfiles

import (
	"context"
	"net"

	"google.golang.org/grpc/credentials"
)

// Credentials returns gRPC client credentials that make every connection
// with the configuration as its files are at that moment. gRPC keeps the
// credentials it was given for as long as a client lives and connects again
// with them whenever its connection breaks, so credentials made from a
// configuration once would verify servers against a CA bundle that has been
// replaced since.
func (c *Client) Credentials() credentials.TransportCredentials {
	return &clientCredentials{client: c}
}

type clientCredentials struct {
	client     *Client
	serverName string // set by OverrideServerName; empty means the address dialed
}

func (c *clientCredentials) now() credentials.TransportCredentials {
	cfg := c.client.Config()
	if c.serverName != "" {
		cfg = cfg.Clone()
		cfg.ServerName = c.serverName
	}
	return credentials.NewTLS(cfg)
}

func (c *clientCredentials) ClientHandshake(ctx context.Context, authority string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return c.now().ClientHandshake(ctx, authority, conn)
}

func (c *clientCredentials) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return c.now().ServerHandshake(conn)
}

func (c *clientCredentials) Info() credentials.ProtocolInfo {
	return c.now().Info()
}

func (c *clientCredentials) Clone() credentials.TransportCredentials {
	clone := *c
	return &clone
}

func (c *clientCredentials) OverrideServerName(name string) error {
	c.serverName = name
	return nil
}
