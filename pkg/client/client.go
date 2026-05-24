package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"cronos_db/pkg/client/internal/connpool"
	"cronos_db/pkg/client/internal/metadata"
	"cronos_db/pkg/types"
	"cronos_db/pkg/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Client is the CronosDB SDK entry point for metadata-aware routing and transport.
type Client struct {
	cfg      Config
	pool     *connpool.Pool
	metadata *metadata.Manager

	bgCancel  context.CancelFunc
	closeOnce sync.Once
}

// Dial creates and initializes a client with pooled connections and metadata cache.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, wrapError("client.dial", ErrorKindValidation, err)
	}

	dialOpts, err := buildDialOptions(cfg)
	if err != nil {
		return nil, wrapError("client.dial_options", ErrorKindValidation, err)
	}

	pool, err := connpool.New(ctx, cfg.BootstrapAddresses, connpool.Config{
		ConnectionsPerNode: cfg.ConnectionsPerNode,
		DialTimeout:        cfg.DialTimeout,
		ResolveDNS:         cfg.ResolveDNS,
		DialOptions:        dialOpts,
	})
	if err != nil {
		return nil, wrapError("client.pool_init", ErrorKindTransport, err)
	}

	metaManager := metadata.NewManager(pool, metadata.Config{
		TTL:                  cfg.Metadata.TTL,
		RefreshInterval:      cfg.Metadata.RefreshInterval,
		RequestTimeout:       cfg.RequestTimeout,
		NodeIDToAddress:      cfg.NodeIDToAddress,
		StaticPartitionCount: cfg.PartitionCount,
		OnRefresh:            cfg.Hooks.OnMetadataRefresh,
	})

	if err := metaManager.Refresh(ctx); err != nil {
		_ = pool.Close()
		return nil, wrapError("client.metadata_init", ErrorKindMetadataStale, err)
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	metaManager.Start(bgCtx)

	return &Client{
		cfg:      cfg,
		pool:     pool,
		metadata: metaManager,
		bgCancel: cancel,
	}, nil
}

// Close shuts down background workers and all pooled connections.
func (c *Client) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		if c.bgCancel != nil {
			c.bgCancel()
		}
		if c.metadata != nil {
			c.metadata.Close()
		}
		if c.pool != nil {
			closeErr = c.pool.Close()
		}
	})
	return closeErr
}

// ForceMetadataRefresh triggers an immediate metadata refresh.
func (c *Client) ForceMetadataRefresh(ctx context.Context) error {
	return c.metadata.Refresh(ctx)
}

// MarkMetadataStale marks metadata stale so background refresh prioritizes reload.
func (c *Client) MarkMetadataStale() {
	c.metadata.MarkStale()
}

// PartitionForKey returns the partition ID using server-compatible hashing.
func (c *Client) PartitionForKey(key string) (int32, error) {
	partitions := c.metadata.PartitionCount()
	if partitions <= 0 {
		partitions = c.cfg.PartitionCount
	}
	if partitions <= 0 {
		return 0, wrapError("client.partition_for_key", ErrorKindMetadataStale, fmt.Errorf("partition count unavailable"))
	}
	return utils.HashToPartitionID(key, partitions), nil
}

// RouteForPartition returns routing candidates for a partition.
func (c *Client) RouteForPartition(partitionID int32) (*Route, error) {
	info, ok := c.metadata.GetPartitionInfo(partitionID)
	if !ok {
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.RequestTimeout)
		defer cancel()
		_ = c.metadata.EnsureFresh(ctx)
		info, ok = c.metadata.GetPartitionInfo(partitionID)
		if !ok {
			return nil, wrapError("client.route_for_partition", ErrorKindMetadataStale, fmt.Errorf("partition %d not in metadata", partitionID))
		}
	}

	preferred, _ := c.metadata.ResolveLeaderAddress(partitionID)
	return &Route{
		PartitionID:        partitionID,
		LeaderID:           info.GetLeaderId(),
		PreferredAddress:   preferred,
		CandidateAddresses: c.metadata.CandidateAddresses(partitionID),
	}, nil
}

// RouteForKey resolves partition and route using a partition key.
func (c *Client) RouteForKey(key string) (*Route, error) {
	partitionID, err := c.PartitionForKey(key)
	if err != nil {
		return nil, err
	}
	return c.RouteForPartition(partitionID)
}

// PartitionMetadata returns a snapshot of current partition metadata.
func (c *Client) PartitionMetadata() []PartitionMetadata {
	infos := c.metadata.Partitions()
	out := make([]PartitionMetadata, 0, len(infos))
	for _, info := range infos {
		out = append(out, partitionMetadataFromProto(info))
	}
	return out
}

func (c *Client) eventClientForAddress(addr string) (types.EventServiceClient, error) {
	client, err := c.pool.EventClient(addr)
	if err == nil {
		return client, nil
	}
	if !strings.Contains(err.Error(), "not found in pool") {
		return nil, err
	}

	bootstrapCtx, cancel := context.WithTimeout(context.Background(), c.cfg.DialTimeout)
	defer cancel()
	if addErr := c.pool.AddNode(bootstrapCtx, addr); addErr != nil {
		return nil, err
	}
	return c.pool.EventClient(addr)
}

func (c *Client) partitionClientForAddress(addr string) (types.PartitionServiceClient, error) {
	client, err := c.pool.PartitionClient(addr)
	if err == nil {
		return client, nil
	}
	if !strings.Contains(err.Error(), "not found in pool") {
		return nil, err
	}

	bootstrapCtx, cancel := context.WithTimeout(context.Background(), c.cfg.DialTimeout)
	defer cancel()
	if addErr := c.pool.AddNode(bootstrapCtx, addr); addErr != nil {
		return nil, err
	}
	return c.pool.PartitionClient(addr)
}

func (c *Client) observeRequest(op string, nodeAddr string, start time.Time, err error) {
	if c == nil || c.cfg.Hooks == nil {
		return
	}
	c.cfg.Hooks.OnRequest(op, nodeAddr, time.Since(start), err)
}

func (c *Client) observeError(op string, kind ErrorKind, err error) {
	if c == nil || c.cfg.Hooks == nil || err == nil {
		return
	}
	if hook, ok := c.cfg.Hooks.(ErrorHook); ok {
		hook.OnError(op, kind, err)
	}
}

func (c *Client) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.cfg.RequestTimeout)
}

func buildDialOptions(cfg Config) ([]grpc.DialOption, error) {
	transportCreds, err := buildTransportCredentials(cfg.Security)
	if err != nil {
		return nil, err
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                cfg.KeepaliveTime,
			Timeout:             cfg.KeepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithReadBufferSize(cfg.ReadBufferSize),
		grpc.WithWriteBufferSize(cfg.WriteBufferSize),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(cfg.MaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(cfg.MaxSendMsgSize),
		),
	}

	if cfg.Security.PerRPCCredentials != nil {
		opts = append(opts, grpc.WithPerRPCCredentials(cfg.Security.PerRPCCredentials))
	} else if cfg.Security.BearerToken != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(&staticTokenCredentials{
			scheme:     cfg.Security.BearerScheme,
			token:      cfg.Security.BearerToken,
			insecureOK: cfg.Security.Insecure,
		}))
	}

	return opts, nil
}

func buildTransportCredentials(sec SecurityConfig) (credentials.TransportCredentials, error) {
	if sec.Insecure {
		return insecure.NewCredentials(), nil
	}

	tlsCfg := &tls.Config{
		ServerName:         sec.ServerName,
		InsecureSkipVerify: sec.InsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}

	if sec.CACertFile != "" {
		caPEM, err := os.ReadFile(sec.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("read ca cert file: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("parse ca cert file: no certs loaded")
		}
		tlsCfg.RootCAs = roots
	}

	if sec.ClientCertFile != "" && sec.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(sec.ClientCertFile, sec.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return credentials.NewTLS(tlsCfg), nil
}

type staticTokenCredentials struct {
	scheme     string
	token      string
	insecureOK bool
}

func (c *staticTokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": fmt.Sprintf("%s %s", c.scheme, c.token),
	}, nil
}

func (c *staticTokenCredentials) RequireTransportSecurity() bool {
	return !c.insecureOK
}
