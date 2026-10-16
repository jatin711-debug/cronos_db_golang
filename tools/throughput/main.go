// Command throughput runs bounded, reconciled publish/delivery measurements.
// Use scripts/measure-throughput.ps1 for the disposable cluster and host guards.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

type histogram struct{ buckets [60001]atomic.Uint64 }

func (h *histogram) add(d time.Duration) { h.buckets[min(max(d.Microseconds()/100, 0), 60000)].Add(1) }
func (h *histogram) percentile(p float64) float64 {
	var total, n uint64
	for i := range h.buckets {
		total += h.buckets[i].Load()
	}
	if total == 0 {
		return 0
	}
	target := uint64(float64(total-1)*p) + 1
	for i := range h.buckets {
		n += h.buckets[i].Load()
		if n >= target {
			return float64(i+1) / 10
		}
	}
	return 6000
}

type ledger struct {
	prefix                                                              string
	states                                                              []atomic.Uint32 // 1=acknowledged new publish, 2=delivered at least once
	next, accepted, delivered, duplicates, errors, ambiguous, throttles atomic.Int64
	latency, lateness                                                   histogram
}

func (l *ledger) delivery(e *types.Event) {
	if e == nil || !strings.HasPrefix(e.MessageId, l.prefix) {
		return
	}
	i, err := strconv.Atoi(strings.TrimPrefix(e.MessageId, l.prefix))
	if err != nil || i < 0 || i >= len(l.states) {
		return
	}
	for {
		old := l.states[i].Load()
		if old&2 != 0 {
			l.duplicates.Add(1)
			return
		}
		if l.states[i].CompareAndSwap(old, old|2) {
			l.delivered.Add(1)
			l.lateness.add(time.Duration(time.Now().UnixMilli()-e.ScheduleTs) * time.Millisecond)
			return
		}
	}
}

type result struct {
	Name                string  `json:"name"`
	Batch               int     `json:"batch"`
	Publishers          int     `json:"publishers"`
	Payload             int     `json:"payload_bytes"`
	PublishSeconds      float64 `json:"publish_seconds"`
	AccountedSeconds    float64 `json:"accounted_seconds"`
	Accepted            int64   `json:"unique_accepted"`
	Delivered           int64   `json:"accepted_delivered"`
	Missing             int64   `json:"missing_accepted"`
	DuplicateDeliveries int64   `json:"duplicate_deliveries"`
	Errors              int64   `json:"errors"`
	Ambiguous           int64   `json:"ambiguous_events"`
	BacklogPauses       int64   `json:"backlog_pauses"`
	AcceptedEPS         float64 `json:"accepted_events_per_second"`
	AccountedEPS        float64 `json:"accounted_events_per_second"`
	P50                 float64 `json:"publish_p50_ms"`
	P95                 float64 `json:"publish_p95_ms"`
	P99                 float64 `json:"publish_p99_ms"`
	Lateness99          float64 `json:"delivery_lateness_p99_ms"`
	Valid               bool    `json:"valid"`
	Stopped             bool    `json:"stopped"`
}

var outputMu sync.Mutex

func emit(v any) { outputMu.Lock(); defer outputMu.Unlock(); _ = json.NewEncoder(os.Stdout).Encode(v) }

func main() {
	mode := flag.String("mode", "run", "init creates ephemeral test credentials; run measures throughput")
	dir := flag.String("dir", "", "private run directory")
	names := flag.String("names", "", "certificate DNS names, comma separated")
	nodes := flag.String("nodes", "", "nodeID=address pairs, comma separated")
	seconds := flag.Int("seconds", 15, "bounded publish seconds per trial (5..60)")
	maxEvents := flag.Int("max-events", 500000, "bounded IDs per trial (1000..1000000)")
	maxBacklog := flag.Int64("max-backlog", 20000, "pause publishers above this undelivered count")
	payloadBytes := flag.Int("payload", 256, "payload bytes (1..4096)")
	plan := flag.String("plan", "1:1,1:4,100:1,100:4,100:8,1000:1,1000:4,1000:8,1000:16", "batch:publisher ramp")
	flag.Parse()
	if *dir == "" {
		fail(fmt.Errorf("dir is required"))
	}
	if *mode == "init" {
		if err := initialize(*dir, strings.Split(*names, ",")); err != nil {
			fail(err)
		}
		return
	}
	if *mode != "run" || *seconds < 5 || *seconds > 60 || *maxEvents < 1000 || *maxEvents > 1000000 || *maxBacklog < 1000 || *maxBacklog > 50000 || *payloadBytes < 1 || *payloadBytes > 4096 {
		fail(fmt.Errorf("unsafe or invalid bounds"))
	}
	if err := run(*dir, *nodes, *plan, time.Duration(*seconds)*time.Second, *maxEvents, *maxBacklog, *payloadBytes); err != nil {
		fail(err)
	}
}

func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }

func initialize(dir string, names []string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(dir, "server.env")); !os.IsNotExist(err) {
		return fmt.Errorf("refusing to overwrite existing credentials")
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(now.UnixNano()), Subject: pkix.Name{CommonName: "cronos-peak-ephemeral-ca"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(3 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(now.UnixNano() + 1), Subject: pkix.Name{CommonName: "cronos-peak"}, DNSNames: append(names, "localhost"), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	random := make([]byte, 32)
	if _, err = rand.Read(random); err != nil {
		return err
	}
	secret := hex.EncodeToString(random)
	token, err := auth.GenerateToken("peak", []byte(secret), 3*time.Hour)
	if err != nil {
		return err
	}
	if _, err = rand.Read(random); err != nil {
		return err
	}
	files := map[string][]byte{
		"ca.crt":      pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		"tls.crt":     pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		"tls.key":     pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		"master.key":  []byte(hex.EncodeToString(random)[:32]),
		"server.env":  []byte("CRONOS_AUTH_JWT_SECRET=" + secret + "\nGOMEMLIMIT=230MiB\nGOMAXPROCS=2\n"),
		"token":       []byte(token),
		"policy.json": []byte(`{"peak":{"admin":true}}`),
	}
	for name, data := range files {
		if err = os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			return err
		}
	}
	return nil
}

func run(dir, nodes, plan string, duration time.Duration, maxEvents int, maxBacklog int64, payloadBytes int) error {
	addresses := map[string]string{}
	var bootstrap []string
	for _, pair := range strings.Split(nodes, ",") {
		id, addr, ok := strings.Cut(pair, "=")
		if !ok || id == "" || addr == "" {
			return fmt.Errorf("invalid nodes")
		}
		addresses[id] = addr
		bootstrap = append(bootstrap, addr)
	}
	if len(addresses) != 3 {
		return fmt.Errorf("requires exactly three disposable nodes")
	}
	token, err := os.ReadFile(filepath.Join(dir, "token"))
	if err != nil {
		return err
	}
	cfg := client.DefaultConfig(bootstrap...)
	cfg.NodeIDToAddress = addresses
	cfg.ConnectionsPerNode = 2
	cfg.ReadBufferSize = 64 << 10
	cfg.WriteBufferSize = 64 << 10
	cfg.RequestTimeout = 3 * time.Second
	cfg.Security = client.SecurityConfig{CACertFile: filepath.Join(dir, "ca.crt"), BearerToken: strings.TrimSpace(string(token)), ServerName: "localhost"}
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Minute)
	defer cancel()
	cl, err := client.Dial(ctx, cfg)
	if err != nil {
		return err
	}
	defer cl.Close()
	producer, err := cl.NewProducer(client.DefaultProducerConfig())
	if err != nil {
		return err
	}
	defer producer.Close()
	meta := cl.PartitionMetadata()
	if len(meta) == 0 {
		return fmt.Errorf("no partition metadata")
	}
	var partitions []int32
	for _, p := range meta {
		partitions = append(partitions, p.PartitionID)
	}
	var current atomic.Pointer[ledger]
	consumerCfg := client.DefaultConsumerConfig("peak", "peak-workers")
	consumerCfg.StartOffset = 0
	consumerCfg.MaxBufferSize = 512
	consumerCfg.WorkerConcurrency = 4
	consumerCfg.AckBatchSize = 256
	consumerCfg.AckFlushInterval = 10 * time.Millisecond
	consumerCfg.AckQueueSize = 2048
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- cl.Subscribe(consumerCtx, consumerCfg, func(_ context.Context, d client.Delivery) error {
			if l := current.Load(); l != nil {
				l.delivery(d.Event)
				for _, e := range d.Batch {
					l.delivery(e)
				}
			}
			return nil
		})
	}()
	time.Sleep(2 * time.Second)
	stop := func() bool { _, err := os.Stat(filepath.Join(dir, "STOP")); return err == nil || ctx.Err() != nil }
	var results []result
	trial := func(name string, batch, workers int, span time.Duration, capEvents int) result {
		l := &ledger{prefix: name + "-", states: make([]atomic.Uint32, capEvents)}
		current.Store(l)
		start := time.Now()
		pubCtx, stopPublish := context.WithTimeout(ctx, span)
		defer stopPublish()
		var stopped atomic.Bool
		done := make(chan struct{})
		go func() {
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					if stop() {
						stopped.Store(true)
						stopPublish()
						return
					}
				}
			}
		}()
		var wg sync.WaitGroup
		body := make([]byte, payloadBytes)
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				iteration := worker
				for pubCtx.Err() == nil {
					for l.accepted.Load()-l.delivered.Load() > maxBacklog && pubCtx.Err() == nil {
						l.throttles.Add(1)
						time.Sleep(10 * time.Millisecond)
					}
					if pubCtx.Err() != nil {
						return
					}
					first := int(l.next.Add(int64(batch))) - batch
					if first >= capEvents {
						return
					}
					n := min(batch, capEvents-first)
					partition := partitions[iteration%len(partitions)]
					iteration++
					messages := make([]client.Message, n)
					schedule := time.Now().UnixMilli()
					for i := range messages {
						messages[i] = client.Message{MessageID: l.prefix + strconv.Itoa(first+i), Topic: "peak", PartitionID: &partition, Payload: body, ScheduleTS: schedule}
					}
					// Admission stops at the deadline, while admitted RPCs get their own
					// bounded timeout so the end of a trial does not manufacture failures.
					rpcCtx, rpcCancel := context.WithTimeout(ctx, 4*time.Second)
					rpcStart := time.Now()
					resp, callErr := producer.SendBatch(rpcCtx, messages)
					rpcCancel()
					l.latency.add(time.Since(rpcStart))
					if callErr != nil || resp == nil {
						l.errors.Add(int64(n))
						continue
					}
					if resp.PublishedCount != int32(n) || resp.ErrorCount != 0 || resp.DuplicateCount != 0 {
						l.errors.Add(int64(resp.ErrorCount))
						l.ambiguous.Add(int64(n))
						continue
					}
					for i := first; i < first+n; i++ {
						l.states[i].Or(1)
					}
					l.accepted.Add(int64(n))
				}
			}(worker)
		}
		wg.Wait()
		publishEnd := time.Now()
		close(done)
		for until := time.Now().Add(45 * time.Second); time.Now().Before(until); {
			if stop() {
				stopped.Store(true)
				break
			}
			if l.delivered.Load() >= l.accepted.Load() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		end := time.Now()
		var verified, missing int64
		for i := range l.states {
			state := l.states[i].Load()
			if state&1 != 0 {
				if state&2 != 0 {
					verified++
				} else {
					missing++
				}
			}
		}
		r := result{Name: name, Batch: batch, Publishers: workers, Payload: payloadBytes, PublishSeconds: publishEnd.Sub(start).Seconds(), AccountedSeconds: end.Sub(start).Seconds(), Accepted: l.accepted.Load(), Delivered: verified, Missing: missing, DuplicateDeliveries: l.duplicates.Load(), Errors: l.errors.Load(), Ambiguous: l.ambiguous.Load(), BacklogPauses: l.throttles.Load(), P50: l.latency.percentile(.5), P95: l.latency.percentile(.95), P99: l.latency.percentile(.99), Lateness99: l.lateness.percentile(.99), Stopped: stopped.Load()}
		r.AcceptedEPS = float64(r.Accepted) / r.PublishSeconds
		r.AccountedEPS = float64(r.Delivered) / r.AccountedSeconds
		r.Valid = r.Missing == 0 && r.Errors == 0 && r.Ambiguous == 0 && !r.Stopped && r.Accepted > 0
		emit(map[string]any{"type": "result", "result": r})
		return r
	}
	if warmup := trial("warmup", 100, 1, 5*time.Second, 10000); !warmup.Valid {
		return fmt.Errorf("warmup failed reconciliation; refusing to ramp load")
	}
	for index, pair := range strings.Split(plan, ",") {
		if stop() {
			break
		}
		b, w, ok := strings.Cut(pair, ":")
		batch, e1 := strconv.Atoi(b)
		workers, e2 := strconv.Atoi(w)
		if !ok || e1 != nil || e2 != nil || batch < 1 || batch > 1000 || workers < 1 || workers > 16 {
			return fmt.Errorf("invalid ramp stage")
		}
		r := trial(fmt.Sprintf("stage%02d", index+1), batch, workers, duration, maxEvents)
		results = append(results, r)
		data, _ := json.MarshalIndent(results, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "results.json"), data, 0600); err != nil {
			return err
		}
		if !r.Valid {
			break
		}
		time.Sleep(2 * time.Second)
	}
	// Repeat the highest reconciled rate instead of selecting a single spike.
	var best *result
	for i := range results {
		r := &results[i]
		if r.Valid && (best == nil || r.AccountedEPS > best.AccountedEPS) {
			best = r
		}
	}
	if best != nil && !stop() {
		batch, workers := best.Batch, best.Publishers
		for i := 1; i <= 2 && !stop(); i++ {
			r := trial(fmt.Sprintf("repeat%02d", i), batch, workers, duration, maxEvents)
			results = append(results, r)
			if !r.Valid {
				break
			}
			time.Sleep(2 * time.Second)
		}
	}
	data, _ := json.MarshalIndent(results, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "results.json"), data, 0600); err != nil {
		return err
	}
	stopConsumer()
	select {
	case <-consumerDone:
	case <-time.After(5 * time.Second):
	}
	if best == nil {
		return fmt.Errorf("no fully reconciled trial passed")
	}
	return nil
}
