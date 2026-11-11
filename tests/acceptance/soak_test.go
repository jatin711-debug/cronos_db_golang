//go:build acceptance

package acceptance

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// The soak. A three-node cluster with the production durability settings
// (every_event fsync, encryption at rest, mutual TLS between nodes, RF=3,
// minISR=2, a change-data-capture sink) takes a steady scheduled workload for
// hours, with hourly bursts, a seeded backlog of delayed events, a consumer
// group that falls behind, a poison topic that dead-letters, and a failing CDC
// destination. One disruption at a time is done to it, and the next waits
// until the ledger and the replicas have reconciled. Throughout, what each
// node holds in memory and on disk is sampled.
//
// It is opt-in and long:
//
//	CRONOS_SOAK_DURATION=2h go test -tags acceptance -run '^TestSoak$' -count=1 -v -timeout 4h ./tests/acceptance/
//
// Settings (environment):
//
//	CRONOS_SOAK_DURATION         how long the steady workload runs (required, >= 20m)
//	CRONOS_SOAK_RATE             steady publishes per second (100)
//	CRONOS_SOAK_BURST_RATE       publishes per second during a burst (500)
//	CRONOS_SOAK_BURST_EVERY      time between bursts (1h, or a quarter of the run if shorter)
//	CRONOS_SOAK_BURST_FOR        length of a burst (1m)
//	CRONOS_SOAK_SEED             delayed events published before anything else (100000)
//	CRONOS_SOAK_NODE_MEMORY_MIB  each node's --memory-limit (256)
//	CRONOS_SOAK_NODE_PROCS       GOMAXPROCS of each node (2)
//	CRONOS_SOAK_SEGMENT_MIB      WAL segment size (32)
//	CRONOS_SOAK_SAMPLE           sampling interval (1m)
//	CRONOS_SOAK_RECOVER_WITHIN   bound on recovery and reconciliation after a disruption (15m)
//	CRONOS_SOAK_OUT              directory for the CSV and JSON reports
//	CRONOS_SOAK_PPROF            set to serve each node's profiles on a loopback port
//
// The nodes run as processes of this machine below normal priority, so that
// the soak leaves the machine to whatever else it is doing. That makes this
// single-host evidence: it finds lost or early events, leaks and unbounded
// growth, but the nodes share one kernel, disk, clock and power supply, and
// its latencies are not those of production hosts.

const (
	soakTopic       = "soak"
	soakPoisonTopic = "soak-poison"
	soakMainGroup   = "soak-workers"
	soakSlowGroup   = "soak-slow"
	soakPoisonGroup = "soak-poison-workers"
	soakMaxRetries  = 3
	soakPublishers  = 48
	soakSeeders     = 16
	// soakMarkEvery is how many events of a stream share one record of when
	// they were sent.
	soakMarkEvery = 256
)

type soakSettings struct {
	duration      time.Duration
	rate          float64
	burstRate     float64
	burstEvery    time.Duration
	burstFor      time.Duration
	seed          int
	nodeMemory    int64
	nodeProcs     int
	segmentBytes  int64
	sample        time.Duration
	recoverWithin time.Duration
	out           string
}

func envNumber(t *testing.T, name string, fallback float64) float64 {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v < 0 {
		t.Fatalf("%s=%q is not a non-negative number", name, raw)
	}
	return v
}

func envDuration(t *testing.T, name string, fallback time.Duration) time.Duration {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	v, err := time.ParseDuration(raw)
	if err != nil || v < 0 {
		t.Fatalf("%s=%q is not a duration", name, raw)
	}
	return v
}

func loadSoakSettings(t *testing.T) soakSettings {
	t.Helper()
	raw := os.Getenv("CRONOS_SOAK_DURATION")
	if raw == "" {
		t.Skip("set CRONOS_SOAK_DURATION (for example 2h or 24h) to run the soak")
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 20*time.Minute {
		t.Fatalf("CRONOS_SOAK_DURATION=%q: want a duration of at least 20m", raw)
	}
	s := soakSettings{
		duration:      d,
		rate:          envNumber(t, "CRONOS_SOAK_RATE", 100),
		burstRate:     envNumber(t, "CRONOS_SOAK_BURST_RATE", 500),
		burstEvery:    envDuration(t, "CRONOS_SOAK_BURST_EVERY", min(time.Hour, d/4)),
		burstFor:      envDuration(t, "CRONOS_SOAK_BURST_FOR", time.Minute),
		seed:          int(envNumber(t, "CRONOS_SOAK_SEED", 100000)),
		nodeMemory:    int64(envNumber(t, "CRONOS_SOAK_NODE_MEMORY_MIB", 256)) << 20,
		nodeProcs:     int(envNumber(t, "CRONOS_SOAK_NODE_PROCS", 2)),
		segmentBytes:  int64(envNumber(t, "CRONOS_SOAK_SEGMENT_MIB", 32)) << 20,
		sample:        envDuration(t, "CRONOS_SOAK_SAMPLE", time.Minute),
		recoverWithin: envDuration(t, "CRONOS_SOAK_RECOVER_WITHIN", 15*time.Minute),
		out:           os.Getenv("CRONOS_SOAK_OUT"),
	}
	if s.rate <= 0 || s.burstRate < s.rate || s.burstFor >= s.burstEvery || s.nodeProcs < 1 || s.segmentBytes < 1<<20 || s.sample < 5*time.Second {
		t.Fatalf("inconsistent soak settings: %+v", s)
	}
	return s
}

// The ledger of a soak holds millions of events, so it keeps 12 bytes for
// each instead of a map entry. An event's ID names its stream and its
// sequence number in it, which is its index in the stream's slice.

const (
	soakAccepted uint8 = 1 << iota
	soakExported
)

type soakEvent struct {
	due       uint32 // milliseconds after the ledger's epoch
	offset    uint32 // acknowledged offset + 1; 0 when the acknowledgement carried none
	partition int8   // -1 until acknowledged with one
	flags     uint8
	main      uint8 // deliveries to the main group, or to the poison group (saturating)
	slow      uint8 // deliveries to the slow group (saturating)
}

type soakStream struct {
	name   string
	poison bool
	events []soakEvent
	// sentAt[k] is when event k*soakMarkEvery was first sent, in milliseconds
	// after the epoch.
	sentAt []uint32
}

type soakGroup int

const (
	groupMain soakGroup = iota
	groupSlow
	groupPoison
)

type soakLedger struct {
	run   string
	epoch int64 // unix milliseconds

	mu       sync.Mutex
	streams  map[string]*soakStream
	early    []string
	earlyN   int
	unknown  []string
	unknownN int
	// cdcUnknown counts exported events this run never published.
	cdcUnknown int

	accepted, mainDelivered, slowDelivered, poisonDelivered, exported atomic.Int64
	byPartition                                                       [partitionCount]atomic.Int64

	lateness *phaseHist // first deliveries to the main group, by phase
	phase    func(time.Time) string
}

func newSoakLedger(phase func(time.Time) string) *soakLedger {
	return &soakLedger{
		run:      "s" + strconv.FormatInt(time.Now().UnixNano()%1_000_000_000, 36),
		epoch:    time.Now().UnixMilli(),
		streams:  make(map[string]*soakStream),
		lateness: newPhaseHist(),
		phase:    phase,
	}
}

func (l *soakLedger) stream(name string, poison bool) *soakStream {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := &soakStream{name: name, poison: poison}
	l.streams[name] = st
	return st
}

func (l *soakLedger) id(st *soakStream, seq int) string {
	return l.run + "." + st.name + "." + strconv.Itoa(seq)
}

// sent records the next event of a stream before its first publish attempt
// and returns its sequence number.
func (l *soakLedger) sent(st *soakStream, due int64) int {
	now := uint32(time.Now().UnixMilli() - l.epoch)
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := len(st.events)
	st.events = append(st.events, soakEvent{due: uint32(due - l.epoch), partition: -1})
	if seq%soakMarkEvery == 0 {
		st.sentAt = append(st.sentAt, now)
	}
	return seq
}

func (l *soakLedger) acked(st *soakStream, seq int, partition int32, offset int64) {
	l.mu.Lock()
	ev := &st.events[seq]
	first := ev.flags&soakAccepted == 0
	ev.flags |= soakAccepted
	if partition >= 0 && offset >= 0 {
		ev.partition, ev.offset = int8(partition), uint32(offset+1)
	}
	l.mu.Unlock()
	if first {
		l.accepted.Add(1)
		if partition >= 0 && partition < partitionCount {
			l.byPartition[partition].Add(1)
		}
	}
}

// lookup finds an event by its ID. The caller holds mu.
func (l *soakLedger) lookup(id string) (*soakStream, int, bool) {
	run, rest, ok := strings.Cut(id, ".")
	if !ok || run != l.run {
		return nil, 0, false
	}
	name, seqText, ok := strings.Cut(rest, ".")
	if !ok {
		return nil, 0, false
	}
	st := l.streams[name]
	seq, err := strconv.Atoi(seqText)
	if st == nil || err != nil || seq < 0 || seq >= len(st.events) {
		return nil, 0, false
	}
	return st, seq, true
}

func (l *soakLedger) note(list *[]string, count *int, text string) {
	*count++
	if len(*list) < 50 {
		*list = append(*list, text)
	}
}

func (l *soakLedger) delivered(group soakGroup, event *types.Event, receivedAt int64) {
	id := event.GetMessageId()
	l.mu.Lock()
	defer l.mu.Unlock()
	st, seq, ok := l.lookup(id)
	if !ok {
		l.note(&l.unknown, &l.unknownN, fmt.Sprintf("%s (partition %d offset %d)", id, event.GetPartitionId(), event.GetOffset()))
		return
	}
	ev := &st.events[seq]
	due := l.epoch + int64(ev.due)
	if event.GetScheduleTs() != due || receivedAt < due {
		l.note(&l.early, &l.earlyN, fmt.Sprintf("%s: scheduled for %d (delivery says %d), received at %d, %d ms early",
			id, due, event.GetScheduleTs(), receivedAt, due-receivedAt))
	}
	switch {
	case group == groupSlow:
		if ev.slow == 0 {
			l.slowDelivered.Add(1)
		}
		ev.slow = min(ev.slow, 254) + 1
	default:
		if ev.main == 0 {
			if group == groupPoison {
				l.poisonDelivered.Add(1)
			} else {
				l.mainDelivered.Add(1)
				now := time.UnixMilli(receivedAt)
				l.lateness.add(l.phase(now), float64(max(receivedAt-due, 0)))
			}
		}
		ev.main = min(ev.main, 254) + 1
	}
}

func (l *soakLedger) exportedID(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, seq, ok := l.lookup(id)
	if !ok {
		l.cdcUnknown++
		return
	}
	ev := &st.events[seq]
	if ev.flags&soakExported == 0 {
		ev.flags |= soakExported
		l.exported.Add(1)
	}
}

// publishedBefore reports whether an event was sent before cut, judged by the
// block of soakMarkEvery events it belongs to. The caller holds mu.
func (l *soakLedger) publishedBefore(st *soakStream, seq int, cut int64) bool {
	k := seq/soakMarkEvery + 1
	return k < len(st.sentAt) && l.epoch+int64(st.sentAt[k]) <= cut
}

func (l *soakLedger) describe(st *soakStream, seq int) string {
	ev := st.events[seq]
	return fmt.Sprintf("%s (partition %d offset %d, due %s)", l.id(st, seq), ev.partition, int64(ev.offset)-1,
		time.UnixMilli(l.epoch+int64(ev.due)).Format("15:04:05.000"))
}

// soakGap is what the cluster owes at a moment.
type soakGap struct {
	main, slow, poison, unexported     []string
	mainN, slowN, poisonN, unexportedN int
}

// gap lists the acknowledged events due by dueBy that a group has not received,
// and those published before sentBy that the CDC destination has not. A zero
// sentBy skips the export check; a negative one checks every event.
func (l *soakLedger) gap(dueBy, sentBy int64, slow bool) soakGap {
	var g soakGap
	add := func(list *[]string, n *int, st *soakStream, seq int) {
		*n++
		if len(*list) < 25 {
			*list = append(*list, l.describe(st, seq))
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, st := range l.streams {
		for seq := range st.events {
			ev := &st.events[seq]
			if ev.flags&soakAccepted == 0 {
				continue
			}
			due := l.epoch + int64(ev.due)
			switch {
			case st.poison:
				if ev.main == 0 && due <= dueBy {
					add(&g.poison, &g.poisonN, st, seq)
				}
			default:
				if ev.main == 0 && due <= dueBy {
					add(&g.main, &g.mainN, st, seq)
				}
				if slow && ev.slow == 0 && due <= dueBy {
					add(&g.slow, &g.slowN, st, seq)
				}
			}
			if sentBy != 0 && ev.flags&soakExported == 0 && (sentBy < 0 || l.publishedBefore(st, seq, sentBy)) {
				add(&g.unexported, &g.unexportedN, st, seq)
			}
		}
	}
	return g
}

func (g soakGap) empty() bool {
	return g.mainN == 0 && g.slowN == 0 && g.poisonN == 0 && g.unexportedN == 0
}

func (g soakGap) String() string {
	var parts []string
	for _, p := range []struct {
		what string
		n    int
		list []string
	}{
		{"not delivered to " + soakMainGroup, g.mainN, g.main},
		{"not delivered to " + soakSlowGroup, g.slowN, g.slow},
		{"poison events never delivered", g.poisonN, g.poison},
		{"not exported to the CDC destination", g.unexportedN, g.unexported},
	} {
		if p.n > 0 {
			parts = append(parts, fmt.Sprintf("%d acknowledged events %s:\n  %s", p.n, p.what, strings.Join(p.list, "\n  ")))
		}
	}
	return strings.Join(parts, "\n")
}

// maxPoisonDeliveries returns the most deliveries any poison event had.
func (l *soakLedger) maxPoisonDeliveries() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	most := 0
	for _, st := range l.streams {
		if st.poison {
			for _, ev := range st.events {
				most = max(most, int(ev.main))
			}
		}
	}
	return most
}

// hist is a log-scale histogram of milliseconds, 5% wide buckets.
type hist struct {
	counts map[int]int64
	n      int64
	max    float64
}

func (h *hist) add(ms float64) {
	b := 0
	if ms > 1 {
		b = int(math.Log(ms)/math.Log(1.05)) + 1
	}
	if h.counts == nil {
		h.counts = make(map[int]int64)
	}
	h.counts[b]++
	h.n++
	h.max = max(h.max, ms)
}

// quantile returns the upper bound of the bucket holding the q-quantile.
func (h *hist) quantile(q float64) float64 {
	if h.n == 0 {
		return 0
	}
	keys := make([]int, 0, len(h.counts))
	for b := range h.counts {
		keys = append(keys, b)
	}
	slices.Sort(keys)
	want, cum := int64(math.Ceil(q*float64(h.n))), int64(0)
	for _, b := range keys {
		cum += h.counts[b]
		if cum >= want {
			if b == 0 {
				return 1
			}
			return math.Min(math.Pow(1.05, float64(b)), h.max)
		}
	}
	return h.max
}

// phaseHist keeps a histogram per phase of the run and one for the current
// sampling interval.
type phaseHist struct {
	mu       sync.Mutex
	byPhase  map[string]*hist
	interval *hist
}

func newPhaseHist() *phaseHist {
	return &phaseHist{byPhase: make(map[string]*hist), interval: &hist{}}
}

func (p *phaseHist) add(phase string, ms float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.byPhase[phase]
	if h == nil {
		h = &hist{}
		p.byPhase[phase] = h
	}
	h.add(ms)
	p.interval.add(ms)
}

// takeInterval returns the current interval's histogram and starts another.
func (p *phaseHist) takeInterval() *hist {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.interval
	p.interval = &hist{}
	return h
}

func (p *phaseHist) summary() map[string]map[string]float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]map[string]float64)
	for phase, h := range p.byPhase {
		out[phase] = map[string]float64{"count": float64(h.n), "p50_ms": h.quantile(0.5), "p99_ms": h.quantile(0.99), "max_ms": h.max}
	}
	return out
}

// cdcReceiver is the destination of the cluster's change data capture: a
// webhook that records which events reached it, and that can be made to fail.
type cdcReceiver struct {
	ledger   *soakLedger
	url      string
	failing  atomic.Bool
	requests atomic.Int64
	refused  atomic.Int64

	// arrivals, when set, gets a line for every event the destination takes.
	arrivalsMu sync.Mutex
	arrivals   *bufio.Writer
}

func (r *cdcReceiver) flush() {
	r.arrivalsMu.Lock()
	if r.arrivals != nil {
		_ = r.arrivals.Flush()
	}
	r.arrivalsMu.Unlock()
}

func startCDCReceiver(t *testing.T, ledger *soakLedger) *cdcReceiver {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &cdcReceiver{ledger: ledger, url: "http://" + ln.Addr().String() + "/cdc"}
	srv := &http.Server{Handler: r, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return r
}

func (r *cdcReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.requests.Add(1)
	if r.failing.Load() {
		_, _ = io.Copy(io.Discard, req.Body)
		r.refused.Add(1)
		http.Error(w, "destination unavailable", http.StatusServiceUnavailable)
		return
	}
	var change struct {
		PartitionID int32 `json:"partition_id"`
		Offset      int64 `json:"offset"`
		Event       struct {
			MessageID string `json:"message_id"`
		} `json:"event"`
	}
	if err := json.NewDecoder(req.Body).Decode(&change); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.ledger.exportedID(change.Event.MessageID)
	r.arrivalsMu.Lock()
	if r.arrivals != nil {
		_, _ = fmt.Fprintf(r.arrivals, "%s %d %d %s %s\n", time.Now().Format("15:04:05.000"), change.PartitionID, change.Offset, change.Event.MessageID, req.RemoteAddr)
	}
	r.arrivalsMu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

type soakConsumer struct {
	name     string
	cancel   context.CancelFunc
	done     chan error
	stopping atomic.Bool
}

// disruptionRecord is what a disruption did and how the cluster came back.
type disruptionRecord struct {
	Name              string    `json:"name"`
	Start             time.Time `json:"start"`
	Detail            string    `json:"detail"`
	ResumedSeconds    float64   `json:"service_resumed_seconds,omitempty"`
	ReadySeconds      float64   `json:"ready_seconds"`
	ReconciledSeconds float64   `json:"reconciled_seconds"`
}

// nodeSample is one sampling of one node.
type nodeSample struct {
	at                    time.Time
	processStart          float64
	rss, heap, held       float64
	goroutines, handles   float64
	dataBytes, dlqBytes   int64
	files                 int
	segments, blockingAge float64
	coldEntries, timers   float64
	resident              bool // the node answered its metrics
}

type soak struct {
	t   *testing.T
	cfg soakSettings
	c   *cluster
	l   *soakLedger
	cdc *cdcReceiver
	out string

	observer *client.Client
	producer *client.Producer

	steadyStart, publishEnd time.Time
	// steadyAt is steadyStart for the sampler, which runs before it is set.
	steadyAt  atomic.Int64
	phaseName atomic.Value // string; "" means steady or burst by the clock

	stopPublishing chan struct{}
	publishers     sync.WaitGroup
	tokens         chan struct{}
	shortfall      atomic.Int64
	refused        atomic.Int64
	failedAttempts atomic.Int64
	publishLatency *phaseHist

	slow       atomic.Bool
	lastPoison atomic.Int64
	consumers  []*soakConsumer
	slowGroup  *soakConsumer

	records   []disruptionRecord
	samplesMu sync.Mutex
	samples   map[string][]nodeSample
	nodeCSV   *os.File
	workCSV   *os.File
}

func (s *soak) phase(now time.Time) string {
	if p, _ := s.phaseName.Load().(string); p != "" {
		return p
	}
	if s.inBurst(now) {
		return "burst"
	}
	return "steady"
}

func (s *soak) inBurst(now time.Time) bool {
	began := s.steadyAt.Load()
	if began == 0 || now.UnixNano() < began {
		return false
	}
	into := now.Sub(time.Unix(0, began)) % s.cfg.burstEvery
	return into >= s.cfg.burstEvery/2 && into < s.cfg.burstEvery/2+s.cfg.burstFor
}

func (s *soak) rateAt(now time.Time) float64 {
	if s.inBurst(now) {
		return s.cfg.burstRate
	}
	return s.cfg.rate
}

// wait sleeps for d while watching for nodes and consumers that stop by
// themselves, which fails the soak at once.
func (s *soak) wait(d time.Duration) {
	s.t.Helper()
	until := time.Now().Add(d)
	for {
		s.checkAlive()
		left := time.Until(until)
		if left <= 0 {
			return
		}
		time.Sleep(min(left, time.Second))
	}
}

func (s *soak) waitUntil(at time.Time) { s.wait(time.Until(at)) }

func (s *soak) checkAlive() {
	s.t.Helper()
	for _, n := range s.c.nodes {
		n.mu.Lock()
		cmd, exited := n.cmd, n.exited
		n.mu.Unlock()
		if cmd == nil {
			continue
		}
		select {
		case <-exited:
			s.t.Fatalf("%s exited by itself", n.id)
		default:
		}
	}
	for _, sc := range s.consumers {
		if sc.stopping.Load() {
			continue
		}
		select {
		case err := <-sc.done:
			s.t.Fatalf("consumer %s stopped by itself: %v", sc.name, err)
		default:
		}
	}
}

func (s *soak) subscribe(name, topic, group string, buffer int32, handle func(*types.Event, int64) error) *soakConsumer {
	s.t.Helper()
	cl := s.c.dial()
	ctx, cancel := context.WithCancel(context.Background())
	sc := &soakConsumer{name: name, cancel: cancel, done: make(chan error, 1)}
	cfg := client.DefaultConsumerConfig(topic, group)
	cfg.ReconnectBackoff = 200 * time.Millisecond
	cfg.MaxReconnectBackoff = 2 * time.Second
	cfg.MaxBufferSize = buffer
	go func() {
		sc.done <- cl.Subscribe(ctx, cfg, func(_ context.Context, d client.Delivery) error {
			now := time.Now().UnixMilli()
			var err error
			if d.Event != nil {
				err = errors.Join(err, handle(d.Event, now))
			}
			for _, event := range d.Batch {
				err = errors.Join(err, handle(event, now))
			}
			return err
		})
	}()
	s.consumers = append(s.consumers, sc)
	return sc
}

func (s *soak) unsubscribe(sc *soakConsumer) {
	s.t.Helper()
	sc.stopping.Store(true)
	sc.cancel()
	select {
	case <-sc.done:
	case <-time.After(30 * time.Second):
		s.t.Errorf("consumer %s did not stop", sc.name)
	}
}

var errPoison = errors.New("poison event: refused by the handler")

func (s *soak) startConsumers() {
	main := func(e *types.Event, now int64) error {
		s.l.delivered(groupMain, e, now)
		return nil
	}
	s.subscribe("main-1", soakTopic, soakMainGroup, 1024, main)
	s.subscribe("main-2", soakTopic, soakMainGroup, 1024, main)
	s.startSlowGroup()
	s.subscribe("poison", soakPoisonTopic, soakPoisonGroup, 16, func(e *types.Event, now int64) error {
		s.l.delivered(groupPoison, e, now)
		s.lastPoison.Store(now)
		return errPoison
	})
}

// startSlowGroup subscribes the group that falls behind. When slow, it takes
// 200 ms over every delivery; its small buffer keeps that well within the
// acknowledgement timeout, so it lags without having its events retried.
func (s *soak) startSlowGroup() {
	s.slowGroup = s.subscribe("slow", soakTopic, soakSlowGroup, 4, func(e *types.Event, now int64) error {
		s.l.delivered(groupSlow, e, now)
		return nil
	})
}

func isRefusal(err error) bool {
	m := err.Error()
	return strings.Contains(m, "at capacity") || strings.Contains(m, "ResourceExhausted")
}

// send publishes one event until it is acknowledged, as an application that
// must not lose it does. After stop is closed it gets 45 seconds more.
func (s *soak) send(st *soakStream, seq int, topic string, payload []byte, due int64, stop <-chan struct{}) {
	id := s.l.id(st, seq)
	var giveUp <-chan time.Time
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		began := time.Now()
		result, err := s.producer.Send(ctx, client.Message{MessageID: id, Topic: topic, Payload: payload, ScheduleTS: due})
		cancel()
		if err == nil {
			s.publishLatency.add(s.phase(began), float64(time.Since(began).Microseconds())/1000)
			s.l.acked(st, seq, result.PartitionID, result.Offset)
			return
		}
		if acceptedEarlier(err) {
			s.l.acked(st, seq, -1, -1)
			return
		}
		if isRefusal(err) {
			s.refused.Add(1)
		} else {
			s.failedAttempts.Add(1)
		}
		select {
		case <-stop:
			stop, giveUp = nil, time.After(45*time.Second)
		case <-giveUp:
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

var (
	soakSmall = make([]byte, 1<<10)
	soakLarge = make([]byte, 64<<10)
)

// seedDelayed publishes the delayed backlog, due evenly over the middle of
// the run, before anything else happens.
func (s *soak) seedDelayed() {
	s.t.Helper()
	if s.cfg.seed == 0 {
		return
	}
	lo := time.Now().Add(s.cfg.duration / 4).UnixMilli()
	hi := time.Now().Add(s.cfg.duration * 85 / 100).UnixMilli()
	step(s.t, "seeding %d delayed events of 1 KiB, due between %s and %s", s.cfg.seed,
		time.UnixMilli(lo).Format("15:04:05"), time.UnixMilli(hi).Format("15:04:05"))
	began := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < soakSeeders; w++ {
		st := s.l.stream(fmt.Sprintf("seed%02d", w), false)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < s.cfg.seed; i += soakSeeders {
				due := lo + (hi-lo)*int64(i)/int64(s.cfg.seed)
				seq := s.l.sent(st, due)
				s.send(st, seq, soakTopic, soakSmall, due, nil)
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	for {
		select {
		case <-done:
			step(s.t, "seeded %d events in %s", s.l.accepted.Load(), time.Since(began).Round(time.Second))
			return
		case <-time.After(time.Second):
			s.checkAlive()
		}
	}
}

// pace hands out publish tokens at the rate of the moment. Tokens the
// publishers cannot take are counted as shortfall.
func (s *soak) pace(stop <-chan struct{}) {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	credit, last := 0.0, time.Now()
	for {
		select {
		case <-stop:
			return
		case now := <-tick.C:
			credit += s.rateAt(now) * now.Sub(last).Seconds()
			last = now
			for ; credit >= 1; credit-- {
				select {
				case s.tokens <- struct{}{}:
				default:
					s.shortfall.Add(1)
				}
			}
		}
	}
}

// dueFor spreads the steady workload's schedules: most events are due within
// seconds, some within minutes, a few within the hour, none long after the
// publishing ends.
func (s *soak) dueFor(seq int, now time.Time) time.Time {
	var due time.Time
	switch k := seq % 20; {
	case k < 14:
		due = now.Add(500*time.Millisecond + time.Duration(seq%10)*500*time.Millisecond)
	case k < 19:
		due = now.Add(time.Minute + time.Duration(seq%9)*time.Minute)
	default:
		due = now.Add(10*time.Minute + time.Duration(seq%6)*10*time.Minute)
	}
	if latest := s.publishEnd.Add(5 * time.Minute); due.After(latest) {
		due = latest
	}
	return due
}

func (s *soak) startPublishers() {
	s.stopPublishing = make(chan struct{})
	s.tokens = make(chan struct{}, int(s.cfg.burstRate/2)+1)
	go s.pace(s.stopPublishing)
	for w := 0; w < soakPublishers; w++ {
		st := s.l.stream(fmt.Sprintf("w%02d", w), false)
		s.publishers.Add(1)
		go func() {
			defer s.publishers.Done()
			for {
				select {
				case <-s.stopPublishing:
					return
				case <-s.tokens:
				}
				now := time.Now()
				due := s.dueFor(len(st.events), now).UnixMilli()
				seq := s.l.sent(st, due)
				payload := soakSmall
				if seq%100 == 42 {
					payload = soakLarge
				}
				s.send(st, seq, soakTopic, payload, due, s.stopPublishing)
			}
		}()
	}
	// One poison event every ten seconds, on a topic of its own so that it is
	// never batched with events that are acknowledged.
	poison := s.l.stream("poison", true)
	s.publishers.Add(1)
	go func() {
		defer s.publishers.Done()
		for {
			select {
			case <-s.stopPublishing:
				return
			case <-time.After(10 * time.Second):
			}
			due := time.Now().Add(time.Second).UnixMilli()
			seq := s.l.sent(poison, due)
			s.send(poison, seq, soakPoisonTopic, soakSmall, due, s.stopPublishing)
		}
	}()
}

// metrics reads a node's metrics, summed and maximised over each name's
// series.
func metricsOf(n *node) (sum, most map[string]float64, err error) {
	httpClient := http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get("http://" + n.httpAddr + "/metrics")
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, nil, err
	}
	sum, most = make(map[string]float64), make(map[string]float64)
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		name := line
		if i := strings.IndexAny(line, "{ "); i >= 0 {
			name = line[:i]
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil || math.IsNaN(v) {
			continue
		}
		sum[name] += v
		if old, seen := most[name]; !seen || v > old {
			most[name] = v
		}
	}
	return sum, most, nil
}

// diskUse adds up the files under a node's data directory, and separately
// those of its dead-letter journals.
func diskUse(root string) (total, dlq int64, files int) {
	sep := string(filepath.Separator)
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		total += info.Size()
		files++
		if strings.Contains(path, sep+"dlq"+sep) {
			dlq += info.Size()
		}
		return nil
	})
	return total, dlq, files
}

const soakNodeHeader = "time,elapsed_s,node,process_start,rss_mib,heap_inuse_mib,held_mib,goroutines,handles,data_mib,dlq_mib,files,wal_segments,retention_blocking_age_s,scheduler_cold_entries,scheduler_active_timers\n"
const soakWorkHeader = "time,elapsed_s,phase,target_rate,accepted,delivered_main,delivered_slow,exported,poison_delivered,main_lag,slow_lag,unexported,publish_n,publish_p50_ms,publish_p99_ms,publish_max_ms,lateness_n,lateness_p50_ms,lateness_p99_ms,lateness_max_ms,refused,failed_attempts,shortfall,cdc_refused,early,unknown\n"

func (s *soak) openReports() {
	s.t.Helper()
	s.out = s.cfg.out
	if s.out == "" {
		s.out = filepath.Join(os.TempDir(), "cronos-soak-"+s.l.run)
	}
	if err := os.MkdirAll(s.out, 0o755); err != nil {
		s.t.Fatal(err)
	}
	var err error
	if s.nodeCSV, err = os.Create(filepath.Join(s.out, "nodes.csv")); err != nil {
		s.t.Fatal(err)
	}
	if s.workCSV, err = os.Create(filepath.Join(s.out, "workload.csv")); err != nil {
		s.t.Fatal(err)
	}
	_, _ = s.nodeCSV.WriteString(soakNodeHeader)
	_, _ = s.workCSV.WriteString(soakWorkHeader)
	if os.Getenv("CRONOS_SOAK_CDC_LOG") != "" {
		f, err := os.Create(filepath.Join(s.out, "cdc-arrivals.log"))
		if err != nil {
			s.t.Fatal(err)
		}
		s.cdc.arrivalsMu.Lock()
		s.cdc.arrivals = bufio.NewWriterSize(f, 1<<20)
		s.cdc.arrivalsMu.Unlock()
		s.t.Cleanup(func() { s.cdc.flush(); _ = f.Close() })
	}
	s.t.Cleanup(func() { _ = s.nodeCSV.Close(); _ = s.workCSV.Close() })
	step(s.t, "reports go to %s", s.out)
}

// sampleOnce records every node and the workload.
func (s *soak) sampleOnce(began time.Time) {
	now := time.Now()
	elapsed := now.Sub(began).Seconds()
	stamp := now.Format(time.RFC3339)
	mib := func(v float64) float64 { return v / (1 << 20) }
	for _, n := range s.c.nodes {
		sm := nodeSample{at: now}
		if n.running() {
			if sum, most, err := metricsOf(n); err == nil {
				sm.resident = true
				sm.processStart = most["process_start_time_seconds"]
				sm.rss = most["process_resident_memory_bytes"]
				sm.heap = most["go_memstats_heap_inuse_bytes"]
				sm.held = most["cronos_memory_held_bytes"]
				sm.goroutines = most["go_goroutines"]
				sm.handles = max(most["process_open_fds"], most["process_open_handles"])
				sm.segments = sum["cronos_wal_segment_count"]
				sm.blockingAge = most["cronos_retention_blocking_age_seconds"]
				sm.coldEntries = sum["cronos_scheduler_cold_store_entries"]
				sm.timers = sum["cronos_scheduler_active_timers"]
			}
		}
		sm.dataBytes, sm.dlqBytes, sm.files = diskUse(n.dataDir)
		s.samplesMu.Lock()
		s.samples[n.id] = append(s.samples[n.id], sm)
		s.samplesMu.Unlock()
		_, _ = fmt.Fprintf(s.nodeCSV, "%s,%.0f,%s,%.0f,%.1f,%.1f,%.1f,%.0f,%.0f,%.1f,%.2f,%d,%.0f,%.0f,%.0f,%.0f\n",
			stamp, elapsed, n.id, sm.processStart, mib(sm.rss), mib(sm.heap), mib(sm.held), sm.goroutines, sm.handles,
			mib(float64(sm.dataBytes)), mib(float64(sm.dlqBytes)), sm.files, sm.segments, sm.blockingAge, sm.coldEntries, sm.timers)
	}
	g := s.l.gap(now.Add(-5*time.Second).UnixMilli(), now.Add(-10*time.Second).UnixMilli(), true)
	pub, late := s.publishLatency.takeInterval(), s.l.lateness.takeInterval()
	s.l.mu.Lock()
	early, unknown := s.l.earlyN, s.l.unknownN
	s.l.mu.Unlock()
	_, _ = fmt.Fprintf(s.workCSV, "%s,%.0f,%s,%.0f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.1f,%.1f,%.1f,%d,%.1f,%.1f,%.1f,%d,%d,%d,%d,%d,%d\n",
		stamp, elapsed, s.phase(now), s.rateAt(now), s.l.accepted.Load(), s.l.mainDelivered.Load(), s.l.slowDelivered.Load(),
		s.l.exported.Load(), s.l.poisonDelivered.Load(), g.mainN, g.slowN, g.unexportedN,
		pub.n, pub.quantile(0.5), pub.quantile(0.99), pub.max, late.n, late.quantile(0.5), late.quantile(0.99), late.max,
		s.refused.Load(), s.failedAttempts.Load(), s.shortfall.Load(), s.cdc.refused.Load(), early, unknown)
	_ = s.nodeCSV.Sync()
	_ = s.workCSV.Sync()
	s.cdc.flush()
}

func (s *soak) sampleLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	began := time.Now()
	for {
		s.sampleOnce(began)
		select {
		case <-stop:
			s.sampleOnce(began)
			return
		case <-time.After(s.cfg.sample):
		}
	}
}

// waitReady waits until every node answers its readiness check.
func (s *soak) waitReady(within time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(within)
	for _, n := range s.c.nodes {
		for {
			ok, detail := s.c.ready(n)
			if ok {
				break
			}
			s.checkAlive()
			if time.Now().After(deadline) {
				s.t.Fatalf("%s is not ready after %s: %s", n.id, within, detail)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}

// reconcile waits until the cluster is whole and owes nothing that is due:
// every node ready, every partition with all of them in sync, every
// acknowledged event due 30 s ago delivered, and, while the CDC destination
// works, every event published a minute ago exported.
func (s *soak) reconcile(rec *disruptionRecord, since time.Time) {
	s.t.Helper()
	s.waitReady(s.cfg.recoverWithin)
	rec.ReadySeconds = time.Since(since).Seconds()
	s.c.waitTopologyWithin(s.c.nodes, "after "+rec.Name, s.cfg.recoverWithin)
	deadline := time.Now().Add(s.cfg.recoverWithin)
	for {
		now := time.Now()
		sentBy := int64(0)
		if !s.cdc.failing.Load() {
			sentBy = now.Add(-time.Minute).UnixMilli()
		}
		g := s.l.gap(now.Add(-30*time.Second).UnixMilli(), sentBy, !s.slow.Load())
		if g.empty() {
			break
		}
		if now.After(deadline) {
			s.t.Fatalf("%s: the cluster did not reconcile within %s:\n%s", rec.Name, s.cfg.recoverWithin, g)
		}
		s.wait(2 * time.Second)
	}
	rec.ReconciledSeconds = time.Since(since).Seconds()
}

// ledBy returns the partitions n leads.
func (s *soak) ledBy(n *node) []int32 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = s.observer.ForceMetadataRefresh(ctx)
	cancel()
	var led []int32
	for _, meta := range s.observer.PartitionMetadata() {
		if meta.LeaderID == n.id {
			led = append(led, meta.PartitionID)
		}
	}
	slices.Sort(led)
	return slices.Compact(led)
}

func (s *soak) acceptedCounts() (counts [partitionCount]int64) {
	for p := range counts {
		counts[p] = s.l.byPartition[p].Load()
	}
	return counts
}

// resumed waits until every given partition has acknowledged a publish beyond
// the counts in before, and returns how long after since that was.
func (s *soak) resumed(partitions []int32, before [partitionCount]int64, since time.Time) time.Duration {
	s.t.Helper()
	deadline := since.Add(s.cfg.recoverWithin)
	for {
		done := true
		for _, p := range partitions {
			if s.l.byPartition[p].Load() <= before[p] {
				done = false
			}
		}
		if done {
			return time.Since(since)
		}
		s.checkAlive()
		if time.Now().After(deadline) {
			s.t.Fatalf("partitions %v took no publishes within %s", partitions, s.cfg.recoverWithin)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *soak) followerOf(partition int32, leader *node) *node {
	for _, n := range s.c.nodes {
		if n != leader && n.index != 0 {
			return n
		}
	}
	return s.c.nodes[(leader.index+1)%len(s.c.nodes)]
}

// disrupt runs one disruption and reconciles after it, and records both.
func (s *soak) disrupt(name string, do func(rec *disruptionRecord)) {
	s.t.Helper()
	rec := disruptionRecord{Name: name, Start: time.Now()}
	s.phaseName.Store("disruption")
	step(s.t, "disruption: %s", name)
	do(&rec)
	s.reconcile(&rec, rec.Start)
	s.phaseName.Store("")
	step(s.t, "disruption %s: %s; ready after %.1f s, reconciled after %.1f s", name, rec.Detail, rec.ReadySeconds, rec.ReconciledSeconds)
	s.records = append(s.records, rec)
}

func clampDuration(d, lo, hi time.Duration) time.Duration { return min(max(d, lo), hi) }

func TestSoak(t *testing.T) {
	cfg := loadSoakSettings(t)

	c := newCluster(t,
		"--fsync-mode=every_event",
		fmt.Sprintf("--segment-size=%d", cfg.segmentBytes),
		"--compaction-interval=30s",
		"--bloom-capacity=1000000",
		"--dedup-ttl=168",
		fmt.Sprintf("--memory-limit=%d", cfg.nodeMemory),
		fmt.Sprintf("--max-retries=%d", soakMaxRetries),
		"--backup-interval=0",
		"--encryption-enabled",
	)
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	c.encryptionKey = []byte(hex.EncodeToString(key))
	keyFile := filepath.Join(c.dir, "wal.key")
	if err := os.WriteFile(keyFile, c.encryptionKey, 0o600); err != nil {
		t.Fatal(err)
	}
	c.extraArgs = append(c.extraArgs, "--encryption-key-file="+keyFile)
	c.lowPriority = true
	if os.Getenv("CRONOS_SOAK_PPROF") != "" {
		ports := freePorts(t, len(c.nodes))
		pprofAddr := make(map[string]string, len(c.nodes))
		for i, n := range c.nodes {
			pprofAddr[n.id] = fmt.Sprintf("127.0.0.1:%d", ports[i])
			t.Logf("%s serves profiles at http://%s/debug/pprof/", n.id, pprofAddr[n.id])
		}
		c.nodeArgs = func(n *node) []string { return []string{"--pprof-addr=" + pprofAddr[n.id]} }
	}

	s := &soak{t: t, cfg: cfg, c: c, publishLatency: newPhaseHist(), samples: make(map[string][]nodeSample)}
	s.phaseName.Store("seed")
	s.l = newSoakLedger(s.phase)
	s.cdc = startCDCReceiver(t, s.l)
	c.env = []string{fmt.Sprintf("GOMAXPROCS=%d", cfg.nodeProcs), "CRONOS_CDC_WEBHOOK_URL=" + s.cdc.url}

	t.Logf("soak %s: %s steady at %.0f/s, bursts of %.0f/s for %s every %s, %d delayed events seeded, nodes limited to %d MiB and %d CPUs, %d MiB segments",
		s.l.run, cfg.duration, cfg.rate, cfg.burstRate, cfg.burstFor, cfg.burstEvery, cfg.seed, cfg.nodeMemory>>20, cfg.nodeProcs, cfg.segmentBytes>>20)
	c.startAll()
	s.openReports()
	s.observer = c.dial()
	var err error
	if s.producer, err = c.dial().NewProducer(client.DefaultProducerConfig()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.producer.Close() })

	stopSampling, sampled := make(chan struct{}), make(chan struct{})
	go s.sampleLoop(stopSampling, sampled)
	stopSamplingOnce := sync.OnceFunc(func() { close(stopSampling); <-sampled })
	t.Cleanup(stopSamplingOnce)

	s.startConsumers()
	s.seedDelayed()

	s.steadyStart = time.Now()
	s.publishEnd = s.steadyStart.Add(cfg.duration)
	s.steadyAt.Store(s.steadyStart.UnixNano())
	s.phaseName.Store("")
	step(t, "steady workload starts; publishing ends at %s", s.publishEnd.Format("15:04:05"))
	s.startPublishers()

	warm := clampDuration(cfg.duration/12, 10*time.Minute, time.Hour)
	cool := clampDuration(cfg.duration/12, 10*time.Minute, time.Hour)
	const disruptions = 7
	slot := (cfg.duration - warm - cool) / disruptions
	window := clampDuration(slot/2, time.Minute, 2*time.Hour)
	down := clampDuration(slot/6, 30*time.Second, 5*time.Minute)
	at := func(i int) time.Time { return s.steadyStart.Add(warm + time.Duration(i)*slot) }
	t.Logf("disruptions every %s after %s of warm-up; outage windows of %s, nodes down for %s", slot, warm, window, down)

	// 1. The node that leads partition 0 is lost, and comes back later.
	s.waitUntil(at(0))
	s.disrupt("leader lost", func(rec *disruptionRecord) {
		victim := c.leaderOf(s.observer, 0)
		led, before := s.ledBy(victim), s.acceptedCounts()
		killed := time.Now()
		c.kill(victim)
		resumed := s.resumed(led, before, killed)
		rec.ResumedSeconds = resumed.Seconds()
		rec.Detail = fmt.Sprintf("killed %s, which led partitions %v; they took publishes again after %.1f s; down for %s", victim.id, led, resumed.Seconds(), down)
		s.wait(down)
		c.start(victim)
	})

	// 2. The leader stalls for 20 s, as in a long pause or a stuck disk.
	s.waitUntil(at(1))
	s.disrupt("leader frozen", func(rec *disruptionRecord) {
		victim := c.leaderOf(s.observer, 0)
		c.freeze(victim)
		s.wait(20 * time.Second)
		c.thaw(victim)
		rec.Detail = fmt.Sprintf("froze %s, which led partition 0, for 20 s", victim.id)
	})

	// 3. A follower falls behind, and the leader is lost while it catches up.
	s.waitUntil(at(2))
	s.disrupt("leader lost during catch-up", func(rec *disruptionRecord) {
		leader := c.leaderOf(s.observer, 0)
		follower := s.followerOf(0, leader)
		c.kill(follower)
		s.wait(down)
		c.start(follower)
		s.wait(3 * time.Second)
		leader = c.leaderOf(s.observer, 0)
		led, before := s.ledBy(leader), s.acceptedCounts()
		killed := time.Now()
		c.kill(leader)
		resumed := s.resumed(led, before, killed)
		rec.ResumedSeconds = resumed.Seconds()
		rec.Detail = fmt.Sprintf("%s was down for %s and restarted; 3 s later %s, leading %v, was killed; publishes resumed after %.1f s",
			follower.id, down, leader.id, led, resumed.Seconds())
		s.wait(30 * time.Second)
		c.start(leader)
	})

	// 4. The CDC destination fails for a window, then recovers; the export
	// has to catch up without losing anything.
	s.waitUntil(at(3))
	s.disrupt("CDC destination down", func(rec *disruptionRecord) {
		before := s.l.exported.Load()
		s.cdc.failing.Store(true)
		s.wait(window)
		s.cdc.failing.Store(false)
		rec.Detail = fmt.Sprintf("the webhook answered 503 for %s (%d requests refused); %d events were exported before it", window, s.cdc.refused.Load(), before)
	})

	// 5. A consumer group is slow for half a window and gone for the other
	// half, and then has to catch up.
	s.waitUntil(at(4))
	s.disrupt("consumer group behind", func(rec *disruptionRecord) {
		s.slow.Store(true)
		s.unsubscribe(s.slowGroup)
		s.slowGroup = s.subscribe("slow", soakTopic, soakSlowGroup, 4, func(e *types.Event, now int64) error {
			s.l.delivered(groupSlow, e, now)
			if s.slow.Load() {
				time.Sleep(200 * time.Millisecond)
			}
			return nil
		})
		s.wait(window / 2)
		s.unsubscribe(s.slowGroup)
		s.wait(window / 2)
		lag := s.l.gap(time.Now().UnixMilli(), 0, true).slowN
		s.slow.Store(false)
		s.startSlowGroup()
		rec.Detail = fmt.Sprintf("%s was slow for %s and offline for %s; it was %d events behind", soakSlowGroup, window/2, window/2, lag)
	})

	// 6. A node loses its disk and is replaced by an empty one, which has to
	// be filled with everything the others retain.
	s.waitUntil(at(5))
	s.disrupt("node replaced", func(rec *disruptionRecord) {
		victim := s.followerOf(0, c.leaderOf(s.observer, 0))
		c.kill(victim)
		held, _, _ := diskUse(victim.dataDir)
		c.wipe(victim)
		c.start(victim)
		rec.Detail = fmt.Sprintf("%s lost %d MiB of data and was started empty", victim.id, held>>20)
	})

	// 7. Every node is lost at once.
	s.waitUntil(at(6))
	s.disrupt("whole cluster lost", func(rec *disruptionRecord) {
		before := s.acceptedCounts()
		killed := time.Now()
		for _, n := range c.nodes {
			c.kill(n)
		}
		s.wait(5 * time.Second)
		for _, n := range c.nodes {
			c.start(n)
		}
		resumed := s.resumed([]int32{0, 1, 2, 3}, before, killed)
		rec.ResumedSeconds = resumed.Seconds()
		rec.Detail = fmt.Sprintf("killed every node, started them 5 s later; every partition took publishes again after %.1f s", resumed.Seconds())
	})

	s.waitUntil(s.publishEnd)
	s.phaseName.Store("drain")
	step(t, "publishing ends")
	close(s.stopPublishing)
	s.publishers.Wait()

	// Everything acknowledged has to arrive: in both groups, at the CDC
	// destination, and the poison events at their consumer, which refuses
	// them until they are dead-lettered.
	deadline := time.Now().Add(15*time.Minute + 5*time.Minute)
	for {
		g := s.l.gap(math.MaxInt64, -1, true)
		if g.empty() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after publishing ended, the cluster still owes:\n%s", g)
		}
		s.wait(2 * time.Second)
	}
	step(t, "every acknowledged event was delivered to both groups and exported")
	for quiet := time.Now().Add(15 * time.Minute); time.Since(time.UnixMilli(s.lastPoison.Load())) < 2*time.Minute; s.wait(5 * time.Second) {
		if time.Now().After(quiet) {
			t.Errorf("poison events were still being delivered 15 minutes after publishing ended")
			break
		}
	}
	if most := s.l.maxPoisonDeliveries(); most > 10*(soakMaxRetries+1) {
		t.Errorf("a poison event was delivered %d times; with --max-retries=%d it should have been dead-lettered after %d", most, soakMaxRetries, soakMaxRetries+1)
	}

	// Once everything is finished, the logs are pruned down to what is
	// still being written to. Then the replicas must hold the same log, and
	// every acknowledged event that is still in it must be where its
	// producer was told.
	pruned := s.waitPruned(5 * time.Minute)
	for _, sc := range s.consumers {
		if !sc.stopping.Load() {
			s.unsubscribe(sc)
		}
	}
	stopSamplingOnce()
	if pruned {
		s.checkFinalLogs()
	}

	s.l.mu.Lock()
	early, unknown, cdcUnknown := slices.Clone(s.l.early), slices.Clone(s.l.unknown), s.l.cdcUnknown
	earlyN, unknownN := s.l.earlyN, s.l.unknownN
	s.l.mu.Unlock()
	if earlyN > 0 {
		t.Errorf("%d deliveries arrived before their scheduled time:\n  %s", earlyN, strings.Join(early, "\n  "))
	}
	if unknownN > 0 {
		t.Errorf("%d deliveries were of events nobody published:\n  %s", unknownN, strings.Join(unknown, "\n  "))
	}
	if cdcUnknown > 0 {
		t.Errorf("the CDC destination received %d events nobody published", cdcUnknown)
	}
	s.report(earlyN, unknownN)
}

// waitPruned waits until no partition's log holds more than two segments'
// worth of entries on any node, and reports whether that happened.
func (s *soak) waitPruned(within time.Duration) bool {
	s.t.Helper()
	limit := 2 * s.cfg.segmentBytes / int64(len(soakSmall))
	deadline := time.Now().Add(within)
	for {
		largest, where := int64(0), ""
		for _, n := range s.c.nodes {
			for p := int32(0); p < partitionCount; p++ {
				first, last, err := s.c.logBounds(n, p)
				if err != nil {
					largest, where = math.MaxInt64, fmt.Sprintf("%s partition %d: %v", n.id, p, err)
					continue
				}
				if size := last - first + 1; size > largest {
					largest, where = size, fmt.Sprintf("%s partition %d holds offsets %d to %d", n.id, p, first, last)
				}
			}
		}
		if largest <= limit {
			step(s.t, "the logs are pruned; the largest: %s", where)
			return true
		}
		if time.Now().After(deadline) {
			s.t.Errorf("%s after everything was delivered and exported, the logs were not pruned: %s", within, where)
			return false
		}
		s.wait(5 * time.Second)
	}
}

// checkFinalLogs compares every partition's log across its replicas and with
// what producers were told.
func (s *soak) checkFinalLogs() {
	s.t.Helper()
	var wrong []string
	for p := int32(0); p < partitionCount; p++ {
		log, start := agreedLog(s.t, s.c, p, soakTopic)
		at := make(map[string]int64, len(log))
		for _, e := range log {
			id := e.GetMessageId()
			if !strings.HasPrefix(id, s.l.run+".") {
				continue
			}
			if prev, dup := at[id]; dup {
				wrong = append(wrong, fmt.Sprintf("%s is in partition %d at offsets %d and %d", id, p, prev, e.GetOffset()))
			}
			at[id] = e.GetOffset()
		}
		s.l.mu.Lock()
		for _, st := range s.l.streams {
			for seq, ev := range st.events {
				if ev.flags&soakAccepted == 0 || int32(ev.partition) != p || ev.offset == 0 || int64(ev.offset)-1 < start {
					continue
				}
				id := s.l.id(st, seq)
				if got, ok := at[id]; !ok || got != int64(ev.offset)-1 {
					wrong = append(wrong, fmt.Sprintf("%s was acknowledged at partition %d offset %d; the log has it at %d (present: %v)", id, p, int64(ev.offset)-1, got, ok))
				}
			}
		}
		s.l.mu.Unlock()
	}
	if len(wrong) > 0 {
		s.t.Errorf("%d events are not where their producers were told:\n  %s", len(wrong), sample(wrong, 25))
	}
}

// growth fits a line through a node's resident memory over each lifetime of
// its process of at least an hour, leaving out its first 15 minutes, and
// returns the steepest slope in MiB per hour with the span it was fitted on.
func growth(samples []nodeSample, value func(nodeSample) float64) (slope float64, span time.Duration) {
	lifetimes := map[float64][]nodeSample{}
	for _, sm := range samples {
		if sm.resident && sm.processStart > 0 {
			lifetimes[sm.processStart] = append(lifetimes[sm.processStart], sm)
		}
	}
	for started, list := range lifetimes {
		from := time.Unix(int64(started), 0).Add(15 * time.Minute)
		var xs, ys []float64
		for _, sm := range list {
			if sm.at.After(from) {
				xs = append(xs, sm.at.Sub(from).Hours())
				ys = append(ys, value(sm))
			}
		}
		if len(xs) < 10 || xs[len(xs)-1]-xs[0] < 1 {
			continue
		}
		var mx, my float64
		for i := range xs {
			mx += xs[i]
			my += ys[i]
		}
		mx /= float64(len(xs))
		my /= float64(len(xs))
		var num, den float64
		for i := range xs {
			num += (xs[i] - mx) * (ys[i] - my)
			den += (xs[i] - mx) * (xs[i] - mx)
		}
		if den > 0 && (num/den > slope || span == 0) {
			slope, span = num/den, time.Duration((xs[len(xs)-1]-xs[0])*float64(time.Hour))
		}
	}
	return slope, span
}

func (s *soak) report(early, unknown int) {
	s.t.Helper()
	nodes := map[string]any{}
	s.samplesMu.Lock()
	for id, list := range s.samples {
		var peakRSS, peakHeap, peakHeld, peakGoroutines float64
		var peakData, peakDLQ, lastData int64
		for _, sm := range list {
			peakRSS, peakHeap, peakHeld = max(peakRSS, sm.rss), max(peakHeap, sm.heap), max(peakHeld, sm.held)
			peakGoroutines = max(peakGoroutines, sm.goroutines)
			peakData, peakDLQ = max(peakData, sm.dataBytes), max(peakDLQ, sm.dlqBytes)
			lastData = sm.dataBytes
		}
		rssSlope, rssSpan := growth(list, func(sm nodeSample) float64 { return sm.rss / (1 << 20) })
		goSlope, _ := growth(list, func(sm nodeSample) float64 { return sm.goroutines })
		nodes[id] = map[string]any{
			"peak_rss_mib": peakRSS / (1 << 20), "peak_heap_inuse_mib": peakHeap / (1 << 20), "peak_held_mib": peakHeld / (1 << 20),
			"peak_goroutines": peakGoroutines, "peak_data_mib": peakData >> 20, "final_data_mib": lastData >> 20, "peak_dlq_kib": peakDLQ >> 10,
			"rss_slope_mib_per_hour": rssSlope, "rss_slope_span_hours": rssSpan.Hours(), "goroutine_slope_per_hour": goSlope,
		}
		// A lifetime of four hours or more whose memory climbs by more than
		// 16 MiB an hour is growth this soak cannot explain.
		if rssSpan >= 4*time.Hour && rssSlope > 16 {
			s.t.Errorf("%s: resident memory grew by %.1f MiB an hour over %s", id, rssSlope, rssSpan.Round(time.Minute))
		}
		if peakRSS > 1.5*float64(s.cfg.nodeMemory) {
			s.t.Errorf("%s: resident memory reached %.0f MiB against a limit of %d MiB", id, peakRSS/(1<<20), s.cfg.nodeMemory>>20)
		}
	}
	s.samplesMu.Unlock()
	summary := map[string]any{
		"run":                   s.l.run,
		"result":                map[bool]string{true: "failed", false: "passed"}[s.t.Failed()],
		"settings":              map[string]any{"duration": s.cfg.duration.String(), "rate": s.cfg.rate, "burst_rate": s.cfg.burstRate, "burst_every": s.cfg.burstEvery.String(), "burst_for": s.cfg.burstFor.String(), "seed": s.cfg.seed, "node_memory_mib": s.cfg.nodeMemory >> 20, "node_gomaxprocs": s.cfg.nodeProcs, "segment_mib": s.cfg.segmentBytes >> 20, "fsync": "every_event", "encryption": true, "replication_tls": true, "rf": 3, "min_isr": 2, "partitions": partitionCount, "auth": false},
		"steady_start":          s.steadyStart,
		"publish_end":           s.publishEnd,
		"accepted":              s.l.accepted.Load(),
		"delivered_main":        s.l.mainDelivered.Load(),
		"delivered_slow":        s.l.slowDelivered.Load(),
		"exported":              s.l.exported.Load(),
		"poison_delivered":      s.l.poisonDelivered.Load(),
		"max_poison_deliveries": s.l.maxPoisonDeliveries(),
		"early_deliveries":      early,
		"unknown_deliveries":    unknown,
		"refused_attempts":      s.refused.Load(),
		"failed_attempts":       s.failedAttempts.Load(),
		"rate_shortfall":        s.shortfall.Load(),
		"cdc_requests":          s.cdc.requests.Load(),
		"cdc_refused":           s.cdc.refused.Load(),
		"publish_latency":       s.publishLatency.summary(),
		"delivery_lateness":     s.l.lateness.summary(),
		"disruptions":           s.records,
		"nodes":                 nodes,
	}
	data, _ := json.MarshalIndent(summary, "", "  ")
	if err := os.WriteFile(filepath.Join(s.out, "summary.json"), data, 0o644); err != nil {
		s.t.Error(err)
	}
	s.t.Logf("summary:\n%s", data)
}
