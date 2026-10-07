package cluster

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// A cluster is created once, by one node. Every node of it remembers the
// cluster in its Raft state, so a node that has that state never creates
// anything. The question is what a node without it should do, and "create a
// cluster" is the right answer only the first time: the same node, back with
// an empty disk, would create a second cluster beside the one it belonged
// to, add the other nodes to it, and leave them with two histories of who
// leads what.
//
// So a node creates a cluster only when it is the one told to
// (--cluster-bootstrap), has no state, and every other node in its seed list
// has said that it has none either. One node that has state is enough for
// the answer to be no. A node that cannot be reached leaves the question
// open, and the node waits: it cannot know what the silent node knows.

const (
	// probeTimeout bounds one question to one node.
	probeTimeout = 3 * time.Second
	// probeInterval is the pause between rounds of questions, and
	// probeLogInterval how often a node that is still waiting says so.
	probeInterval    = time.Second
	probeLogInterval = 15 * time.Second
)

// probeAnswer is what a node says when asked whether it belongs to a cluster.
type probeAnswer struct {
	NodeID string `json:"node_id"`
	// ClusterFormed is true when the node has Raft state: it created a
	// cluster or was added to one.
	ClusterFormed bool `json:"cluster_formed"`
}

// probeNode asks the node at addr whether it belongs to a cluster.
func probeNode(ctx context.Context, addr, self string, clientTLS *tls.Config) (probeAnswer, error) {
	var answer probeAnswer
	conn, err := dialPeer(ctx, addr, probeTimeout, clientTLS)
	if err != nil {
		return answer, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(probeTimeout))
	if err := json.NewEncoder(conn).Encode(&GossipMessage{Type: "probe", NodeID: self, Timestamp: time.Now().UnixMilli()}); err != nil {
		return answer, err
	}
	if err := json.NewDecoder(conn).Decode(&answer); err != nil {
		return answer, err
	}
	if answer.NodeID == "" {
		return answer, fmt.Errorf("answer from %s names no node", addr)
	}
	return answer, nil
}

// ownAddress reports whether a seed address is this node's membership port,
// which it listens on at bind. A seed list usually names every node, this one
// included, and this one cannot be asked: it is not listening yet.
func ownAddress(seed, bind string) bool {
	if seed == bind {
		return true
	}
	seedAddr, err := net.ResolveTCPAddr("tcp", seed)
	if err != nil {
		return false
	}
	bindAddr, err := net.ResolveTCPAddr("tcp", bind)
	if err != nil || seedAddr.Port != bindAddr.Port {
		return false
	}
	if bindAddr.IP != nil && !bindAddr.IP.IsUnspecified() {
		return bindAddr.IP.Equal(seedAddr.IP)
	}
	// Bound to every interface: the port at any address of this machine.
	if seedAddr.IP.IsLoopback() {
		return true
	}
	addrs, _ := net.InterfaceAddrs()
	for _, addr := range addrs {
		if network, ok := addr.(*net.IPNet); ok && network.IP.Equal(seedAddr.IP) {
			return true
		}
	}
	return false
}

// otherSeeds returns the seed addresses that are not this node's own.
func otherSeeds(config *ClusterConfig) []string {
	var others []string
	for _, seed := range config.SeedNodes {
		seed = strings.TrimSpace(seed)
		if seed == "" || ownAddress(seed, config.BindAddr) {
			continue
		}
		others = append(others, seed)
	}
	return others
}

// createsCluster decides whether this node creates the cluster. hasState says
// whether it has Raft state on disk. It may wait, for as long as nodes it has
// to ask do not answer; it gives up only when the manager is stopped.
func (m *Manager) createsCluster(hasState bool) (bool, error) {
	others := otherSeeds(m.config)
	switch {
	case hasState:
		// What is on disk says which cluster this node belongs to.
		return false, nil
	case len(others) == 0:
		// Nobody to ask: a single node, or the first node of a cluster whose
		// other nodes are pointed at it.
		if m.config.ReplicationFactor > 1 || m.config.ExpectedNodes > 1 {
			log.Printf("[CLUSTER] WARNING: this node names no other node in --cluster-seeds, so it creates a cluster whenever it starts without state. " +
				"If it comes back with an empty disk while the cluster exists, it creates a second one. " +
				"Name every node in --cluster-seeds on every node, and give this one --cluster-bootstrap")
		}
		return true, nil
	case !m.config.Bootstrap:
		return false, nil
	}

	log.Printf("[CLUSTER] This node may create the cluster. Asking the %d other nodes whether one exists", len(others))
	started, lastLog := time.Now(), time.Time{}
	for {
		none := make(map[string]bool, len(others)) // seeds that said they have no cluster
		var formed []string
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, addr := range others {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				answer, err := probeNode(m.ctx, addr, m.config.NodeID, m.config.ClientTLS)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err != nil:
				case answer.NodeID == m.config.NodeID:
					none[addr] = true // this node under another name
				case answer.ClusterFormed:
					formed = append(formed, answer.NodeID)
				default:
					none[addr] = true
				}
			}(addr)
		}
		wg.Wait()

		if len(formed) > 0 {
			sort.Strings(formed)
			log.Printf("[CLUSTER] A cluster exists already, known to %s. This node has no state and joins it; it does not create one", strings.Join(formed, ", "))
			return false, nil
		}
		if len(none) == len(others) {
			log.Printf("[CLUSTER] None of the other nodes belongs to a cluster")
			return true, nil
		}
		if time.Since(lastLog) >= probeLogInterval {
			lastLog = time.Now()
			var silent []string
			for _, addr := range others {
				if !none[addr] {
					silent = append(silent, addr)
				}
			}
			log.Printf("[CLUSTER] Not creating a cluster yet: %s did not answer whether one exists (waiting for %s). "+
				"A node that has no state cannot tell a new cluster from one it has lost its copy of",
				strings.Join(silent, ", "), time.Since(started).Round(time.Second))
		}
		select {
		case <-m.ctx.Done():
			return false, fmt.Errorf("stopped while waiting to learn whether a cluster exists")
		case <-time.After(probeInterval):
		}
	}
}
