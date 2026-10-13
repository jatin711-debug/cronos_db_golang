//go:build acceptance

package acceptance

import (
	"testing"
	"time"
)

// Certificates expire, and the CA that issues them is replaced now and then.
// Whatever issues them writes new files in the place of the old ones, and the
// nodes have to go on with what the files hold: a cluster cannot be stopped to
// be given a certificate.
//
// The test replaces the CA of a running cluster in the three steps an
// operator takes: every node is told of the new CA as well, the nodes get
// certificates of the new CA, the old CA is taken away. Nothing in that makes
// a node connect to another, so the test then restarts one node. It comes
// back with the new files only, and it is let in by, and lets in, two nodes
// that were never restarted: on the membership port, the Raft port and the
// replication port, in both directions. Had the two kept the files they
// started with, they would present a certificate it does not accept and
// refuse the one it presents.
func TestCertificatesAndTheirCAReplacedWhileRunning(t *testing.T) {
	// The nodes look at their files when they make or accept a connection, at
	// most every two seconds.
	const lookAgain = 3 * time.Second

	c := newCluster(t)
	c.startAll()
	w := startWorkload(t, c, "rotated", "rotated-workers", 2, 20, 1)
	accepted := func() int64 { return w.ledger.accepted.Load() }
	delivered := func() int64 { return w.ledger.delivered.Load() }
	w.waitProgress(accepted, 100, 60*time.Second, "publishes accepted by the healthy cluster")
	w.waitProgress(delivered, 50, 60*time.Second, "events delivered by the healthy cluster")

	second := newAuthority(t, "second acceptance cluster CA")
	step(t, "telling every node of a second CA as well")
	replaceFile(t, c.caFile, append(append([]byte{}, c.ca.pem...), second.pem...))
	time.Sleep(lookAgain)

	step(t, "giving the nodes a certificate of the second CA")
	certPEM, keyPEM := second.issue(t, c.certHosts...)
	replaceFile(t, c.keyFile, keyPEM)
	replaceFile(t, c.certFile, certPEM)
	time.Sleep(lookAgain)

	step(t, "taking the first CA away")
	replaceFile(t, c.caFile, second.pem)
	time.Sleep(lookAgain)
	w.waitProgress(accepted, 60, 60*time.Second, "publishes accepted after the files were replaced")

	restarted, kept := c.nodes[0], c.nodes[1:]
	step(t, "restarting %s, which comes back knowing only the second CA; %s and %s keep running", restarted.id, kept[0].id, kept[1].id)
	c.kill(restarted)
	c.start(restarted)
	c.waitReady(c.nodes...)
	w.waitProgress(accepted, 100, 90*time.Second, "publishes accepted after "+restarted.id+" came back")
	w.waitProgress(delivered, 100, 90*time.Second, "events delivered after "+restarted.id+" came back")
	for partition := int32(0); partition < partitionCount; partition++ {
		byPartition := func() int64 { return w.ledger.acceptedByPartition[partition].Load() }
		w.waitProgress(byPartition, 5, 90*time.Second, "publishes accepted on every partition after the certificates were replaced")
	}
	for _, n := range kept {
		if c.logged(n, "A TLS certificate was replaced and read again") == 0 {
			t.Errorf("%s was not restarted and does not say that it read its certificate again", n.id)
		}
		if c.logged(n, "A TLS CA bundle was replaced and read again") == 0 {
			t.Errorf("%s was not restarted and does not say that it read its CA file again", n.id)
		}
	}

	// The cluster has to be whole on the new certificates, not just limping:
	// with one of the nodes that kept running down, the one that came back is
	// needed for every publish.
	step(t, "killing %s; the cluster now depends on %s", kept[0].id, restarted.id)
	c.kill(kept[0])
	w.waitProgress(accepted, 100, 90*time.Second, "publishes accepted by "+restarted.id+" and one other node")
	c.start(kept[0])
	c.waitReady(c.nodes...)

	step(t, "waiting for the remaining deliveries")
	w.stopPublishers()
	checkDelivery(t, w)
	w.stopConsumers()
	for partition := int32(0); partition < partitionCount; partition++ {
		agreedLog(t, c, partition, w.topic)
	}
}
