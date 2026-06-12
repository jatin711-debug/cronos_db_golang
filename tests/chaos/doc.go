// Package chaos contains failure-injection tests for CronosDB.
//
// These tests require external tooling (toxiproxy, Docker, iptables, libfaketime)
// and are skipped by default. They validate the system's resilience under
// network partitions, node failures, disk corruption, and clock skew.
//
// Run with: go test -v -tags chaos ./tests/chaos/...
package chaos
