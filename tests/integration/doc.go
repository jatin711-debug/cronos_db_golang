// Package integration contains end-to-end integration tests for CronosDB.
//
// These tests require a running CronosDB server. Set CRONOS_TEST_ADDR to the
// gRPC address (default: localhost:9000) and CRONOS_TEST_HTTP_ADDR to the
// HTTP health address (default: localhost:8080).
//
// Run with: go test -v -tags integration ./tests/integration/...
package integration
