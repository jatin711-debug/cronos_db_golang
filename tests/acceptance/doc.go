// Package acceptance holds the release acceptance tests.
//
// They run CronosDB the way it is deployed, as separate server processes that
// form a three-node cluster on this machine, and use it the way an application
// does, through the client library. While producers and a consumer group are
// working, the tests kill, freeze and restart the nodes, and at the end they
// check what a release promises:
//
//   - every publish that was acknowledged to its producer was delivered,
//   - nothing was delivered before its scheduled time,
//   - every replica of a partition ends up with the same log, and that log
//     holds each acknowledged event exactly once, at the offset it was
//     acknowledged with.
//
// The tests are behind the "acceptance" build tag because they build the
// server and take a few minutes:
//
//	go test -tags acceptance -count=1 -timeout 20m ./tests/acceptance/
//
// CRONOS_BINARY names a server binary to use instead of building one, and
// CRONOS_ACCEPTANCE_ARTIFACTS a directory the node logs are copied to.
package acceptance
