package replication

// NeedsSnapshot decides whether a follower whose log ends at localLast copies
// its leader's log, which holds the offsets from leaderStart to leaderLast,
// as a snapshot instead of having what it lacks replayed. Replay sends the
// entries after localLast, or all of the leader's log when the follower's
// ends before the leader's starts; a snapshot sends all of it. So the
// follower must lack at least threshold entries and at least half of the
// leader's log: a snapshot then sends at most twice what replay would, and
// sends it as files instead of batches that are each verified and synced.
// A leaderStart of 0 may mean the leader did not say where its log starts;
// counting from 0 makes the follower ask for a snapshot less readily.
func NeedsSnapshot(localLast, leaderStart, leaderLast, threshold int64) bool {
	if threshold <= 0 || leaderLast < 0 {
		return false
	}
	leaderStart = max(leaderStart, 0)
	held := leaderLast - leaderStart + 1
	missing := leaderLast - max(localLast, leaderStart-1)
	return missing >= threshold && 2*missing >= held
}
