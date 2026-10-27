# Closed WAL segment boundaries

A segment is preallocated, so zero bytes after the last readable record can be
either unused space or erased data. Record checksums alone cannot distinguish
the two if all records beyond that point are gone. A sparse index is also not
an authoritative boundary: it can be absent, incomplete or stale.

## Boundary evidence

For a newly rotated segment, the header of its successor records the closed
segment's first and last offsets and logical byte end. The successor header's
existing CRC covers these fields. The predecessor is flushed and synced before
the successor header is created and synced; its directory entry is synced on
platforms that support directory fsync. A failure while publishing the header
fences the WAL if publication is uncertain. Version 2 uses the same header
size and places the boundary in bytes previously reserved by version 1.
The layout is `BND1` at bytes 32–35, followed by three big-endian 64-bit
values at bytes 36–59: predecessor first offset, last offset, and logical byte
end. The existing CRC covers bytes 0–59. Every successor must contain `BND1`;
the first retained segment may omit it.

At startup and during `cronos-admin check-log`, recovery compares each recorded
boundary to the prefix recovered from the preceding segment. A mismatch is
reported as log damage, including when the successor is empty, and startup
fails closed. This does not rely on an index. Explicit offline repair cuts the
damaged segment at its last readable record and updates the successor boundary;
it discards all later records in that segment, even if some could still be read.
If readable records extend beyond the recorded boundary, offline repair refuses
to change the segment; restore a backup or rebuild the replica instead.
For a replicated partition, prefer rebuilding the replica from a healthy leader
or restoring a backup over cutting out data.

WAL checkpoints and full backups copy the successor header along with the log.
The legacy incremental backup excludes active records but copies the active
header as an empty successor when it carries the last closed segment's
boundary; a later incremental backup replaces that placeholder with the full
segment. Snapshot copies of an active segment are cuts, so events appended
after the cut are not included. The legacy incremental `BackupWAL` helper is
not a coordinated live checkpoint; do not use it concurrently with truncation or
retention. Production full backups use coordinated checkpoint cuts.

## Interrupted operations

A successor's original boundary stays in place until that successor is deleted
during follower truncation. A crash after its predecessor was shortened but
before the successor was deleted may therefore cause startup to fail closed;
rebuild that replica from its leader. Repair writes and syncs the discarded
tail before updating the successor's header. A torn header update fails the
header CRC and requires recovery from a healthy replica or backup.

## Clean-start requirement and limits

From v0.9.0 the server reads and writes only version-2 WAL segments. It
rejects version-1 data and backups without modifying or migrating their WAL
segments. Older binaries reject version-2 segments. Start with new, empty data
directories and use the updated binary on every node; rolling upgrades and
mixed-version clusters are unsupported. Keep existing directories separately if
their data might be needed. Do not restore a version-1 backup into a version-2
cluster.

The currently active segment has no closed boundary; a zeroed active tail can
still resemble an interrupted write. Removing a successor (including by
retention) also removes the evidence it contains, and deletion of whole files,
simultaneous loss of data and its evidence, maliciously recomputed checksums,
and continuous detection while a node is running are outside this mechanism.
The sparse-index check can identify some additional damage but is not a proof
that the rest of a log is intact.
