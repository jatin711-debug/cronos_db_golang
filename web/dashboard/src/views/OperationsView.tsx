import { useState } from "react";
import { useClusterTopology, useRunRetention, useRunCompaction, useTriggerRebalance } from "@/lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Loading } from "@/components/Loading";
import { ErrorAlert } from "@/components/ErrorAlert";

export function OperationsView() {
  const topology = useClusterTopology();
  const partitionIds = topology.data?.partitions.map((p) => p.partition_id) ?? [];

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Operations</CardTitle>
          <CardDescription>Run administrative actions against the cluster.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-8">
          {topology.loading && <Loading message="Loading partitions..." />}
          {topology.error && <ErrorAlert message={topology.error} onRetry={topology.refetch} isAuth={topology.error.startsWith("401")} />}
          {topology.data && partitionIds.length === 0 && (
            <p className="text-sm text-[var(--color-muted-foreground)]">No partitions available on this node.</p>
          )}
          {topology.data && partitionIds.length > 0 && (
            <>
              <RetentionPanel partitions={partitionIds} />
              <CompactionPanel partitions={partitionIds} />
              <RebalancePanel />
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function RetentionPanel({ partitions }: { partitions: number[] }) {
  const [partitionId, setPartitionId] = useState<number>(partitions[0] ?? 0);
  const [maxAgeHours, setMaxAgeHours] = useState<number>(24);
  const retention = useRunRetention();

  return (
    <div className="rounded-lg border border-[var(--color-border)] p-4">
      <h3 className="mb-3 text-sm font-medium">Run Retention</h3>
      <div className="flex flex-col gap-3 sm:flex-row sm:items-end">
        <label className="flex flex-col gap-1 text-xs text-[var(--color-muted-foreground)]">
          Partition
          <select
            className="rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 py-2 text-sm text-[var(--color-foreground)] outline-none focus:ring-1 focus:ring-[var(--color-ring)]"
            value={partitionId}
            onChange={(e) => setPartitionId(parseInt(e.target.value, 10))}
          >
            {partitions.map((id) => (
              <option key={id} value={id}>{id}</option>
            ))}
          </select>
        </label>
        <label className="flex flex-col gap-1 text-xs text-[var(--color-muted-foreground)]">
          Max age (hours)
          <input
            type="number"
            min={0}
            value={maxAgeHours}
            onChange={(e) => setMaxAgeHours(parseInt(e.target.value, 10) || 0)}
            className="rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 py-2 text-sm text-[var(--color-foreground)] outline-none focus:ring-1 focus:ring-[var(--color-ring)]"
          />
        </label>
        <Button
          onClick={() => retention.mutate({ partition_id: partitionId, max_age_hours: maxAgeHours })}
          disabled={retention.loading}
        >
          {retention.loading ? "Running..." : "Run Retention"}
        </Button>
      </div>
      {retention.error && <ErrorAlert message={retention.error} onRetry={retention.reset} isAuth={retention.error.startsWith("401")} />}
      {retention.data && (
        <p className="mt-3 text-sm">
          {retention.data.success ? "✅" : "⚠️"} Events removed: {retention.data.events_removed}
          {retention.data.error && ` — ${retention.data.error}`}
        </p>
      )}
    </div>
  );
}

function CompactionPanel({ partitions }: { partitions: number[] }) {
  const [partitionId, setPartitionId] = useState<number>(partitions[0] ?? 0);
  const compaction = useRunCompaction();

  return (
    <div className="rounded-lg border border-[var(--color-border)] p-4">
      <h3 className="mb-3 text-sm font-medium">Run Compaction</h3>
      <div className="flex flex-col gap-3 sm:flex-row sm:items-end">
        <label className="flex flex-col gap-1 text-xs text-[var(--color-muted-foreground)]">
          Partition
          <select
            className="rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 py-2 text-sm text-[var(--color-foreground)] outline-none focus:ring-1 focus:ring-[var(--color-ring)]"
            value={partitionId}
            onChange={(e) => setPartitionId(parseInt(e.target.value, 10))}
          >
            {partitions.map((id) => (
              <option key={id} value={id}>{id}</option>
            ))}
          </select>
        </label>
        <Button onClick={() => compaction.mutate({ partition_id: partitionId })} disabled={compaction.loading}>
          {compaction.loading ? "Running..." : "Run Compaction"}
        </Button>
      </div>
      {compaction.error && <ErrorAlert message={compaction.error} onRetry={compaction.reset} isAuth={compaction.error.startsWith("401")} />}
      {compaction.data && (
        <p className="mt-3 text-sm">
          {compaction.data.success ? "✅" : "⚠️"} Segments compacted:{" "}
          {compaction.data.segments_compacted}
          {compaction.data.error && ` — ${compaction.data.error}`}
        </p>
      )}
    </div>
  );
}

function RebalancePanel() {
  const rebalance = useTriggerRebalance();

  return (
    <div className="rounded-lg border border-[var(--color-border)] p-4">
      <h3 className="mb-3 text-sm font-medium">Cluster Rebalance</h3>
      <p className="mb-3 text-xs text-[var(--color-muted-foreground)]">
        Trigger an explicit leadership rebalance. In the current implementation this is a
        no-op notification; the control plane reconciles leadership every 5 seconds.
      </p>
      <Button onClick={() => rebalance.mutate()} disabled={rebalance.loading}>
        {rebalance.loading ? "Triggering..." : "Trigger Rebalance"}
      </Button>
      {rebalance.error && <ErrorAlert message={rebalance.error} onRetry={rebalance.reset} isAuth={rebalance.error.startsWith("401")} />}
      {rebalance.data && (
        <p className="mt-3 text-sm">
          {rebalance.data.success ? "✅" : "⚠️"} Partitions moved: {rebalance.data.partitions_moved}
          {rebalance.data.error && ` — ${rebalance.data.error}`}
        </p>
      )}
    </div>
  );
}
