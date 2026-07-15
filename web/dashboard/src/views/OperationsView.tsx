import { useEffect, useState } from "react";
import { useClusterTopology, useRunRetention, useRunCompaction, useTriggerRebalance } from "@/lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Loading } from "@/components/Loading";
import { ErrorAlert } from "@/components/ErrorAlert";
import { useToast } from "@/components/ToastProvider";

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
  const { addToast } = useToast();

  useEffect(() => {
    if (!retention.data && !retention.error) return;
    if (retention.error) {
      addToast({ title: "Retention failed", description: retention.error, variant: "error" });
      return;
    }
    if (retention.data?.success) {
      addToast({
        title: "Retention completed",
        description: `Removed ${retention.data.events_removed} events.`,
        variant: "success",
      });
    } else {
      addToast({
        title: "Retention completed with issues",
        description: retention.data?.error || "No events removed.",
        variant: "error",
      });
    }
  }, [retention.data, retention.error, addToast]);

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
    </div>
  );
}

function CompactionPanel({ partitions }: { partitions: number[] }) {
  const [partitionId, setPartitionId] = useState<number>(partitions[0] ?? 0);
  const compaction = useRunCompaction();
  const { addToast } = useToast();

  useEffect(() => {
    if (!compaction.data && !compaction.error) return;
    if (compaction.error) {
      addToast({ title: "Compaction failed", description: compaction.error, variant: "error" });
      return;
    }
    if (compaction.data?.success) {
      addToast({
        title: "Compaction completed",
        description: `Compacted ${compaction.data.segments_compacted} segments.`,
        variant: "success",
      });
    } else {
      addToast({
        title: "Compaction completed with issues",
        description: compaction.data?.error || "No segments compacted.",
        variant: "error",
      });
    }
  }, [compaction.data, compaction.error, addToast]);

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
    </div>
  );
}

function RebalancePanel() {
  const rebalance = useTriggerRebalance();
  const { addToast } = useToast();

  useEffect(() => {
    if (!rebalance.data && !rebalance.error) return;
    if (rebalance.error) {
      addToast({ title: "Rebalance failed", description: rebalance.error, variant: "error" });
      return;
    }
    if (rebalance.data?.success) {
      addToast({
        title: "Rebalance triggered",
        description: `Partitions moved: ${rebalance.data.partitions_moved}. ${rebalance.data.error || ""}`,
        variant: rebalance.data.partitions_moved > 0 ? "success" : "default",
      });
    } else {
      addToast({
        title: "Rebalance completed with issues",
        description: rebalance.data?.error || "No partitions moved.",
        variant: "error",
      });
    }
  }, [rebalance.data, rebalance.error, addToast]);

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
    </div>
  );
}
