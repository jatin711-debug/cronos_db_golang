import { useState } from "react";
import { useClusterTopology, usePartitionHealth } from "@/lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Loading } from "@/components/Loading";
import { ErrorAlert } from "@/components/ErrorAlert";

export function PartitionHealthView() {
  const topology = useClusterTopology();
  const [selectedId, setSelectedId] = useState<number | null>(null);

  const partitionIds = topology.data?.partitions.map((p) => p.partition_id) ?? [];

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Partition Health</CardTitle>
          <CardDescription>Select a partition to inspect its local health snapshot.</CardDescription>
        </CardHeader>
        <CardContent>
          {topology.loading && <Loading message="Loading partitions..." />}
          {topology.error && <ErrorAlert message={topology.error} onRetry={topology.refetch} />}
          {topology.data && partitionIds.length === 0 && (
            <p className="text-sm text-[var(--color-muted-foreground)]">No partitions available on this node.</p>
          )}
          {topology.data && partitionIds.length > 0 && (
            <div className="flex flex-wrap gap-2">
              {partitionIds.map((id) => (
                <button
                  key={id}
                  onClick={() => setSelectedId(id)}
                  className={`rounded-md border px-3 py-1.5 text-sm transition-colors ${
                    selectedId === id
                      ? "border-[var(--color-primary)] bg-[var(--color-primary)] text-[var(--color-primary-foreground)]"
                      : "border-[var(--color-border)] hover:bg-[var(--color-accent)]"
                  }`}
                >
                  Partition {id}
                </button>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {selectedId !== null && <HealthDetail partitionId={selectedId} />}
    </div>
  );
}

function HealthDetail({ partitionId }: { partitionId: number }) {
  const { data, loading, error, refetch } = usePartitionHealth(partitionId);

  if (loading) return <Loading message="Loading partition health..." />;
  if (error) return <ErrorAlert message={error} onRetry={refetch} />;
  if (!data) return <ErrorAlert message="No health data returned." onRetry={refetch} />;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Partition {data.partition_id} Health</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-3">
          <Stat label="High Watermark" value={data.high_watermark} />
          <Stat label="First Offset" value={data.first_offset} />
          <Stat label="Segments" value={data.segment_count} />
          <Stat label="Disk Usage" value={data.disk_usage_bytes} />
          <Stat label="Active Timers" value={data.active_timers} />
          <Stat label="Ready Events" value={data.ready_events} />
          <Stat label="Cold Store Entries" value={data.cold_store_entries} />
        </div>
        {data.last_error && (
          <p className="mt-4 text-sm text-[var(--color-destructive)]">Last error: {data.last_error}</p>
        )}
        {data.replay_error && (
          <p className="mt-2 text-sm text-[var(--color-destructive)]">Replay error: {data.replay_error}</p>
        )}
      </CardContent>
    </Card>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div>
      <div className="text-2xl font-semibold">{value}</div>
      <div className="text-xs text-[var(--color-muted-foreground)]">{label}</div>
    </div>
  );
}
