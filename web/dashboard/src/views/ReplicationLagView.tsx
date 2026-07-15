import { useState } from "react";
import { useClusterTopology, useReplicationLag } from "@/lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Loading } from "@/components/Loading";
import { ErrorAlert } from "@/components/ErrorAlert";

export function ReplicationLagView() {
  const topology = useClusterTopology();
  const [selectedId, setSelectedId] = useState<number | undefined>(undefined);

  const partitionIds = topology.data?.partitions.map((p) => p.partition_id) ?? [];

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Replication Lag</CardTitle>
          <CardDescription>Inspect follower lag for a partition.</CardDescription>
        </CardHeader>
        <CardContent>
          {topology.loading && <Loading message="Loading partitions..." />}
          {topology.error && <ErrorAlert message={topology.error} onRetry={topology.refetch} isAuth={topology.error.startsWith("401")} />}
          {topology.data && partitionIds.length === 0 && (
            <p className="text-sm text-[var(--color-muted-foreground)]">No partitions available on this node.</p>
          )}
          {topology.data && partitionIds.length > 0 && (
            <div className="flex flex-wrap gap-2">
              <button
                onClick={() => setSelectedId(undefined)}
                className={`rounded-md border px-3 py-1.5 text-sm transition-colors ${
                  selectedId === undefined
                    ? "border-[var(--color-primary)] bg-[var(--color-primary)] text-[var(--color-primary-foreground)]"
                    : "border-[var(--color-border)] hover:bg-[var(--color-accent)]"
                }`}
              >
                All partitions
              </button>
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

      <LagDetail partitionId={selectedId} />
    </div>
  );
}

function LagDetail({ partitionId }: { partitionId?: number }) {
  const { data, loading, error, refetch } = useReplicationLag(partitionId);

  if (loading) return <Loading message="Loading replication lag..." />;
  if (error) return <ErrorAlert message={error} onRetry={refetch} isAuth={error.startsWith("401")} />;
  if (!data) return <ErrorAlert message="No lag data returned." onRetry={refetch} />;

  return (
    <Card>
      <CardHeader>
        <CardTitle>
          {data.partition_id === 0 ? "All Partitions" : `Partition ${data.partition_id}`}
        </CardTitle>
        <CardDescription>Leader HWM: {data.leader_high_watermark}</CardDescription>
      </CardHeader>
      <CardContent>
        {data.followers.length === 0 ? (
          <p className="text-sm text-[var(--color-muted-foreground)]">No follower offsets reported.</p>
        ) : (
          <table className="w-full text-sm">
            <thead className="text-left text-[var(--color-muted-foreground)]">
              <tr>
                <th className="py-2 font-medium">Follower</th>
                <th className="py-2 font-medium">Offset</th>
                <th className="py-2 font-medium">Lag Events</th>
                <th className="py-2 font-medium">Lag Seconds</th>
                <th className="py-2 font-medium">In Sync</th>
              </tr>
            </thead>
            <tbody>
              {data.followers.map((f) => (
                <tr key={f.follower_id} className="border-t border-[var(--color-border)]">
                  <td className="py-2 font-mono">{f.follower_id}</td>
                  <td className="py-2 font-mono">{f.offset}</td>
                  <td className="py-2 font-mono">{f.lag_events}</td>
                  <td className="py-2 font-mono">{f.lag_seconds}</td>
                  <td className="py-2">
                    <Badge variant={f.in_sync ? "success" : "warning"}>{f.in_sync ? "yes" : "no"}</Badge>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </CardContent>
    </Card>
  );
}
