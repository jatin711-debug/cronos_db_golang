import { useClusterTopology } from "@/lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Loading } from "@/components/Loading";
import { ErrorAlert } from "@/components/ErrorAlert";

export function ClusterTopologyView() {
  const { data, loading, error, refetch } = useClusterTopology();

  if (loading) return <Loading message="Loading cluster topology..." />;
  if (error) return <ErrorAlert message={error} onRetry={refetch} />;
  if (!data) return <ErrorAlert message="No topology available." onRetry={refetch} />;

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Cluster Topology</CardTitle>
          <CardDescription>
            Snapshot from <span className="font-mono">{data.local_node_id}</span> at{" "}
            <span className="font-mono">
              {new Date(data.captured_at_unix_ms).toISOString()}
            </span>
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
            <Stat label="Total Nodes" value={data.total_nodes} />
            <Stat label="Alive Nodes" value={data.alive_nodes} />
            <Stat label="Total Partitions" value={data.total_partitions} />
            <Stat label="Leader Partitions" value={data.leader_partitions} />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Nodes</CardTitle>
        </CardHeader>
        <CardContent>
          {data.nodes.length === 0 ? (
            <p className="text-sm text-[var(--color-muted-foreground)]">No nodes reported.</p>
          ) : (
            <table className="w-full text-sm">
              <thead className="text-left text-[var(--color-muted-foreground)]">
                <tr>
                  <th className="py-2 font-medium">Node ID</th>
                  <th className="py-2 font-medium">Address</th>
                  <th className="py-2 font-medium">State</th>
                  <th className="py-2 font-medium">Local</th>
                </tr>
              </thead>
              <tbody>
                {data.nodes.map((n) => (
                  <tr key={n.node_id} className="border-t border-[var(--color-border)]">
                    <td className="py-2 font-mono">{n.node_id}</td>
                    <td className="py-2 font-mono text-[var(--color-muted-foreground)]">{n.address || "-"}</td>
                    <td className="py-2">
                      <Badge variant={n.is_alive ? "success" : "destructive"}>{n.state}</Badge>
                    </td>
                    <td className="py-2">{n.is_local ? "yes" : "no"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Partitions</CardTitle>
        </CardHeader>
        <CardContent>
          {data.partitions.length === 0 ? (
            <p className="text-sm text-[var(--color-muted-foreground)]">
              No partitions assigned to this node in the current snapshot.
            </p>
          ) : (
            <table className="w-full text-sm">
              <thead className="text-left text-[var(--color-muted-foreground)]">
                <tr>
                  <th className="py-2 font-medium">ID</th>
                  <th className="py-2 font-medium">Leader</th>
                  <th className="py-2 font-medium">Replicas</th>
                  <th className="py-2 font-medium">ISR</th>
                  <th className="py-2 font-medium">HWM</th>
                </tr>
              </thead>
              <tbody>
                {data.partitions.map((p) => (
                  <tr key={p.partition_id} className="border-t border-[var(--color-border)]">
                    <td className="py-2 font-mono">{p.partition_id}</td>
                    <td className="py-2 font-mono">{p.leader_id}</td>
                    <td className="py-2 font-mono">{p.replicas.join(", ") || "-"}</td>
                    <td className="py-2 font-mono">{p.isr.join(", ") || "-"}</td>
                    <td className="py-2 font-mono">{p.leader_high_watermark}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </CardContent>
      </Card>
    </div>
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
