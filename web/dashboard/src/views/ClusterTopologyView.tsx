import { useAdmin } from "@/lib/api-mock";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

export function ClusterTopologyView() {
  const admin = useAdmin();
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Cluster Topology</CardTitle>
          <CardDescription>
            Snapshot from <span className="font-mono">{admin.local_node_id}</span> at{" "}
            <span className="font-mono">
              {new Date(admin.captured_at_unix_ms).toISOString()}
            </span>
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
            <Stat label="Total Nodes" value={admin.total_nodes} />
            <Stat label="Alive Nodes" value={admin.alive_nodes} />
            <Stat label="Total Partitions" value={admin.total_partitions} />
            <Stat label="Leader Partitions" value={admin.leader_partitions} />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Nodes</CardTitle>
        </CardHeader>
        <CardContent>
          {admin.nodes.length === 0 ? (
            <p className="text-sm text-[var(--color-muted-foreground)]">No nodes reported.</p>
          ) : (
            <table className="w-full text-sm">
              <thead className="text-left text-[var(--color-muted-foreground)]">
                <tr>
                  <th className="py-2 font-medium">Node ID</th>
                  <th className="py-2 font-medium">State</th>
                  <th className="py-2 font-medium">Local</th>
                </tr>
              </thead>
              <tbody>
                {admin.nodes.map((n) => (
                  <tr key={n.node_id} className="border-t border-[var(--color-border)]">
                    <td className="py-2 font-mono">{n.node_id}</td>
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
          {admin.partitions.length === 0 ? (
            <p className="text-sm text-[var(--color-muted-foreground)]">
              No partitions assigned to this node in the current snapshot.
            </p>
          ) : (
            <table className="w-full text-sm">
              <thead className="text-left text-[var(--color-muted-foreground)]">
                <tr>
                  <th className="py-2 font-medium">ID</th>
                  <th className="py-2 font-medium">Leader</th>
                  <th className="py-2 font-medium">In-Sync Count</th>
                  <th className="py-2 font-medium">HWM</th>
                </tr>
              </thead>
              <tbody>
                {admin.partitions.map((p) => (
                  <tr key={p.partition_id} className="border-t border-[var(--color-border)]">
                    <td className="py-2 font-mono">{p.partition_id}</td>
                    <td className="py-2 font-mono">{p.leader_id}</td>
                    <td className="py-2">{p.in_sync_count}</td>
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
