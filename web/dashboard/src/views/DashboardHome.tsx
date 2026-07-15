import { useClusterTopology } from "@/lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Loading } from "@/components/Loading";
import { ErrorAlert } from "@/components/ErrorAlert";

export function DashboardHome() {
  const { data, loading, error, refetch } = useClusterTopology();

  if (loading) return <Loading message="Loading cluster snapshot..." />;
  if (error) return <ErrorAlert message={error} onRetry={refetch} />;
  if (!data) return <ErrorAlert message="No cluster snapshot available." onRetry={refetch} />;

  const mode = data.is_cluster_mode ? "cluster" : "standalone";
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>CronosDB Admin</CardTitle>
          <CardDescription>
            Connected to <span className="font-mono">{data.local_node_id}</span> in{" "}
            <Badge variant={data.is_cluster_mode ? "default" : "secondary"}>{mode}</Badge>{" "}
            mode.
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
