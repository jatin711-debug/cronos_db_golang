import { useState } from "react";
import { useClusterTopology, useConsumerGroups, useConsumerGroupLag } from "@/lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Loading } from "@/components/Loading";
import { ErrorAlert } from "@/components/ErrorAlert";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from "recharts";

export function ConsumerGroupsView() {
  const groups = useConsumerGroups();
  const topology = useClusterTopology();
  const [selected, setSelected] = useState<{ groupId: string; partitionId: number } | null>(null);

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Consumer Groups</CardTitle>
          <CardDescription>Groups and their assigned partitions.</CardDescription>
        </CardHeader>
        <CardContent>
          {groups.loading && <Loading message="Loading consumer groups..." />}
          {groups.error && <ErrorAlert message={groups.error} onRetry={groups.refetch} isAuth={groups.error.startsWith("401")} />}
          {groups.data && groups.data.groups.length === 0 && (
            <p className="text-sm text-[var(--color-muted-foreground)]">No consumer groups found.</p>
          )}
          {groups.data && groups.data.groups.length > 0 && (
            <table className="w-full text-sm">
              <thead className="text-left text-[var(--color-muted-foreground)]">
                <tr>
                  <th className="py-2 font-medium">Group ID</th>
                  <th className="py-2 font-medium">Topic</th>
                  <th className="py-2 font-medium">Partitions</th>
                  <th className="py-2 font-medium">Members</th>
                </tr>
              </thead>
              <tbody>
                {groups.data.groups.map((g) => (
                  <tr key={g.group_id} className="border-t border-[var(--color-border)]">
                    <td className="py-2 font-mono">{g.group_id}</td>
                    <td className="py-2 font-mono">{g.topic || "-"}</td>
                    <td className="py-2 font-mono">{g.partitions.join(", ") || "-"}</td>
                    <td className="py-2">{g.member_count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Lag Lookup</CardTitle>
          <CardDescription>Select a group and partition to fetch committed offset lag.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {topology.loading && <Loading message="Loading partitions..." />}
          {topology.error && <ErrorAlert message={topology.error} onRetry={topology.refetch} isAuth={topology.error.startsWith("401")} />}
          {topology.data && (
            <div className="flex flex-col gap-4 sm:flex-row">
              <select
                className="rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 py-2 text-sm outline-none focus:ring-1 focus:ring-[var(--color-ring)]"
                onChange={(e) =>
                  setSelected((s) => ({
                    groupId: e.target.value,
                    partitionId: s?.partitionId ?? topology.data?.partitions[0]?.partition_id ?? 0,
                  }))
                }
                value={selected?.groupId ?? ""}
              >
                <option value="">Select group...</option>
                {groups.data?.groups.map((g) => (
                  <option key={g.group_id} value={g.group_id}>
                    {g.group_id}
                  </option>
                ))}
              </select>
              <select
                className="rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 py-2 text-sm outline-none focus:ring-1 focus:ring-[var(--color-ring)]"
                onChange={(e) =>
                  setSelected((s) => ({
                    groupId: s?.groupId ?? groups.data?.groups[0]?.group_id ?? "",
                    partitionId: parseInt(e.target.value, 10),
                  }))
                }
                value={selected?.partitionId ?? ""}
              >
                <option value="">Select partition...</option>
                {topology.data.partitions.map((p) => (
                  <option key={p.partition_id} value={p.partition_id}>
                    {p.partition_id}
                  </option>
                ))}
              </select>
            </div>
          )}
          {selected?.groupId && selected.partitionId !== undefined && (
            <LagResult groupId={selected.groupId} partitionId={selected.partitionId} />
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function LagResult({ groupId, partitionId }: { groupId: string; partitionId: number }) {
  const { data, loading, error, refetch } = useConsumerGroupLag(groupId, partitionId);

  if (loading) return <Loading message="Loading lag..." />;
  if (error) return <ErrorAlert message={error} onRetry={refetch} isAuth={error.startsWith("401")} />;
  if (!data) return <ErrorAlert message="No lag data returned." onRetry={refetch} />;

  const chartData = [
    { name: "Committed", value: data.committed_offset },
    { name: "HWM", value: data.partition_high_watermark },
    { name: "Lag", value: data.lag },
  ];

  return (
    <div className="space-y-6">
      <div className="h-48 w-full">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData}>
            <XAxis dataKey="name" tick={{ fontSize: 12 }} />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip
              contentStyle={{
                backgroundColor: "var(--color-popover)",
                borderColor: "var(--color-border)",
                color: "var(--color-popover-foreground)",
              }}
            />
            <Legend />
            <Bar dataKey="value" name={`${groupId} / partition ${partitionId}`} fill="var(--color-primary)" />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <Stat label="Committed Offset" value={data.committed_offset} />
        <Stat label="Partition HWM" value={data.partition_high_watermark} />
        <Stat label="Lag" value={data.lag} />
      </div>
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
