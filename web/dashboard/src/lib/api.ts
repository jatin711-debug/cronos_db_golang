// Real API client for the CronosDB AdminService JSON proxy.
//
// The TypeScript interfaces below mirror the proto-generated JSON shapes in
// pkg/types/admin.pb.go (snake_case, omitempty). The hooks call the
// /api/admin/* endpoints served by internal/api/web_handler.go.
//
// Auth: in dev mode (--dev) the Go proxy allows anonymous requests. In
// production the user can paste a JWT into the header token input; it is
// stored in localStorage as 'cronos_admin_token' and sent as
// Authorization: Bearer <token> on every request.

import { useCallback, useEffect, useState } from "react";

const STORAGE_KEY = "cronos_admin_token";

export function getAdminToken(): string | null {
  try {
    return localStorage.getItem(STORAGE_KEY);
  } catch {
    return null;
  }
}

export function setAdminToken(token: string | null): void {
  try {
    if (token === null || token === "") {
      localStorage.removeItem(STORAGE_KEY);
    } else {
      localStorage.setItem(STORAGE_KEY, token);
    }
  } catch {
    // Ignore storage errors (e.g. private mode).
  }
}

export async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getAdminToken();
  const headers: Record<string, string> = {
    Accept: "application/json",
    ...(init.headers as Record<string, string>),
  };
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const resp = await fetch(path, {
    ...init,
    credentials: "include",
    headers,
  });

  if (!resp.ok) {
    let detail = resp.statusText;
    try {
      const body = await resp.json();
      detail = body.error || JSON.stringify(body);
    } catch {
      // Fall back to statusText.
    }
    throw new Error(`${resp.status} ${detail}`);
  }

  // Some POST responses may have a body; others may be empty. Treat empty as
  // null and let the caller handle it.
  const text = await resp.text();
  if (text === "") {
    return null as unknown as T;
  }
  return JSON.parse(text) as T;
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface AdminNode {
  node_id: string;
  address: string;
  is_local: boolean;
  is_alive: boolean;
  state: string;
  rack: string;
  zone: string;
  region: string;
}

export interface AdminPartition {
  partition_id: number;
  leader_id: string;
  replicas: string[];
  isr: string[];
  epoch: number;
  leader_high_watermark: number;
  follower_offsets: Record<string, number>;
  in_sync_count: number;
}

export interface ClusterTopology {
  local_node_id: string;
  is_cluster_mode: boolean;
  total_nodes: number;
  alive_nodes: number;
  total_partitions: number;
  leader_partitions: number;
  nodes: AdminNode[];
  partitions: AdminPartition[];
  captured_at_unix_ms: number;
}

export interface PartitionHealthResponse {
  partition_id: number;
  high_watermark: number;
  first_offset: number;
  segment_count: number;
  disk_usage_bytes: number;
  active_timers: number;
  ready_events: number;
  cold_store_entries: number;
  replay_error: string;
  last_error: string;
}

export interface FollowerLag {
  follower_id: string;
  offset: number;
  lag_events: number;
  lag_seconds: number;
  in_sync: boolean;
}

export interface ReplicationLagResponse {
  partition_id: number;
  leader_high_watermark: number;
  followers: FollowerLag[];
}

export interface AdminConsumerGroupSummary {
  group_id: string;
  topic: string;
  partitions: number[];
  member_count: number;
}

export interface AdminListConsumerGroupsResponse {
  groups: AdminConsumerGroupSummary[];
}

export interface AdminConsumerGroupLagResponse {
  group_id: string;
  partition_id: number;
  committed_offset: number;
  partition_high_watermark: number;
  lag: number;
}

export interface RunRetentionResponse {
  success: boolean;
  error: string;
  events_removed: number;
}

export interface RunCompactionResponse {
  success: boolean;
  error: string;
  segments_compacted: number;
}

export interface RebalanceResponse {
  success: boolean;
  error: string;
  partitions_moved: number;
}

export interface SchemaSummary {
  topic: string;
  latest_version: number;
  compatibility_mode: string;
}

export interface ListSchemasResponse {
  schemas: SchemaSummary[];
}

export interface GetSchemaResponse {
  topic: string;
  version: number;
  schema_type: string;
  definition: string;
}

export interface TenantUsageResponse {
  tenant_id: string;
  in_flight: number;
  storage_bytes: number;
  limits_configured: boolean;
  publish_rate_per_sec: number;
  max_in_flight: number;
}

// ---------------------------------------------------------------------------
// State shape used by all query hooks
// ---------------------------------------------------------------------------

export interface QueryState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

function useQuery<T>(fetcher: () => Promise<T>, deps: unknown[] = []): QueryState<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);

  const refetch = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetcher()
      .then((value) => {
        if (!cancelled) {
          setData(value);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err));
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tick, ...deps]);

  return { data, loading, error, refetch };
}

// ---------------------------------------------------------------------------
// Query hooks
// ---------------------------------------------------------------------------

export function useClusterTopology(): QueryState<ClusterTopology> {
  return useQuery(() => apiFetch<ClusterTopology>("/api/admin/topology"));
}

export function usePartitionHealth(partitionId: number): QueryState<PartitionHealthResponse> {
  return useQuery(
    () => apiFetch<PartitionHealthResponse>(`/api/admin/partition-health?partition_id=${partitionId}`),
    [partitionId],
  );
}

export function useReplicationLag(partitionId?: number): QueryState<ReplicationLagResponse> {
  const qs = partitionId === undefined ? "" : `?partition_id=${partitionId}`;
  return useQuery(() => apiFetch<ReplicationLagResponse>(`/api/admin/replication-lag${qs}`), [partitionId]);
}

export function useConsumerGroups(): QueryState<AdminListConsumerGroupsResponse> {
  return useQuery(() => apiFetch<AdminListConsumerGroupsResponse>("/api/admin/consumer-groups"));
}

export function useConsumerGroupLag(
  groupId: string,
  partitionId: number,
): QueryState<AdminConsumerGroupLagResponse> {
  return useQuery(
    () =>
      apiFetch<AdminConsumerGroupLagResponse>(
        `/api/admin/consumer-group-lag?group_id=${encodeURIComponent(groupId)}&partition_id=${partitionId}`,
      ),
    [groupId, partitionId],
  );
}

export function useListSchemas(): QueryState<ListSchemasResponse> {
  return useQuery(() => apiFetch<ListSchemasResponse>("/api/admin/schemas"));
}

export function useGetSchema(topic: string, version?: number): QueryState<GetSchemaResponse> {
  const qs = version === undefined ? `?topic=${encodeURIComponent(topic)}` : `?topic=${encodeURIComponent(topic)}&version=${version}`;
  return useQuery(() => apiFetch<GetSchemaResponse>(`/api/admin/schema${qs}`), [topic, version]);
}

export function useTenantUsage(tenantId?: string): QueryState<TenantUsageResponse> {
  const qs = tenantId ? `?tenant_id=${encodeURIComponent(tenantId)}` : "";
  return useQuery(() => apiFetch<TenantUsageResponse>(`/api/admin/tenant-usage${qs}`), [tenantId]);
}

// ---------------------------------------------------------------------------
// Mutator hooks
// ---------------------------------------------------------------------------

export interface MutatorState<T, P> {
  data: T | null;
  loading: boolean;
  error: string | null;
  mutate: (params: P) => Promise<void>;
  reset: () => void;
}

function useMutation<T, P>(execute: (params: P) => Promise<T>): MutatorState<T, P> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const mutate = useCallback(
    async (params: P) => {
      setLoading(true);
      setError(null);
      try {
        const result = await execute(params);
        setData(result);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setLoading(false);
      }
    },
    [execute],
  );

  const reset = useCallback(() => {
    setData(null);
    setError(null);
    setLoading(false);
  }, []);

  return { data, loading, error, mutate, reset };
}

export interface RunRetentionParams {
  partition_id?: number;
  max_age_hours?: number;
  max_size_bytes?: number;
}

export function useRunRetention(): MutatorState<RunRetentionResponse, RunRetentionParams> {
  return useMutation((params) => {
    const qs = new URLSearchParams();
    if (params.partition_id !== undefined) qs.set("partition_id", String(params.partition_id));
    if (params.max_age_hours !== undefined) qs.set("max_age_hours", String(params.max_age_hours));
    if (params.max_size_bytes !== undefined) qs.set("max_size_bytes", String(params.max_size_bytes));
    return apiFetch<RunRetentionResponse>(`/api/admin/retention/run?${qs.toString()}`, { method: "POST" });
  });
}

export function useRunCompaction(): MutatorState<RunCompactionResponse, { partition_id: number }> {
  return useMutation((params) =>
    apiFetch<RunCompactionResponse>(`/api/admin/compaction/run?partition_id=${params.partition_id}`, {
      method: "POST",
    }),
  );
}

export function useTriggerRebalance(): MutatorState<RebalanceResponse, void> {
  return useMutation(() => apiFetch<RebalanceResponse>("/api/admin/cluster/rebalance", { method: "POST" }));
}
