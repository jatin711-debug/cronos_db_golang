// Mock data layer for step 4. The TS types here mirror the Go
// proto-generated types in pkg/types/admin.pb.go so step 5 can swap
// the body of useAdmin() to a real fetch('/api/admin/topology') call
// without touching any of the views.
//
// Once step 5 lands, this file is replaced by `lib/api.ts` that
// exports the same useAdmin() hook backed by fetch. Components
// continue to import useAdmin from '@/lib/api'.

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

// Mock data shaped like a real ClusterTopology response. Mirrors the
// standalone-mode fallback from AdminServiceHandler.GetClusterTopology.
const MOCK_TOPOLOGY: ClusterTopology = {
  local_node_id: "demo-node",
  is_cluster_mode: false,
  total_nodes: 1,
  alive_nodes: 1,
  total_partitions: 0,
  leader_partitions: 0,
  nodes: [
    {
      node_id: "demo-node",
      address: "",
      is_local: true,
      is_alive: true,
      state: "alive",
      rack: "",
      zone: "",
      region: "",
    },
  ],
  partitions: [],
  captured_at_unix_ms: Date.now(),
};

// useAdmin returns the current AdminService snapshot.
//
// Step 5 will replace this body with a real fetch call:
//   const [data, setData] = useState<ClusterTopology | null>(null);
//   useEffect(() => {
//     fetch("/api/admin/topology", { credentials: "include" })
//       .then(r => r.json()).then(setData);
//   }, []);
//   return data;
//
// For step 4 the hook returns the mock synchronously so views render
// immediately.
export function useAdmin(): ClusterTopology {
  return MOCK_TOPOLOGY;
}
