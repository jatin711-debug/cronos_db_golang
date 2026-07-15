import { describe, it, expect, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import { ClusterTopologyView } from "@/views/ClusterTopologyView";
import * as api from "@/lib/api";

const MOCK_TOPOLOGY: api.ClusterTopology = {
  local_node_id: "test-node",
  is_cluster_mode: true,
  total_nodes: 3,
  alive_nodes: 2,
  total_partitions: 5,
  leader_partitions: 3,
  nodes: [
    {
      node_id: "node-a",
      address: "127.0.0.1:9000",
      is_local: true,
      is_alive: true,
      state: "alive",
      rack: "r1",
      zone: "z1",
      region: "us-east",
    },
  ],
  partitions: [
    {
      partition_id: 1,
      leader_id: "node-a",
      replicas: ["node-a", "node-b"],
      isr: ["node-a"],
      epoch: 1,
      leader_high_watermark: 42,
      follower_offsets: { "node-b": 30 },
      in_sync_count: 1,
    },
  ],
  captured_at_unix_ms: 1_700_000_000_000,
};

describe("ClusterTopologyView", () => {
  it("renders topology stats and tables", () => {
    vi.spyOn(api, "useClusterTopology").mockReturnValue({
      data: MOCK_TOPOLOGY,
      loading: false,
      error: null,
      refetch: vi.fn(),
    });

    render(<ClusterTopologyView />);

    expect(screen.getByText("Cluster Topology")).toBeInTheDocument();

    // Stats: assert labels anchor the numbers so duplicates don't collide.
    const totalNodes = screen.getByText("Total Nodes").parentElement as HTMLElement;
    expect(within(totalNodes).getByText("3")).toBeInTheDocument();

    const leaderParts = screen.getByText("Leader Partitions").parentElement as HTMLElement;
    expect(within(leaderParts).getByText("3")).toBeInTheDocument();

    // Nodes table
    const nodesTable = screen.getByText("Node ID").closest("table") as HTMLElement;
    expect(within(nodesTable).getByText("node-a")).toBeInTheDocument();
    expect(within(nodesTable).getByText("127.0.0.1:9000")).toBeInTheDocument();

    // Partitions table
    const partitionsTable = screen.getByText("Leader").closest("table") as HTMLElement;
    expect(within(partitionsTable).getByText("42")).toBeInTheDocument();
  });

  it("shows loading state", () => {
    vi.spyOn(api, "useClusterTopology").mockReturnValue({
      data: null,
      loading: true,
      error: null,
      refetch: vi.fn(),
    });

    render(<ClusterTopologyView />);
    expect(screen.getByText("Loading cluster topology...")).toBeInTheDocument();
  });

  it("shows error state", () => {
    vi.spyOn(api, "useClusterTopology").mockReturnValue({
      data: null,
      loading: false,
      error: "network error",
      refetch: vi.fn(),
    });

    render(<ClusterTopologyView />);
    expect(screen.getByText("network error")).toBeInTheDocument();
  });
});
