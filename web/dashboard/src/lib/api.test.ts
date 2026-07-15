import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  apiFetch,
  getAdminToken,
  setAdminToken,
  useClusterTopology,
  usePartitionHealth,
  useRunCompaction,
} from "@/lib/api";
import { renderHook, waitFor, act } from "@testing-library/react";

describe("token storage", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("stores and retrieves the admin token", () => {
    expect(getAdminToken()).toBeNull();
    setAdminToken("my-token");
    expect(getAdminToken()).toBe("my-token");
  });

  it("clears the token when set to empty", () => {
    setAdminToken("my-token");
    setAdminToken("");
    expect(getAdminToken()).toBeNull();
  });
});

describe("apiFetch", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.clear();
  });

  it("fetches json and attaches bearer token when present", async () => {
    setAdminToken("abc123");
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(JSON.stringify({ ok: true }), { status: 200, headers: { "Content-Type": "application/json" } }),
    );

    const result = await apiFetch("/api/admin/topology");
    expect(result).toEqual({ ok: true });
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/admin/topology",
      expect.objectContaining({
        credentials: "include",
        headers: expect.objectContaining({
          Accept: "application/json",
          Authorization: "Bearer abc123",
        }),
      }),
    );
  });

  it("omits authorization header when no token", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(JSON.stringify({ ok: true }), { status: 200, headers: { "Content-Type": "application/json" } }),
    );

    await apiFetch("/api/admin/topology");
    const opts = fetchMock.mock.calls[0][1] as RequestInit;
    expect(opts.headers).not.toHaveProperty("Authorization");
  });

  it("throws with status text on error", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(JSON.stringify({ error: "boom" }), { status: 500, statusText: "Internal Server Error" }),
    );

    await expect(apiFetch("/api/admin/topology")).rejects.toThrow("500 boom");
  });
});

describe("useClusterTopology", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("loads topology and exposes refetch", async () => {
    const topology = {
      local_node_id: "node-1",
      is_cluster_mode: false,
      total_nodes: 1,
      alive_nodes: 1,
      total_partitions: 0,
      leader_partitions: 0,
      nodes: [],
      partitions: [],
      captured_at_unix_ms: Date.now(),
    };

    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(JSON.stringify(topology), { status: 200 }),
    );

    const { result } = renderHook(() => useClusterTopology());
    expect(result.current.loading).toBe(true);

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.data).toEqual(topology);
    expect(result.current.error).toBeNull();
  });
});

describe("usePartitionHealth", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("requests health for a partition", async () => {
    const health = {
      partition_id: 1,
      high_watermark: 100,
      first_offset: 0,
      segment_count: 2,
      disk_usage_bytes: 1024,
      active_timers: 0,
      ready_events: 0,
      cold_store_entries: 0,
      replay_error: "",
      last_error: "",
    };

    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(JSON.stringify(health), { status: 200 }),
    );

    const { result } = renderHook(() => usePartitionHealth(1));
    await waitFor(() => expect(result.current.loading).toBe(false));

    expect(result.current.data).toEqual(health);
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/admin/partition-health?partition_id=1",
      expect.any(Object),
    );
  });
});

describe("useRunCompaction", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("posts compaction request", async () => {
    const response = { success: true, error: "", segments_compacted: 3 };
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(JSON.stringify(response), { status: 200 }),
    );

    const { result } = renderHook(() => useRunCompaction());
    await act(async () => {
      await result.current.mutate({ partition_id: 7 });
    });

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.data).toEqual(response);
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/admin/compaction/run?partition_id=7",
      expect.objectContaining({ method: "POST" }),
    );
  });
});
