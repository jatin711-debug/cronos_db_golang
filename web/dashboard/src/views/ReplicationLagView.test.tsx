import { describe, it, expect, vi, afterEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { ReplicationLagView } from "@/views/ReplicationLagView";
import * as api from "@/lib/api";

const idle = { loading: false, error: null, refetch: () => {}, updatedAt: 0 };

afterEach(() => vi.restoreAllMocks());

describe("ReplicationLagView", () => {
  it("titles partition 0 as partition 0, not as all partitions", () => {
    vi.spyOn(api, "useClusterTopology").mockReturnValue({
      ...idle,
      data: { partitions: [{}, { partition_id: 1 }] },
    } as never);
    const lag = vi.spyOn(api, "useReplicationLag").mockReturnValue({
      ...idle,
      // The server leaves partition_id out of its JSON when it is 0.
      data: { leader_high_watermark: 5, followers: [] },
    } as never);

    render(<ReplicationLagView />);
    expect(screen.getByText("All Partitions")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "Partition 0" }));
    expect(lag).toHaveBeenLastCalledWith(0);
    expect(screen.getByText("Partition 0", { selector: "h3, [class*=tracking-tight]" })).toBeTruthy();
    expect(screen.queryByText("All Partitions")).toBeNull();
  });
});
