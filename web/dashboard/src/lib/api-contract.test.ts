import { describe, it, expect, vi, afterEach } from "vitest";
import { apiFetch, normalizeAdminResponse } from "./api";

afterEach(() => vi.restoreAllMocks());
describe("actual Go JSON defaults", () => {
  it.each([ ["topology", "partitions"], ["consumer-groups", "groups"], ["schemas", "schemas"], ["replication-lag", "followers"] ])("expands empty %s lists", (route, field) => {
    expect(normalizeAdminResponse(`/api/admin/${route}`, {})).toHaveProperty(field, []);
  });
  it("keeps partition zero and unknown leader reporting distinct", () => {
    expect(normalizeAdminResponse("/api/admin/topology", {partitions:[{}]})).toMatchObject({total_partitions:0, partitions:[{partition_id:0,replicas:[],isr:[]}]});
  });
  it("shows zero counters and zero-operation results", () => {
    expect(normalizeAdminResponse("/api/admin/tenant-usage", {})).toMatchObject({in_flight:0, storage_bytes:0});
    expect(normalizeAdminResponse("/api/admin/retention/run?partition_id=0", {success:true})).toMatchObject({events_removed:0});
  });
  it("makes unauthorized responses recognizable by every view", async () => {
    vi.spyOn(globalThis,"fetch").mockResolvedValue(new Response("missing token",{status:401,statusText:"Unauthorized"}));
    await expect(apiFetch("/api/admin/topology")).rejects.toThrow("401");
  });
  it("preserves permission denial separately from authentication failure", async () => {
    vi.spyOn(globalThis,"fetch").mockResolvedValue(new Response(JSON.stringify({error:"admin access required"}),{status:403,statusText:"Forbidden"}));
    await expect(apiFetch("/api/admin/topology")).rejects.toThrow("403 admin access required");
  });
});
