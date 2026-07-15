import { useState } from "react";
import { useTenantUsage } from "@/lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Loading } from "@/components/Loading";
import { ErrorAlert } from "@/components/ErrorAlert";
import { Button } from "@/components/ui/button";

export function TenantUsageView() {
  const [tenantId, setTenantId] = useState("");
  const { data, loading, error, refetch, updatedAt } = useTenantUsage(tenantId || undefined);

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Tenant Usage</CardTitle>
          <CardDescription>
            Inspect per-tenant resource usage. Leave empty to aggregate all tenants.
            {updatedAt && (
              <span className="ml-2 text-[var(--color-muted-foreground)]">
                Refreshed {Math.round((Date.now() - updatedAt) / 1000)}s ago
              </span>
            )}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex gap-2">
            <input
              type="text"
              value={tenantId}
              onChange={(e) => setTenantId(e.target.value)}
              placeholder="tenant-id"
              className="rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 py-2 text-sm text-[var(--color-foreground)] outline-none focus:ring-1 focus:ring-[var(--color-ring)]"
            />
            <Button onClick={() => refetch()} disabled={loading}>
              {loading ? "Loading..." : "Refresh"}
            </Button>
          </div>

          {loading && <Loading message="Loading tenant usage..." />}
          {error && <ErrorAlert message={error} onRetry={refetch} isAuth={error.startsWith("401")} />}
          {data && (
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
              <Stat label="Tenant" value={data.tenant_id || "(all)"} />
              <Stat label="In Flight" value={String(data.in_flight)} />
              <Stat label="Storage Bytes" value={String(data.storage_bytes)} />
              <Stat label="Limits Configured" value={data.limits_configured ? "yes" : "no"} />
              {data.limits_configured && (
                <>
                  <Stat label="Publish Rate / sec" value={String(data.publish_rate_per_sec)} />
                  <Stat label="Max In Flight" value={String(data.max_in_flight)} />
                </>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-lg font-semibold">{value}</div>
      <div className="text-xs text-[var(--color-muted-foreground)]">{label}</div>
    </div>
  );
}
