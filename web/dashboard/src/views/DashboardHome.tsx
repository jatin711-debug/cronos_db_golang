import { useAdmin } from "@/lib/api-mock";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

export function DashboardHome() {
  const admin = useAdmin();
  const mode = admin.is_cluster_mode ? "cluster" : "standalone";
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>CronosDB Admin</CardTitle>
          <CardDescription>
            Connected to <span className="font-mono">{admin.local_node_id}</span> in{" "}
            <Badge variant={admin.is_cluster_mode ? "default" : "secondary"}>{mode}</Badge> mode.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-[var(--color-muted-foreground)]">
            Step 4 scaffold. The dashboard shell renders mock data today; step 5 will wire it
            to the AdminService JSON proxy at <code>/api/admin/*</code>.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
