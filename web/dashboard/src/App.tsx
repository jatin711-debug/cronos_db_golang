import { Link, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { cn } from "@/lib/utils";
import { TokenInput } from "@/components/TokenInput";
import { DashboardHome } from "@/views/DashboardHome";
import { ClusterTopologyView } from "@/views/ClusterTopologyView";
import { PartitionHealthView } from "@/views/PartitionHealthView";
import { ReplicationLagView } from "@/views/ReplicationLagView";
import { ConsumerGroupsView } from "@/views/ConsumerGroupsView";
import { OperationsView } from "@/views/OperationsView";

const NAV = [
  { path: "/dashboard", label: "Dashboard" },
  { path: "/cluster", label: "Cluster" },
  { path: "/partitions", label: "Partitions" },
  { path: "/replication", label: "Replication" },
  { path: "/consumers", label: "Consumers" },
  { path: "/operations", label: "Operations" },
];

export default function App() {
  const location = useLocation();
  return (
    <div className="min-h-screen bg-[var(--color-background)] text-[var(--color-foreground)]">
      <header className="border-b border-[var(--color-border)]">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-4">
          <div className="flex items-center gap-3">
            <span className="text-xl font-semibold">⏰ CronosDB Admin</span>
            <span className="text-xs text-[var(--color-muted-foreground)]">v0.2</span>
          </div>
          <nav className="hidden items-center gap-1 md:flex">
            {NAV.map((item) => {
              const active = location.pathname === item.path;
              return (
                <Link
                  key={item.path}
                  to={item.path}
                  className={cn(
                    "rounded-md px-3 py-1.5 text-sm transition-colors",
                    active
                      ? "bg-[var(--color-primary)] text-[var(--color-primary-foreground)]"
                      : "text-[var(--color-muted-foreground)] hover:bg-[var(--color-accent)]",
                  )}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
          <TokenInput />
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-6 py-8">
        <Routes>
          <Route path="/dashboard" element={<DashboardHome />} />
          <Route path="/cluster" element={<ClusterTopologyView />} />
          <Route path="/partitions" element={<PartitionHealthView />} />
          <Route path="/replication" element={<ReplicationLagView />} />
          <Route path="/consumers" element={<ConsumerGroupsView />} />
          <Route path="/operations" element={<OperationsView />} />
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="*" element={<Navigate to="/dashboard" replace />} />
        </Routes>
      </main>
    </div>
  );
}
