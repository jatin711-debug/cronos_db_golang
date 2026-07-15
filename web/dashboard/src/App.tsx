import { Link, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { cn } from "@/lib/utils";
import { ThemeToggle } from "@/components/ThemeToggle";
import { UserMenu } from "@/components/UserMenu";
import { MobileNav } from "@/components/MobileNav";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { DashboardHome } from "@/views/DashboardHome";
import { ClusterTopologyView } from "@/views/ClusterTopologyView";
import { PartitionHealthView } from "@/views/PartitionHealthView";
import { ReplicationLagView } from "@/views/ReplicationLagView";
import { ConsumerGroupsView } from "@/views/ConsumerGroupsView";
import { OperationsView } from "@/views/OperationsView";
import { LoginView } from "@/views/LoginView";

const NAV = [
  { path: "/dashboard", label: "Dashboard" },
  { path: "/cluster", label: "Cluster" },
  { path: "/partitions", label: "Partitions" },
  { path: "/replication", label: "Replication" },
  { path: "/consumers", label: "Consumers" },
  { path: "/operations", label: "Operations" },
];

function Layout({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  return (
    <div className="min-h-screen bg-(--color-background) text-(--color-foreground)">
      <header className="border-b border-(--color-border)">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-4">
          <div className="flex items-center gap-3">
            <span className="text-xl font-semibold">⏰ CronosDB Admin</span>
            <span className="text-xs text-muted-foreground">v0.2</span>
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
                      ? "bg-(--color-primary) text-(--color-primary-foreground)"
                      : "text-muted-foreground hover:bg-(--color-accent)",
                  )}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
          <div className="flex items-center gap-2">
            <ThemeToggle />
            <UserMenu />
            <MobileNav />
          </div>
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-6 py-8">{children}</main>
    </div>
  );
}

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginView />} />
      <Route
        path="/*"
        element={
          <Layout>
            <ErrorBoundary>
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
            </ErrorBoundary>
          </Layout>
        }
      />
    </Routes>
  );
}
