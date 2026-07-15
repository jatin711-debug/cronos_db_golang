import { lazy, Suspense } from "react";
import { Link, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { cn } from "@/lib/utils";
import { ThemeToggle } from "@/components/ThemeToggle";
import { UserMenu } from "@/components/UserMenu";
import { MobileNav } from "@/components/MobileNav";
import { ErrorBoundary } from "@/components/ErrorBoundary";

const DashboardHome = lazy(() => import("@/views/DashboardHome").then((m) => ({ default: m.DashboardHome })));
const ClusterTopologyView = lazy(() => import("@/views/ClusterTopologyView").then((m) => ({ default: m.ClusterTopologyView })));
const PartitionHealthView = lazy(() => import("@/views/PartitionHealthView").then((m) => ({ default: m.PartitionHealthView })));
const ReplicationLagView = lazy(() => import("@/views/ReplicationLagView").then((m) => ({ default: m.ReplicationLagView })));
const ConsumerGroupsView = lazy(() => import("@/views/ConsumerGroupsView").then((m) => ({ default: m.ConsumerGroupsView })));
const OperationsView = lazy(() => import("@/views/OperationsView").then((m) => ({ default: m.OperationsView })));
const SchemasView = lazy(() => import("@/views/SchemasView").then((m) => ({ default: m.SchemasView })));
const TenantUsageView = lazy(() => import("@/views/TenantUsageView").then((m) => ({ default: m.TenantUsageView })));
const LoginView = lazy(() => import("@/views/LoginView").then((m) => ({ default: m.LoginView })));

export const NAV = [
  { path: "/dashboard", label: "Dashboard" },
  { path: "/cluster", label: "Cluster" },
  { path: "/partitions", label: "Partitions" },
  { path: "/replication", label: "Replication" },
  { path: "/consumers", label: "Consumers" },
  { path: "/schemas", label: "Schemas" },
  { path: "/tenants", label: "Tenants" },
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
          <nav className="hidden items-center gap-1 lg:flex">
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

function RouteSkeleton() {
  return (
    <div className="flex h-64 items-center justify-center text-sm text-muted-foreground">
      Loading view…
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
              <Suspense fallback={<RouteSkeleton />}>
                <Routes>
                  <Route path="/dashboard" element={<DashboardHome />} />
                  <Route path="/cluster" element={<ClusterTopologyView />} />
                  <Route path="/partitions" element={<PartitionHealthView />} />
                  <Route path="/replication" element={<ReplicationLagView />} />
                  <Route path="/consumers" element={<ConsumerGroupsView />} />
                  <Route path="/schemas" element={<SchemasView />} />
                  <Route path="/tenants" element={<TenantUsageView />} />
                  <Route path="/operations" element={<OperationsView />} />
                  <Route path="/" element={<Navigate to="/dashboard" replace />} />
                  <Route path="*" element={<Navigate to="/dashboard" replace />} />
                </Routes>
              </Suspense>
            </ErrorBoundary>
          </Layout>
        }
      />
    </Routes>
  );
}
