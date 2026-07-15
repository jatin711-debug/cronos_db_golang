import { useState } from "react";
import { DashboardHome } from "@/views/DashboardHome";
import { ClusterTopologyView } from "@/views/ClusterTopologyView";
import { cn } from "@/lib/utils";

type View = "dashboard" | "cluster";

const TABS: { id: View; label: string }[] = [
  { id: "dashboard", label: "Dashboard" },
  { id: "cluster", label: "Cluster" },
];

export default function App() {
  const [view, setView] = useState<View>("dashboard");
  return (
    <div className="min-h-screen bg-[var(--color-background)] text-[var(--color-foreground)]">
      <header className="border-b border-[var(--color-border)]">
        <div className="mx-auto flex max-w-5xl items-center justify-between px-6 py-4">
          <div className="flex items-center gap-3">
            <span className="text-xl font-semibold">⏰ CronosDB Admin</span>
            <span className="text-xs text-[var(--color-muted-foreground)]">step 4 scaffold</span>
          </div>
          <nav className="flex gap-2">
            {TABS.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setView(tab.id)}
                className={cn(
                  "rounded-md px-3 py-1.5 text-sm transition-colors",
                  view === tab.id
                    ? "bg-[var(--color-primary)] text-[var(--color-primary-foreground)]"
                    : "text-[var(--color-muted-foreground)] hover:bg-[var(--color-accent)]",
                )}
              >
                {tab.label}
              </button>
            ))}
          </nav>
        </div>
      </header>
      <main className="mx-auto max-w-5xl px-6 py-8">
        {view === "dashboard" ? <DashboardHome /> : <ClusterTopologyView />}
      </main>
    </div>
  );
}
