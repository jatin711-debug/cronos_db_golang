import { useState } from "react";
import { Link, useLocation } from "react-router-dom";
import { Menu, X } from "lucide-react";
import { cn } from "@/lib/utils";

const NAV = [
  { path: "/dashboard", label: "Dashboard" },
  { path: "/cluster", label: "Cluster" },
  { path: "/partitions", label: "Partitions" },
  { path: "/replication", label: "Replication" },
  { path: "/consumers", label: "Consumers" },
  { path: "/operations", label: "Operations" },
];

export function MobileNav() {
  const [open, setOpen] = useState(false);
  const location = useLocation();

  return (
    <div className="md:hidden">
      <button
        onClick={() => setOpen((o) => !o)}
        className="rounded-md p-2 text-[var(--color-muted-foreground)] hover:bg-[var(--color-accent)]"
        aria-label="Toggle menu"
      >
        {open ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
      </button>
      {open && (
        <div className="absolute left-0 right-0 top-[65px] z-50 border-b border-[var(--color-border)] bg-[var(--color-background)] px-6 py-4 shadow-md">
          <nav className="flex flex-col gap-2">
            {NAV.map((item) => {
              const active = location.pathname === item.path;
              return (
                <Link
                  key={item.path}
                  to={item.path}
                  onClick={() => setOpen(false)}
                  className={cn(
                    "rounded-md px-3 py-2 text-sm transition-colors",
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
        </div>
      )}
    </div>
  );
}
