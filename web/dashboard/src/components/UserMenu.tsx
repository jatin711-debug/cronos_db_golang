import { useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import { User, LogOut, KeyRound } from "lucide-react";
import { getAdminToken, setAdminToken } from "@/lib/api";

export function UserMenu() {
  const [open, setOpen] = useState(false);
  const token = getAdminToken();
  const navigate = useNavigate();

  const handleLogout = () => {
    setAdminToken(null);
    setOpen(false);
    navigate("/login", { replace: true });
  };

  return (
    <div className="relative flex items-center">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-1.5 rounded-md px-2 py-1 text-xs text-[var(--color-muted-foreground)] hover:bg-[var(--color-accent)]"
        title={token ? "Admin user" : "Not signed in"}
      >
        <User className="h-3.5 w-3.5" />
        <span className="hidden sm:inline">{token ? "Admin" : "Sign in"}</span>
      </button>
      {open && (
        <div className="absolute right-0 top-8 z-50 w-48 rounded-lg border border-[var(--color-border)] bg-[var(--color-popover)] p-1 shadow-md">
          {token ? (
            <button
              onClick={handleLogout}
              className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-sm text-[var(--color-popover-foreground)] hover:bg-[var(--color-accent)]"
            >
              <LogOut className="h-4 w-4" />
              Sign out
            </button>
          ) : (
            <Link
              to="/login"
              onClick={() => setOpen(false)}
              className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-sm text-[var(--color-popover-foreground)] hover:bg-[var(--color-accent)]"
            >
              <KeyRound className="h-4 w-4" />
              Sign in
            </Link>
          )}
        </div>
      )}
    </div>
  );
}
