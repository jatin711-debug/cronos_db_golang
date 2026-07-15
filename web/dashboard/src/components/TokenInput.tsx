import { useState } from "react";
import { KeyRound } from "lucide-react";
import { getAdminToken, setAdminToken } from "@/lib/api";

export function TokenInput() {
  const [value, setValue] = useState(getAdminToken() || "");
  const [open, setOpen] = useState(false);

  const handleSave = () => {
    setAdminToken(value.trim() || null);
    setOpen(false);
  };

  const handleClear = () => {
    setValue("");
    setAdminToken(null);
    setOpen(false);
  };

  return (
    <div className="relative flex items-center">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-1.5 rounded-md px-2 py-1 text-xs text-[var(--color-muted-foreground)] hover:bg-[var(--color-accent)]"
        title="Admin JWT token"
      >
        <KeyRound className="h-3.5 w-3.5" />
        {getAdminToken() ? "Token set" : "No token"}
      </button>
      {open && (
        <div className="absolute right-0 top-8 z-50 w-72 rounded-lg border border-[var(--color-border)] bg-[var(--color-popover)] p-3 shadow-md">
          <p className="mb-2 text-xs text-[var(--color-muted-foreground)]">
            Paste an admin JWT to use when the API requires auth. Stored locally in
            this browser.
          </p>
          <textarea
            value={value}
            onChange={(e) => setValue(e.target.value)}
            placeholder="eyJhbGciOi..."
            className="mb-2 w-full rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-2 py-1.5 text-xs text-[var(--color-foreground)] outline-none focus:ring-1 focus:ring-[var(--color-ring)]"
            rows={3}
          />
          <div className="flex justify-end gap-2">
            <button
              onClick={handleClear}
              className="rounded-md px-2 py-1 text-xs text-[var(--color-muted-foreground)] hover:bg-[var(--color-accent)]"
            >
              Clear
            </button>
            <button
              onClick={handleSave}
              className="rounded-md bg-[var(--color-primary)] px-2 py-1 text-xs text-[var(--color-primary-foreground)] hover:opacity-90"
            >
              Save
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
