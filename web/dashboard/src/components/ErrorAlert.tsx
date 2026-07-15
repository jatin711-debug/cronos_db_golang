import { AlertCircle } from "lucide-react";

export function ErrorAlert({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div className="rounded-lg border border-[var(--color-destructive)]/30 bg-[var(--color-destructive)]/10 p-4">
      <div className="flex items-start gap-3">
        <AlertCircle className="mt-0.5 h-5 w-5 text-[var(--color-destructive)]" />
        <div className="flex-1">
          <p className="text-sm font-medium text-[var(--color-destructive)]">Error</p>
          <p className="text-sm text-[var(--color-destructive-foreground)]">{message}</p>
        </div>
        {onRetry && (
          <button
            onClick={onRetry}
            className="rounded-md border border-[var(--color-destructive)]/30 px-3 py-1.5 text-xs font-medium text-[var(--color-destructive)] hover:bg-[var(--color-destructive)]/20"
          >
            Retry
          </button>
        )}
      </div>
    </div>
  );
}
