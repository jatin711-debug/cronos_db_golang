import { AlertCircle } from "lucide-react";
import { Link } from "react-router-dom";

export function ErrorAlert({
  message,
  onRetry,
  isAuth,
}: {
  message: string;
  onRetry?: () => void;
  isAuth?: boolean;
}) {
  return (
    <div className="rounded-lg border border-[var(--color-destructive)]/30 bg-[var(--color-destructive)]/10 p-4">
      <div className="flex items-start gap-3">
        <AlertCircle className="mt-0.5 h-5 w-5 text-[var(--color-destructive)]" />
        <div className="flex-1">
          <p className="text-sm font-medium text-[var(--color-destructive)]">{isAuth ? "Authentication required" : "Error"}</p>
          <p className="text-sm text-[var(--color-destructive-foreground)]">{message}</p>
          {isAuth && (
            <Link
              to="/login"
              className="mt-2 inline-block text-sm font-medium text-[var(--color-destructive)] underline underline-offset-2"
            >
              Sign in
            </Link>
          )}
        </div>
        {onRetry && !isAuth && (
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
