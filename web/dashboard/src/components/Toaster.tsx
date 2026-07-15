import { X, CheckCircle2, AlertCircle } from "lucide-react";
import { useToast, type Toast } from "@/components/ToastProvider";

const VARIANTS: Record<Toast["variant"], { border: string; bg: string; icon: typeof CheckCircle2 }> = {
  default: {
    border: "border-[var(--color-border)]",
    bg: "bg-[var(--color-popover)]",
    icon: AlertCircle,
  },
  success: {
    border: "border-emerald-500/30",
    bg: "bg-emerald-500/10",
    icon: CheckCircle2,
  },
  error: {
    border: "border-[var(--color-destructive)]/30",
    bg: "bg-[var(--color-destructive)]/10",
    icon: AlertCircle,
  },
};

export function Toaster() {
  const { toasts, removeToast } = useToast();
  if (toasts.length === 0) return null;

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2">
      {toasts.map((toast) => {
        const style = VARIANTS[toast.variant];
        const Icon = style.icon;
        return (
          <div
            key={toast.id}
            className={`w-80 rounded-lg border ${style.border} ${style.bg} p-4 shadow-md`}
          >
            <div className="flex items-start gap-3">
              <Icon className="mt-0.5 h-5 w-5 shrink-0" />
              <div className="flex-1">
                <p className="text-sm font-medium">{toast.title}</p>
                {toast.description && (
                  <p className="text-xs text-[var(--color-muted-foreground)]">{toast.description}</p>
                )}
              </div>
              <button
                onClick={() => removeToast(toast.id)}
                className="text-[var(--color-muted-foreground)] hover:text-[var(--color-foreground)]"
                aria-label="Dismiss"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          </div>
        );
      })}
    </div>
  );
}
