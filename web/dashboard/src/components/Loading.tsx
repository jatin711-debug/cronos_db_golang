import { Loader2 } from "lucide-react";

export function Loading({ message = "Loading..." }: { message?: string }) {
  return (
    <div className="flex items-center justify-center py-12 text-[var(--color-muted-foreground)]">
      <Loader2 className="mr-2 h-5 w-5 animate-spin" />
      <span className="text-sm">{message}</span>
    </div>
  );
}
