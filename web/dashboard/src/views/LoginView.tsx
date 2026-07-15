import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { setAdminToken } from "@/lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

export function LoginView() {
  const [token, setToken] = useState("");
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    const trimmed = token.trim();
    if (!trimmed) {
      setError("Please paste a JWT token.");
      return;
    }
    setAdminToken(trimmed);
    navigate("/dashboard", { replace: true });
  };

  return (
    <div className="flex min-h-[60vh] items-center justify-center">
      <Card className="w-full max-w-md">
        <CardHeader>
          <CardTitle>Admin Sign In</CardTitle>
          <CardDescription>
            Paste your admin JWT to access the dashboard.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            <textarea
              value={token}
              onChange={(e) => setToken(e.target.value)}
              placeholder="eyJhbGciOi..."
              rows={5}
              className="w-full rounded-md border border-[var(--color-border)] bg-[var(--color-background)] px-3 py-2 text-sm text-[var(--color-foreground)] outline-none focus:ring-1 focus:ring-[var(--color-ring)]"
            />
            {error && (
              <p className="text-sm text-[var(--color-destructive)]">{error}</p>
            )}
            <Button type="submit" className="w-full">Sign In</Button>
            <p className="text-xs text-[var(--color-muted-foreground)]">
              In dev mode the server allows anonymous requests, but you can still sign in with a token.
            </p>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
