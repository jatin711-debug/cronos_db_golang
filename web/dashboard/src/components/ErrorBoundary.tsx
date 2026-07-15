import { Component, ErrorInfo, ReactNode } from "react";
import { Button } from "@/components/ui/button";

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // In a production app you would send this to an error tracking service.
    console.error("Dashboard error boundary caught an error:", error, info);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex min-h-[60vh] flex-col items-center justify-center px-6 text-center">
          <h2 className="text-xl font-semibold">Something went wrong</h2>
          <p className="mt-2 max-w-md text-sm text-[var(--color-muted-foreground)]">
            {this.state.error?.message || "An unexpected error occurred in this view."}
          </p>
          <Button
            onClick={() => this.setState({ hasError: false, error: null })}
            className="mt-4"
          >
            Try again
          </Button>
        </div>
      );
    }
    return this.props.children;
  }
}
