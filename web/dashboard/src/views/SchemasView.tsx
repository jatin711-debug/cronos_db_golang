import { useListSchemas, useGetSchema } from "@/lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Loading } from "@/components/Loading";
import { ErrorAlert } from "@/components/ErrorAlert";
import { useState } from "react";

export function SchemasView() {
  const schemas = useListSchemas();
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null);

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Schemas</CardTitle>
          <CardDescription>Registered topics and their latest schema versions.</CardDescription>
        </CardHeader>
        <CardContent>
          {schemas.loading && <Loading message="Loading schemas..." />}
          {schemas.error && <ErrorAlert message={schemas.error} onRetry={schemas.refetch} isAuth={schemas.error.startsWith("401")} />}
          {schemas.data && schemas.data.schemas.length === 0 && (
            <p className="text-sm text-[var(--color-muted-foreground)]">No schemas registered.</p>
          )}
          {schemas.data && schemas.data.schemas.length > 0 && (
            <table className="w-full text-sm">
              <thead className="text-left text-[var(--color-muted-foreground)]">
                <tr>
                  <th className="py-2 font-medium">Topic</th>
                  <th className="py-2 font-medium">Latest Version</th>
                  <th className="py-2 font-medium">Compatibility</th>
                </tr>
              </thead>
              <tbody>
                {schemas.data.schemas.map((s) => (
                  <tr
                    key={s.topic}
                    className="cursor-pointer border-t border-[var(--color-border)] hover:bg-[var(--color-accent)]"
                    onClick={() => setSelectedTopic(s.topic)}
                  >
                    <td className="py-2 font-mono">{s.topic}</td>
                    <td className="py-2">{s.latest_version}</td>
                    <td className="py-2">{s.compatibility_mode}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </CardContent>
      </Card>

      {selectedTopic && <SchemaDetail topic={selectedTopic} />}
    </div>
  );
}

function SchemaDetail({ topic }: { topic: string }) {
  const { data, loading, error, refetch } = useGetSchema(topic);

  if (loading) return <Loading message="Loading schema..." />;
  if (error) return <ErrorAlert message={error} onRetry={refetch} isAuth={error.startsWith("401")} />;
  if (!data) return <ErrorAlert message="No schema data returned." onRetry={refetch} />;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Schema: {data.topic}</CardTitle>
        <CardDescription>Version {data.version} · {data.schema_type}</CardDescription>
      </CardHeader>
      <CardContent>
        <pre className="max-h-96 overflow-auto rounded-md bg-[var(--color-muted)] p-4 text-xs">
          {data.definition}
        </pre>
      </CardContent>
    </Card>
  );
}
