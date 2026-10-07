#!/usr/bin/env bash
# Run only against the disposable CI cluster created by the workflow.
set -euo pipefail
test "$(kubectl config current-context)" = "kind-cronos-ci"
namespace=cronos-ci
cert_dir=$(mktemp -d)
trap 'rm -rf -- "$cert_dir"' EXIT
kubectl create namespace "$namespace"

# Ephemeral test credentials; never reuse these outside this disposable cluster.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=cronos-ci-ca \
  -keyout "$cert_dir/ca.key" -out "$cert_dir/ca.crt" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj /CN=cronos-ci \
  -keyout "$cert_dir/tls.key" -out "$cert_dir/tls.csr" 2>/dev/null
cat > "$cert_dir/extensions" <<'EOF'
subjectAltName=DNS:localhost,DNS:cronos-cronos-db,DNS:*.cronos-cronos-db-headless.cronos-ci.svc,DNS:*.cronos-cronos-db-headless.cronos-ci.svc.cluster.local
extendedKeyUsage=serverAuth,clientAuth
EOF
openssl x509 -req -in "$cert_dir/tls.csr" -CA "$cert_dir/ca.crt" -CAkey "$cert_dir/ca.key" \
  -CAcreateserial -days 1 -extfile "$cert_dir/extensions" -out "$cert_dir/tls.crt" 2>/dev/null
for secret in cronos-db-tls cronos-db-replication-tls; do
  kubectl -n "$namespace" create secret generic "$secret" \
    --from-file=tls.crt="$cert_dir/tls.crt" --from-file=tls.key="$cert_dir/tls.key" --from-file=ca.crt="$cert_dir/ca.crt"
done
openssl rand -hex 16 | tr -d '\n' > "$cert_dir/master.key"
openssl rand -hex 32 | tr -d '\n' > "$cert_dir/jwt-secret"
printf '%s' '{"ci-admin":{"admin":true}}' > "$cert_dir/policy.json"
kubectl -n "$namespace" create secret generic cronos-db-auth \
  --from-file=jwt-secret="$cert_dir/jwt-secret" --from-file=policy.json="$cert_dir/policy.json"
kubectl -n "$namespace" create secret generic cronos-db-encryption --from-file=master.key="$cert_dir/master.key"

helm install cronos charts/cronos-db -n "$namespace" -f charts/cronos-db/values-production.yaml \
  --set replicaCount=3 --set image.repository=cronos-db --set image.tag=ci --set image.pullPolicy=Never \
  --set config.partitionCount=3 --set config.bloomCapacity=1000 --set config.segmentSizeBytes=1048576 \
  --set persistence.size=1Gi --set metrics.serviceMonitor.enabled=false --set alerts.enabled=false \
  --set resources.requests.cpu=100m --set resources.requests.memory=256Mi \
  --set resources.limits.cpu=1 --set resources.limits.memory=1Gi \
  --wait --timeout 7m

for ordinal in 0 1 2; do
  pod="cronos-cronos-db-$ordinal"
  kubectl -n "$namespace" exec "$pod" -- sh -ec '
    test "$(stat -c %a /etc/cronos/encryption/master.key)" = 600
    test "$CRONOS_DEV" = false
    test "$CRONOS_AUTH_ENABLED" = true
    test "$CRONOS_REPLICATION_TLS_ENABLED" = true
    test -s "$CRONOS_AUTH_POLICY_FILE"
    curl -fsS http://localhost:8080/health/ready >/dev/null
    curl -fsS http://localhost:8080/ui/ | grep -q "<html"
    test "$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/admin/topology)" = 401
  '
done

statefulset=statefulset/cronos-cronos-db
logs_of() { kubectl -n "$namespace" logs "$1" --tail=-1; }

# A rolling restart: every pod is replaced in turn, and each comes back into
# the cluster it has on its volume.
kubectl -n "$namespace" rollout restart "$statefulset"
kubectl -n "$namespace" rollout status "$statefulset" --timeout=10m
kubectl -n "$namespace" wait --for=condition=Ready pod -l app.kubernetes.io/instance=cronos --timeout=5m
first_log=$(logs_of cronos-cronos-db-0)
grep -q "Continuing in the cluster this node has on disk" <<<"$first_log"
if grep -q "Bootstrapping new Raft cluster" <<<"$first_log"; then
  echo "pod 0 created a cluster again after a restart" >&2
  exit 1
fi

# Pod 0 is the one that created the cluster. With its volume emptied it must
# join the cluster the other pods still hold, and not create a second one.
kubectl -n "$namespace" scale "$statefulset" --replicas=0
kubectl -n "$namespace" wait --for=delete pod -l app.kubernetes.io/instance=cronos --timeout=5m
kubectl -n "$namespace" apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: empty-volume-0
spec:
  restartPolicy: Never
  securityContext:
    runAsUser: 1000
    runAsGroup: 1000
    fsGroup: 1000
  containers:
    - name: empty
      image: cronos-db:ci
      imagePullPolicy: Never
      command: ["/bin/sh", "-ec", "find /data -mindepth 1 -delete; test -z \"$(ls -A /data)\""]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: data-cronos-cronos-db-0
EOF
kubectl -n "$namespace" wait --for=jsonpath='{.status.phase}'=Succeeded pod/empty-volume-0 --timeout=3m
kubectl -n "$namespace" delete pod empty-volume-0
kubectl -n "$namespace" scale "$statefulset" --replicas=3
kubectl -n "$namespace" rollout status "$statefulset" --timeout=10m
kubectl -n "$namespace" wait --for=condition=Ready pod -l app.kubernetes.io/instance=cronos --timeout=5m
first_log=$(logs_of cronos-cronos-db-0)
grep -q "A cluster exists already" <<<"$first_log"
if grep -q "Bootstrapping new Raft cluster" <<<"$first_log"; then
  echo "pod 0 created a second cluster after losing its volume" >&2
  exit 1
fi
