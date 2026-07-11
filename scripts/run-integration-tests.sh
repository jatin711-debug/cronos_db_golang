#!/usr/bin/env bash
# Runs the integration test suite against a running server.
# If no server is reachable at CRONOS_TEST_ADDR / CRONOS_TEST_HTTP_ADDR, this
# script builds the server binary, starts it with a temporary data directory,
# waits for the health endpoint, runs the tests, and then tears the server down.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

GRPC_ADDR="${CRONOS_TEST_ADDR:-127.0.0.1:9000}"
HTTP_ADDR="${CRONOS_TEST_HTTP_ADDR:-127.0.0.1:8080}"

BINARY="${REPO_ROOT}/bin/cronos-api"
if [ "$(uname -s)" = "Windows_NT" ] || [ "${OS:-}" = "Windows_NT" ]; then
    BINARY="${BINARY}.exe"
fi

DATA_DIR=""
SERVER_PID=""
MANAGE_SERVER=0

cleanup() {
    if [ -n "${SERVER_PID:-}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        kill "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
    if [ -n "${DATA_DIR:-}" ] && [ -d "${DATA_DIR}" ]; then
        rm -rf "${DATA_DIR}"
    fi
}
trap cleanup EXIT

# Wait up to 60s for the HTTP health endpoint to respond.
wait_for_server() {
    local deadline=$(( $(date +%s) + 60 ))
    while [ "$(date +%s)" -lt "${deadline}" ]; do
        if curl -sf "http://${HTTP_ADDR}/health" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# Check whether a server is already running.
grpc_host="${GRPC_ADDR%:*}"
grpc_port="${GRPC_ADDR##*:}"
http_host="${HTTP_ADDR%:*}"
http_port="${HTTP_ADDR##*:}"

if (echo >/dev/tcp/"${grpc_host}/${grpc_port}") 2>/dev/null && \
   (echo >/dev/tcp/"${http_host}/${http_port}") 2>/dev/null && \
   curl -sf "http://${HTTP_ADDR}/health" >/dev/null 2>&1; then
    echo "Using already-running server at ${GRPC_ADDR} / ${HTTP_ADDR}"
else
    if [ ! -x "${BINARY}" ]; then
        echo "Server binary not found; build it first with: make build"
        exit 1
    fi

    DATA_DIR="$(mktemp -d)"
    echo "Starting integration server: ${BINARY} --dev --data-dir=${DATA_DIR}"

    export LD_LIBRARY_PATH="${REPO_ROOT}/internal/dedup/rust/target/release:${LD_LIBRARY_PATH:-}"

    "${BINARY}" \
        --dev \
        --node-id=ci-test \
        --data-dir="${DATA_DIR}" \
        --grpc-addr="${GRPC_ADDR}" \
        --http-addr="${HTTP_ADDR}" \
        --partition-count=16 \
        --replication-factor=1 \
        --fsync-mode=periodic \
        --flush-interval=50 \
        --index-interval=4096 \
        --segment-size=1073741824 \
        --max-credits=50000 \
        --ack-timeout=60s \
        >/tmp/cronos-integration-server.log 2>&1 &
    SERVER_PID=$!
    MANAGE_SERVER=1

    if ! wait_for_server; then
        echo "Server did not become healthy in time"
        cat /tmp/cronos-integration-server.log || true
        exit 1
    fi
    echo "Server healthy"
fi

export CRONOS_TEST_ADDR="${GRPC_ADDR}"
export CRONOS_TEST_HTTP_ADDR="${HTTP_ADDR}"
export CRONOS_TEST_INTEGRATION=1

set +e
go test -v ./tests/integration/...
TEST_EXIT=$?
set -e

if [ "${MANAGE_SERVER}" -eq 1 ] && [ "${TEST_EXIT}" -ne 0 ]; then
    echo "--- server log ---"
    cat /tmp/cronos-integration-server.log || true
fi

exit "${TEST_EXIT}"
