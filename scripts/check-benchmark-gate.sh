#!/usr/bin/env bash
set -euo pipefail

# check-benchmark-gate.sh
# Runs the primary WAL throughput benchmark and compares the geometric mean
# MB/s against bench/baseline.txt. Fails if throughput drops more than 10%.

BENCH_PATTERN="BenchmarkWAL_AppendBatch_Matrix/fsync=periodic/payload=256B/batch=1000/par=16"
BASELINE_FILE="bench/baseline.txt"
THRESHOLD=10.0

if [[ ! -f "$BASELINE_FILE" ]]; then
    echo "[bench-gate] Baseline file not found: $BASELINE_FILE"
    echo "[bench-gate] Run: make bench-baseline"
    exit 1
fi

echo "[bench-gate] Running benchmark (this may take ~30s)..."

# `make bench-gate` invokes this script through bash. On Windows that may be
# WSL bash, whose non-login PATH does not contain the host Go installation.
# Resolve a usable Go binary explicitly so the gate fails with a useful error
# instead of silently stopping at the benchmark command.
GO_BIN="${GO_BIN:-}"
if [[ -z "$GO_BIN" ]]; then
    for candidate in go "/mnt/c/Program Files/Go/bin/go.exe" "/c/Program Files/Go/bin/go.exe" \
        /usr/local/go/bin/go /snap/bin/go "$HOME/go/bin/go"; do
        if command -v "$candidate" >/dev/null 2>&1; then
            GO_BIN="$candidate"
            break
        fi
        if [[ -x "$candidate" ]]; then
            GO_BIN="$candidate"
            break
        fi
    done
fi
if [[ -z "$GO_BIN" ]]; then
    echo "[bench-gate] Go executable not found. Set GO_BIN to its path."
    exit 1
fi

CURRENT_FILE="$(mktemp "${TMPDIR:-/tmp}/cronos_bench_current.XXXXXX")"
trap 'rm -f "$CURRENT_FILE"' EXIT

if ! CGO_ENABLED=0 "$GO_BIN" test ./internal/storage \
    -bench="$BENCH_PATTERN" \
    -benchtime=3s \
    -count=3 \
    > "$CURRENT_FILE" 2>&1; then
    echo "[bench-gate] Benchmark command failed: $GO_BIN"
    tail -80 "$CURRENT_FILE"
    exit 1
fi

avg_mbps() {
    awk '/MB\/s/ {
        for (i=1; i<=NF; i++) {
            if ($i ~ /MB\/s/) {
                # value is the previous field
                print $(i-1)
            }
        }
    }' "$1" | awk '{
        if ($1 <= 0) next
        sum += log($1)
        n++
    } END {
        if (n == 0) exit 1
        printf "%.2f", exp(sum / n)
    }'
}

baseline=$(avg_mbps "$BASELINE_FILE")
current=$(avg_mbps "$CURRENT_FILE")

echo "[bench-gate] Baseline: ${baseline} MB/s"
echo "[bench-gate] Current:  ${current} MB/s"

pct=$(awk -v c="$current" -v b="$baseline" 'BEGIN { printf "%.2f", (c - b) / b * 100 }')
echo "[bench-gate] Change:   ${pct}%"

drop=$(awk -v c="$current" -v b="$baseline" 'BEGIN { v = (b - c) / b * 100; if (v < 0) v = 0; printf "%.2f", v }')
ok=$(awk -v d="$drop" -v t="$THRESHOLD" 'BEGIN { print (d <= t) ? 1 : 0 }')

if [[ "$ok" -eq 1 ]]; then
    echo "[bench-gate] PASS (drop ${drop}% <= ${THRESHOLD}%)"
    exit 0
else
    echo "[bench-gate] FAIL (drop ${drop}% > ${THRESHOLD}%)"
    exit 1
fi
