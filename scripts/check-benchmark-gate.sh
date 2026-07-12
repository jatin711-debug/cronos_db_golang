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
CGO_ENABLED=0 go test ./internal/storage \
    -bench="$BENCH_PATTERN" \
    -benchtime=3s \
    -count=3 \
    > /tmp/cronos_bench_current.txt 2>/dev/null

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
current=$(avg_mbps /tmp/cronos_bench_current.txt)

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
