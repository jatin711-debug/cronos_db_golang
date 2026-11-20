#!/usr/bin/env bash
# Captures the profiles of the nodes of a running TestSoak that serves them
# (CRONOS_SOAK_PPROF), at given minutes after this script starts: a 30-second
# CPU profile, the goroutines and the heap of each node.
#   capture-profiles.sh <soak log> <out dir> <minute>...
set -uo pipefail
log=$1
out=$2
shift 2
mkdir -p "$out"
start=$(date +%s)
for minute in "$@"; do
  wait_s=$(( start + minute * 60 - $(date +%s) ))
  (( wait_s > 0 )) && sleep "$wait_s"
  # "node1 serves profiles at http://127.0.0.1:41234/debug/pprof/"
  grep -o '[a-z0-9]* serves profiles at http://[^ ]*' "$log" | sort -u | while read -r node _ _ _ url; do
    curl -s -m 60 "${url}profile?seconds=30" -o "$out/cpu-${minute}m-$node.pb.gz" &
    curl -s -m 20 "${url}goroutine?debug=1" -o "$out/goroutine-${minute}m-$node.txt" &
    curl -s -m 20 "${url}heap" -o "$out/heap-${minute}m-$node.pb.gz" &
  done
  wait
  echo "captured at minute $minute: $(ls "$out" | grep -c "${minute}m-") files"
done
