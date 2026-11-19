#!/usr/bin/env bash
# Stands in for remote hosts when trying the soak's remote mode on one Linux
# machine: runs each of three CronosDB nodes as a process under a directory
# of its own, and acts on it as CRONOS_SOAK_REMOTE's commands do.
#
#   local-remote-node.sh setup <cronos-api binary>   certificates, key and remote.json
#   local-remote-node.sh <start|kill|freeze|thaw|wipe|disk|logs> <node1|node2|node3>
#
# Then: CRONOS_SOAK_REMOTE=$CRONOS_REMOTE_ROOT/remote.json CRONOS_SOAK_DURATION=20m \
#   go test -tags acceptance -run '^TestSoak$' -v ./tests/acceptance/
#
# CRONOS_REMOTE_ROOT is where nodes keep their data and logs ($HOME/remote-soak).
# Ports: 19101-19103 gRPC, 19201-19203 HTTP, 19301-19303 gossip,
# 19401-19403 cluster gRPC, 19501-19503 Raft; the soak's change-feed
# receiver listens on 19900. Needs openssl.
set -uo pipefail
root="${CRONOS_REMOTE_ROOT:-$HOME/remote-soak}"
self="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
bin="$root/cronos-api"
port() { echo $(( $1 + ${node#node} )); }

if [[ ${1:-} == setup ]]; then
  [[ -x ${2:-} ]] || { echo "usage: $0 setup <cronos-api binary>" >&2; exit 2; }
  rm -rf "$root" && mkdir -p "$root/certs"
  cp "$2" "$bin" && chmod +x "$bin"
  cd "$root/certs"
  openssl req -x509 -newkey rsa:2048 -nodes -days 7 -subj "/CN=remote-soak-ca" \
    -keyout ca.key -out ca.crt -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
  openssl req -newkey rsa:2048 -nodes -subj "/CN=remote-soak-node" -keyout tls.key -out tls.csr 2>/dev/null
  printf 'subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=critical,digitalSignature,keyEncipherment\n' > ext.cnf
  openssl x509 -req -in tls.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 7 -extfile ext.cnf -out tls.crt 2>/dev/null
  openssl rand -hex 16 > wal.key
  # The server refuses a key that others can read.
  chmod 600 wal.key tls.key ca.key
  {
    echo '{'
    echo '  "ca": "certs/ca.crt", "cert": "certs/tls.crt", "key": "certs/tls.key",'
    echo '  "encryption_key_file": "certs/wal.key",'
    echo '  "cdc_listen": "127.0.0.1:19900", "cdc_url": "http://127.0.0.1:19900/cdc",'
    echo '  "nodes": ['
    for i in 1 2 3; do
      node=node$i
      sep=$([[ $i -lt 3 ]] && echo , || true)
      echo "    {\"id\": \"$node\", \"grpc\": \"127.0.0.1:$(port 19100)\", \"http\": \"127.0.0.1:$(port 19200)\", \"cluster\": \"127.0.0.1:$(port 19400)\","
      for action in start kill freeze thaw wipe disk logs; do
        end=$([[ $action == logs ]] && echo "}$sep" || echo ",")
        echo "     \"$action\": \"bash $self $action $node\"$end"
      done
    done
    echo '  ]'
    echo '}'
  } > "$root/remote.json"
  echo "wrote $root/remote.json"
  exit 0
fi

action=${1:?usage}
node=${2:?usage}
dir="$root/$node"
mkdir -p "$dir"
pidfile="$dir/pid"
case "$action" in
  start)
    args=(--dev --node-id="$node" --auth-enabled=false --cluster --partition-count=4
      --replication-factor=3 --min-insync-replicas=2 --cluster-expected-nodes=3
      --fsync-mode=every_event --segment-size="${REMOTE_SEGMENT_BYTES:-8388608}" --compaction-interval=30s
      --bloom-capacity=1000000 --dedup-ttl=168 --memory-limit="${REMOTE_MEMORY_BYTES:-402653184}" --max-retries=3
      --backup-interval=0 --encryption-enabled --encryption-key-file="$root/certs/wal.key"
      --follower-reads=true --ack-timeout=5s --data-dir="$dir/data"
      --grpc-addr="127.0.0.1:$(port 19100)" --http-addr="127.0.0.1:$(port 19200)"
      --cluster-gossip-addr="127.0.0.1:$(port 19300)" --cluster-grpc-addr="127.0.0.1:$(port 19400)"
      --cluster-raft-addr="127.0.0.1:$(port 19500)" --advertise-grpc-addr="127.0.0.1:$(port 19100)"
      --replication-tls-enabled --replication-tls-ca-file="$root/certs/ca.crt"
      --replication-tls-cert-file="$root/certs/tls.crt" --replication-tls-key-file="$root/certs/tls.key"
      --cluster-seeds=127.0.0.1:19301,127.0.0.1:19302,127.0.0.1:19303)
    [[ $node == node1 ]] && args+=(--cluster-bootstrap)
    echo "=== $(date +%T) start ===" >> "$dir/log"
    GOMAXPROCS="${REMOTE_GOMAXPROCS:-2}" CRONOS_CDC_WEBHOOK_URL=http://127.0.0.1:19900/cdc \
      nohup "$bin" "${args[@]}" >> "$dir/log" 2>&1 < /dev/null &
    echo $! > "$pidfile"
    ;;
  kill)
    if [[ -f $pidfile ]]; then
      pid=$(cat "$pidfile")
      kill -CONT "$pid" 2>/dev/null
      kill -KILL "$pid" 2>/dev/null
      for _ in $(seq 1 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
      rm -f "$pidfile"
      echo "=== $(date +%T) killed ===" >> "$dir/log"
    fi
    ;;
  freeze) kill -STOP "$(cat "$pidfile")" ;;
  thaw) kill -CONT "$(cat "$pidfile")" ;;
  wipe) rm -rf "$dir/data" ;;
  disk) du -sb "$dir/data" 2>/dev/null | cut -f1 || echo 0 ;;
  logs) tail -n 3000 "$dir/log" ;;
  *) echo "unknown action $action" >&2; exit 2 ;;
esac
