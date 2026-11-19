#!/usr/bin/env bash
# Network faults between CronosDB hosts, for soaks on separate machines.
# Run it as root on the host whose traffic is to suffer. Every fault it adds
# is removed by `clear`; nothing it does survives a reboot.
#
#   netem.sh delay  <iface> <ms> [jitter-ms]    add latency to everything sent on iface
#   netem.sh loss   <iface> <percent>           drop that share of packets sent on iface
#   netem.sh both   <iface> <ms> <percent>      latency and loss together
#   netem.sh cut    <peer-ip>...                drop all traffic to and from these hosts
#   netem.sh heal   <peer-ip>...                undo cut for these hosts
#   netem.sh clear  <iface>                     remove every fault this script added
#   netem.sh show   <iface>                     print the faults in place
#
# Examples, on node2 of 10.0.0.1-3:
#   netem.sh delay eth0 80 20     # 80 ms +/- 20 ms to every peer and client
#   netem.sh cut 10.0.0.1         # node2 and node1 no longer reach each other
#   netem.sh clear eth0
#
# delay and loss shape all traffic on the interface, clients included. cut
# uses an iptables chain of its own, CRONOS-NETEM, so it removes only what it
# added. A cut is one-sided: run it on both hosts for a symmetric partition,
# or on one host only to test a link that fails in one direction.
set -euo pipefail

chain=CRONOS-NETEM

usage() {
  sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
}

need_root() {
  if [[ $(id -u) -ne 0 ]]; then
    echo "netem.sh: run as root" >&2
    exit 1
  fi
}

ensure_chain() {
  iptables -N "$chain" 2>/dev/null || true
  iptables -C INPUT -j "$chain" 2>/dev/null || iptables -I INPUT -j "$chain"
  iptables -C OUTPUT -j "$chain" 2>/dev/null || iptables -I OUTPUT -j "$chain"
}

shape() {
  local iface=$1
  shift
  tc qdisc replace dev "$iface" root netem "$@"
}

[[ $# -ge 1 ]] || usage
command=$1
shift
case "$command" in
  delay)
    [[ $# -ge 2 ]] || usage
    need_root
    if [[ $# -ge 3 ]]; then
      shape "$1" delay "${2}ms" "${3}ms" distribution normal
    else
      shape "$1" delay "${2}ms"
    fi
    ;;
  loss)
    [[ $# -eq 2 ]] || usage
    need_root
    shape "$1" loss "${2}%"
    ;;
  both)
    [[ $# -eq 3 ]] || usage
    need_root
    shape "$1" delay "${2}ms" loss "${3}%"
    ;;
  cut)
    [[ $# -ge 1 ]] || usage
    need_root
    ensure_chain
    for peer in "$@"; do
      iptables -C "$chain" -s "$peer" -j DROP 2>/dev/null || iptables -A "$chain" -s "$peer" -j DROP
      iptables -C "$chain" -d "$peer" -j DROP 2>/dev/null || iptables -A "$chain" -d "$peer" -j DROP
    done
    ;;
  heal)
    [[ $# -ge 1 ]] || usage
    need_root
    for peer in "$@"; do
      while iptables -D "$chain" -s "$peer" -j DROP 2>/dev/null; do :; done
      while iptables -D "$chain" -d "$peer" -j DROP 2>/dev/null; do :; done
    done
    ;;
  clear)
    [[ $# -eq 1 ]] || usage
    need_root
    tc qdisc del dev "$1" root 2>/dev/null || true
    if iptables -L "$chain" -n >/dev/null 2>&1; then
      iptables -D INPUT -j "$chain" 2>/dev/null || true
      iptables -D OUTPUT -j "$chain" 2>/dev/null || true
      iptables -F "$chain"
      iptables -X "$chain"
    fi
    ;;
  show)
    [[ $# -eq 1 ]] || usage
    tc qdisc show dev "$1"
    iptables -L "$chain" -n -v 2>/dev/null || echo "no cuts in place"
    ;;
  *)
    usage
    ;;
esac
