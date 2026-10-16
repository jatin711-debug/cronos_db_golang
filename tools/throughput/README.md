# Bounded throughput measurement

From PowerShell 7 on Windows, with the local candidate image already built:

```powershell
./scripts/measure-throughput.ps1
```

The script creates three disposable Linux containers of the candidate image.
It uses production mode, RF=3/minISR=2, batch fsync, deduplication, public TLS,
JWT authorization, replication mTLS and encryption at rest. It creates fresh,
short-lived credentials and removes them with the containers and their data.
Existing containers, services, settings and data are not modified.

Default resource limits:

- One CPU core and 256 MiB memory per server; no container swap.
- Server memory admission at 65%; supervisor stop at 75% of a node's cap.
- At least 2,560 MiB available host RAM to start and a 1,536 MiB abort reserve.
- Stop after three consecutive host CPU samples above 90%.
- Two Go scheduler threads, below-normal priority, a 96 MiB Go memory target
  and a 192 MiB working-set stop threshold for the load generator.
- Stop if either local drive has less than 20 GiB free or any node's logical
  data grows past 512 MiB. Docker logs are capped at 10 MiB per container.
- At most 19 minutes overall. RPCs, trials and delivery drain waits are bounded.

These limits reduce risk; resource measurements are sampled and do not promise
that the desktop can never stall. Other workloads can make the supervisor stop
the test. Keep the guards enabled and close unused apps if preflight refuses.

On a host with 16 or more logical processors and enough free RAM, use
`-NodeCPUs 2 -NodeMemoryMiB 384` for a larger bounded budget. This raises the
RAM preflight requirement to 2,944 MiB; the host abort reserves remain active.

The ramp uses 256-byte payloads and batches of 1, 100 and 1,000, raising the
publisher count gradually to at most 16. A trial normally offers load for 15
seconds, or until its 500,000-event cap. Publishers pause when the undelivered
backlog exceeds 20,000. One consumer group runs throughout; every accepted ID
must have delivery evidence. The best reconciled rate is repeated twice.
The consumer uses a 64-event buffer and one handler worker per partition,
with asynchronous ACKs batched at 256 requests or 10 ms.

Output lives under ignored `build/throughput/cronos-peak-*/`:

- `config.json`: source revision, exact local image ID and workload settings.
- `trials.jsonl` and `results.json`: unique newly accepted events, reconciled
  deliveries, duplicates, errors, ambiguous outcomes, rates and latency.
- `resources.csv` and `safety.json`: sampled headroom and any stop reason.
- Per-node logs and final container states, including OOM status.

Accepted throughput covers the publish interval. Accounted throughput includes
the time needed to deliver all those accepted IDs. Publish percentiles are per
batch RPC, in 0.1 ms histogram buckets, capped at 6,000 ms. Delivery lateness
is measured against each event's immediate schedule timestamp. Duplicate or
ambiguous responses are not counted as new accepted throughput. A trial with
publish errors, ambiguous outcomes, missing accepted IDs or an external stop
does not qualify as valid.

This measures the highest observed rate within the stated local budget. It is
not a maximum for unrestricted hardware, a multi-host capacity result, or a
comparison against another release. Client CPU, the Docker VM and other apps
share the machine. Very short trials that hit the event cap need a longer
follow-up before treating their rate as sustainable.

For a small setup/accounting check:

```powershell
./scripts/measure-throughput.ps1 -Plan '100:1' -Seconds 5 -MaxEvents 3000
```

The host guards remain active for that check. An unstarted or stopped run is
evidence that a guard fired, not a throughput measurement.
