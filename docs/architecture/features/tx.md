# Transactions (2PC) Architecture

## Purpose

Transaction module coordinates prepare and commit phases for multi-step consistency workflows.

## Key Files

- [internal/tx/coordinator.go](../../../internal/tx/coordinator.go)
- [internal/tx/transaction_handler.go](../../../internal/tx/transaction_handler.go)
- [proto/events.proto](../../../proto/events.proto)

## Main Flow

1. Transaction request enters transaction service handler.
2. Coordinator Prepare path executes phase-1 state transition and participant checks.
3. Commit path applies phase-2 commit operations from prepared state.
4. Recovery path replays durable transaction log on restart.

## Production Decisions

- Prepare and commit are explicitly separated to avoid phase conflation.
- State transitions are persisted (`tx_log.json`) for restart safety.
- `tx.NewHandler(pm)` injects the **PartitionManager** into the coordinator so gRPC Begin/Prepare/Commit write real partition participants (not PM-nil no-ops).
- Idempotent recovery rehydrates prepared/committing transactions after crash.

## Debug Pointers

- State transition bugs: [internal/tx/coordinator.go](../../../internal/tx/coordinator.go)
- RPC mapping and validation: [internal/tx/transaction_handler.go](../../../internal/tx/transaction_handler.go)

## Diagrams

### Transaction state machine

```mermaid
stateDiagram-v2
    [*] --> Pending : BeginTransaction
    Pending --> Prepared : PrepareTransaction - all participants vote yes
    Pending --> Aborted : any vote no / AbortTransaction / recovery timeout
    Pending --> Committing : CommitTransaction auto-prepares when Pending
    Prepared --> Committing : CommitTransaction
    Prepared --> Aborted : AbortTransaction
    Committing --> Committed : all participant commit markers written
    Committing --> Committing : commit callback failure - stays Committing, recovery loop retries with backoff
    Committed --> [*]
    Aborted --> [*]

    note right of Committing
        Coordinator statuses are exactly: Pending, Prepared,
        Committing, Committed, Aborted. There are no
        FailedCommit or Recovering states: the recovery loop
        (30s ticker) re-drives Commit for Prepared/Committing
        transactions and only aborts Pending transactions
        that exceeded their timeout.
    end note
```

### Related diagrams

- [System overview](../README.md#system-overview)
