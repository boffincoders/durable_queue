# Storage contract

`QueueStorage` is the persistence boundary. The engine never opens a database itself. `MemoryQueueStorage` keeps tasks in the process and is the reference implementation. A restart only preserves work when the application supplies an adapter that stores `StoredTask` outside memory.

`test/storage_contract.dart` is the executable form of this contract. Adapter packages should mirror those scenarios.

## Records

Persist every `StoredTask` field. `StoredTask.toJson` / `StoredTask.fromJson` is the canonical encoding: ISO-8601 timestamps, retry policy JSON, and the task payload map.

The payload is whatever the application put in `DurableTask.toJson`. Treat it as sensitive. This package does not encrypt it.

## Methods

| Method | Required behavior |
|---|---|
| `save` | Insert a new id. Throw `StateError` if the id exists. |
| `get` | Return the current record, or null. |
| `update` | Replace the record for an existing id. Throw `StateError` if it is missing. |
| `delete` | Remove the id. Do nothing if it is already gone. |
| `getPending` | Tasks in `pending`, oldest `createdAt` first, then `sequence`, then `id`. |
| `getByStatus` | Same order, restricted to one status. Optional positive `limit` bounds results. |
| `getNextReady(now)` | Oldest eligible `pending` or `retryScheduled` task across both statuses, or null. Eligible means `nextAttemptAt` is null or at/before `now`. |
| `getNextWakeAt` | Earliest non-null `nextAttemptAt` across pending and retry-scheduled tasks, including overdue times; null if none. |
| `getMaxSequence` | Maximum stored `sequence`, or zero when empty. |
| `getAll` | Same order, every record. |
| `findActiveByDeduplicationKey` | Oldest task with that key whose status is `pending`, `running`, or `retryScheduled`. Return null if none. |

Each method must be atomic. A reader must not observe a half-written task. Repeating `update` with the same record must be safe: a worker retries the exact replacement after an error, including when an adapter committed the write but failed to acknowledge it. Storage errors must complete the future with an error; the queue cannot detect an adapter future that never completes.

Deduplication is check-then-insert inside `DurableQueue`, which serializes `enqueue`. Adapters do not have to expose a multi-method transaction. They do have to make a single method safe against concurrent calls from one isolate.

## Recovery

On `start`, the queue loads tasks in `running` in batches of at most 100. Each recovered record leaves `running` before the next batch is requested; adapters must honor the query limit. Those records must still be readable after a process restart, including `attempts`, `retryPolicy`, `cancelRequested`, and `lastFailure`.

Do not delete `running` tasks during adapter startup. The queue decides whether they are retried or failed.

## One consumer

Only one live `DurableQueue` should use a storage instance. Two runners will both claim work.

## Bounded worker queries and adapter migration

Adapters implementing the earlier interface must add `getNextReady`, `getNextWakeAt`, and `getMaxSequence`, and accept `limit` in `getByStatus`. These are required methods; there is deliberately no fallback that loads the full queue. Use indexed queries/aggregates and limit before materializing records. Ordering remains `createdAt`, then `sequence`, then `id`, including when due retries compete with pending work.

The worker claims one record per free slot. A due retry moves directly from `retryScheduled` to `running`; other due retries remain stored until claimed. Sequence initialization uses an aggregate, and startup recovery holds at most 100 records per batch. Explicit application calls to `getTasks()` still request all matching records.

`MemoryQueueStorage` maintains ordered indexes for status, active deduplication keys, sequence, time, and eligible task order. It does not sort or copy the full backlog on each claim. As time changes it moves only tasks crossing the eligibility boundary between time indexes, including backwards clock corrections. Memory storage necessarily retains all its records and indexes in the process.

## Worker storage failures

Worker reads and writes retry after `DurableQueue.storageRetryDelay` (default one second), using the injected clock. `storageErrors` reports each failure as a `QueueStorageFailure` value with the operation, original error, stack, timestamp, and consecutive failure count. Task lifecycle events are emitted only after successful writes.

The retry retains the serialized storage lock and, for executing tasks, the execution slot and handler outcome. No new handler is invoked to retry a result write, and storage retries do not consume task attempts. Keeping the lock prevents another operation from overwriting a write whose commit is uncertain. As a consequence an outage can delay enqueue, queries, cancellation, pause, and stop indefinitely. Restore the adapter to allow operations to finish. Handlers already running can finish their business logic while persistence waits.

Public storage calls (enqueue, cancel, and queries) and startup recovery propagate errors to their caller. Failed recovery leaves the queue idle, so `start()` can be retried. Successful recovery writes are retained and their events are emitted even if a later recovery operation fails. Events remain transient, not a durable audit log.
