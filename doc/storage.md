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
| `getByStatus` | Same order, restricted to one status. |
| `getAll` | Same order, every record. |
| `findActiveByDeduplicationKey` | Oldest task with that key whose status is `pending`, `running`, or `retryScheduled`. Return null if none. |

Each method must be atomic. A reader must not observe a half-written task.

Deduplication is check-then-insert inside `DurableQueue`, which serializes `enqueue`. Adapters do not have to expose a multi-method transaction. They do have to make a single method safe against concurrent calls from one isolate.

## Recovery

On `start`, the queue loads tasks in `running`. Those records must still be readable after a process restart, including `attempts`, `retryPolicy`, `cancelRequested`, and `lastFailure`.

Do not delete `running` tasks during adapter startup. The queue decides whether they are retried or failed.

## One consumer

Only one live `DurableQueue` should use a storage instance. Two runners will both claim work.
