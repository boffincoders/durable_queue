# Execution semantics

`durable_queue` stores serializable work and runs it locally. It does not promise that a killed process will be relaunched, and it does not promise exactly-once side effects.

## At least once

A handler can finish its external side effect and then the process can die before the queue writes `completed`. The next `start` treats a `running` task as interrupted and may run it again.

Use an idempotent handler, or pass `idempotencyKey` and send that key to the external system yourself. The queue stores the key and puts it on `TaskContext`. It does not speak HTTP.

## What survives process death

| Situation | What version 0.1.0 does |
|---|---|
| App is running and the queue is started | Eligible tasks run. |
| App is suspended or killed | Nothing is scheduled by the operating system. |
| App launches again and `start` is called | Stored tasks are loaded. `running` tasks are recovered. |

`MemoryQueueStorage` does not survive process death. Persistence across launches requires a storage adapter.

## States

```text
pending → running → completed
                 ↘ retryScheduled → running
                 ↘ failed
pending or retryScheduled → cancelled
```

Due retries are claimed directly from `retryScheduled`; they are not all rewritten as pending when their deadline passes.

`attempts` is the number of attempts that have started. The attempt is incremented when a task moves to `running`, before the handler returns.

## Retries

`RetryPolicy.none()` runs once.

`RetryPolicy.fixed` waits the same duration after each failure that is allowed to retry.

`RetryPolicy.exponential` waits `initialDelay` after attempt 1, then doubles, and stops growing at `maxDelay`. Delays are also capped at 365 days.

`jitter: true` multiplies the delay by a factor in `[1 - jitterFactor, 1 + jitterFactor)` and then caps it at `maxDelay`. The default `jitterFactor` is `0.2`. Pass `randomFraction` to `DurableQueue` to make that choice deterministic in tests. The function must return a value in `[0, 1)`.

`retryIf` on `register` chooses which errors are worth retrying. The default retries every handler error until `maxAttempts` is spent. These are never retried:

* `TaskCancelledException`
* a payload that fails to decode, or a decoded `type` that does not match the stored type
* an unregistered type found in storage
* a `retryIf` callback that throws

A task left in `running` at `start` counts as a failed attempt whose error is `TaskInterruptedException`. `retryIf` is not consulted, because the handler did not throw. The stored retry policy decides whether another attempt is scheduled. If cancellation had already been requested, the task is marked `cancelled` instead.

## Deduplication

When `deduplicationKey` is set, `enqueue` returns the existing id and does not change that task if another task with the same key is `pending`, `running`, or `retryScheduled`.

`completed`, `failed`, and `cancelled` tasks do not block the key. The new payload is not merged into the old one.

`idempotencyKey` never deduplicates. It is metadata for the handler.

## Cancellation

`cancel` on `pending` or `retryScheduled` marks the task `cancelled`. The handler is not called.

`cancel` on `running` does not abort the Dart future. `TaskContext.isCancellationRequested` becomes true.

* If the handler then returns normally, the task is `completed`. The work finished.
* If the handler then throws, including `TaskCancelledException`, the task is `cancelled` and is not retried.

`cancel` on a terminal task does nothing. `cancel` of an unknown id throws `TaskNotFoundException`.

## Controls

| Method | Effect |
|---|---|
| `start` | Allowed from `idle`. Recovers `running` tasks in bounded batches, then starts eligible work. Failed recovery leaves the queue idle and can be retried. |
| `pause` | Allowed from `running`. In-flight handlers finish. Nothing new starts, including due retries. |
| `resume` | Allowed from `paused`. Continues starting work. |
| `stop` | From `running` or `paused`, waits for in-flight handlers and returns to `idle`. Other tasks stay stored. `stop` on `idle` does nothing. `stop` does not finish if a handler never returns. |

`enqueue` while idle, paused, or stopping only stores the task.

## Ordering and concurrency

Eligible tasks start oldest `createdAt` first, then by enqueue `sequence`, then by id. `sequence` keeps tasks ordered when they are stored in the same clock tick. At most `maxConcurrentTasks` handlers run at once. A task waiting for its retry does not occupy a slot and does not block other pending tasks.

## Errors

Handler exceptions are caught per task. One failure does not stop the worker. The latest failure is stored as text on the task. Do not put secrets in exception messages or task payloads.

Storage failures are separate from handler failures. Worker storage operations retry after `storageRetryDelay` (one second by default), and publish diagnostics as values on `queue.storageErrors`. A failed result write retains the handler outcome and execution slot; retries repeat only the storage operation. They do not consume handler attempts. Storage retries keep the serialized lock, so an outage can delay controls, queries, cancellation, and enqueue until storage recovers. `stop()` also waits for outstanding persistence. Public API storage calls and startup recovery propagate errors to the caller. See [storage contract](storage.md) for details.

Stored payloads are deeply immutable snapshots. Each decoder receives an independent mutable JSON copy, so changes made by a handler cannot alter later attempts or query results.

## Clock

Timestamps and retry delays use the injected `QueueClock`. `FakeQueueClock.advance` wakes due delays without waiting in real time. Subscribe to `events` before the action you want to observe. The stream is broadcast and does not replay.
