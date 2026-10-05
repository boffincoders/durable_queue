## 0.2.0

* Add `DurableQueue.retry` to re-queue a failed or cancelled task with a fresh
  attempt budget and an optional replacement retry policy.
* Add `DurableQueue.delete` and `DurableQueue.purge` so finished tasks no longer
  accumulate in storage forever. `purge` filters by terminal status and age.
* Add `DurableQueue.close` and `isClosed` to stop the worker and close the
  `events` and `storageErrors` streams.
* Add `TaskStatus.isTerminal` and `TaskStatus.isActive`.
* Fix four unresolved dartdoc references and the `TaskStatus` state diagram
  (due retries move straight to `running`).
* Add `issue_tracker` and `topics` to the pubspec.
* No storage contract changes: existing `QueueStorage` adapters keep working.
* The storage contract for adapter authors now lives in `STORAGE.md` at the
  package root.

## 0.1.0

* Initial release of the durable task queue.
* Serializable tasks with immutable payload snapshots and isolated execution attempts.
* Indexed in-memory storage, bounded worker queries, and a documented storage contract.
* Constant-delay and exponential retries, jitter, retry predicates, and attempt limits.
* Concurrency limits, deduplication, idempotency metadata, and cancellation.
* Pause, resume, and stop controls.
* Lifecycle events, task queries, and failure metadata.
* Batched startup recovery for interrupted tasks.
* Automatic retries of worker storage operations without rerunning handlers,
  with a storage error stream and configurable retry delay.
* An injectable clock so retry tests do not wait on real time.
