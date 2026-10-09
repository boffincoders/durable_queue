## 0.3.0

Orchestration: priorities, dependencies, chains, and groups.

* Add `priority` to `enqueue`. Among ready tasks, higher priority starts first;
  equal priorities keep enqueue order.
* Add `dependsOn` and `onDependencyFailure` to `enqueue`. A task waits in the
  new `TaskStatus.waiting` state until its dependencies complete. If one fails,
  is cancelled, or is missing, `DependencyFailurePolicy` cancels the task (the
  default), fails it, or runs it anyway. Outcomes cascade to further
  dependents, and the blocking dependency is recorded as a
  `DependencyFailedException` in `lastFailure`.
* Add `enqueueChain` to store tasks that run one after another.
* Add `group` to `enqueue`, `getTasks(group:)`, and `cancelGroup`.
* `retry` restores a task to `waiting` while its dependencies are still active,
  and refuses while a dependency has not completed. `delete` and `purge` never
  remove a task that a waiting task depends on.
* Dependents are written before the task that settles them, so a crash cannot
  leave a task waiting behind a finished dependency. Startup still uses only
  bounded queries.
* Add `example/orchestration.dart`.

### Breaking changes

* `TaskStatus` has a new value, `waiting`. Exhaustive `switch` statements over
  `TaskStatus` need a case for it. It is declared last, so the index of every
  existing value is unchanged. `waiting` is active: it blocks deduplication
  keys and counts for `isActive`.
* Custom `QueueStorage` adapters must implement `getWaitingDependents` and
  `getByGroup`, persist the new `StoredTask` fields (`priority`, `dependsOn`,
  `onDependencyFailure`, `group`), and order `getNextReady` by priority first
  (`compareReadyTasks`). See `STORAGE.md`. Records stored by earlier versions
  load with default values, so no data migration is needed.

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
