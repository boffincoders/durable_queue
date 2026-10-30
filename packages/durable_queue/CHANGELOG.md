## 0.5.10

New features, no breaking changes.

* `schedule(name, every:, task:)` and `unschedule(name)`: recurring tasks.
  One occurrence is stored at a time; the next is stored when it finishes.
  Missed slots run once, not repeatedly. Each occurrence has an idempotency
  key naming its slot. `schedules` lists the names.
* `QueueMetrics`: counts, success rate, and run and wait times, in total and
  per task type, collected from the event stream. `snapshot().toJson()` for
  analytics.
* `QueueLogger`: one log record per event, hold change, and storage error,
  with a level and `minLevel` filter.
* `QueueStorageCallGuard`: a storage can refuse a call before the queue
  waits for other work. `durable_queue_drift` uses it to refuse calls made
  inside a drift transaction, which could otherwise wait forever.
* `clock`: the queue's clock.

## 0.5.9

New features, no breaking changes. Storage adapters must persist the new
`StoredTask.held` and `StoredTask.releaseAt` fields (see STORAGE.md);
adapters that store the `toJson` record, like `durable_queue_sqlite` and
`durable_queue_hive`, need no change.

* `hold(id)` and `release(id)`: pause and resume one task.
  * A task that has not started does not start while held; its start or
    retry time is kept.
  * A running task is asked to stop; it goes back to the queue held, and the
    interrupted attempt does not count.
  * A handed-off task stays running, and the outside work is told to pause.
* `holdChanges`: a stream of `TaskHoldChanged`, reported when a task is held,
  released, or stops running because of a hold. It is separate from
  `events`, so switches over `QueueEvent` are unaffected.
* `StoredTask.held`, `StoredTask.releaseAt`, and
  `StoredTask.heldNextAttemptAt`.
* A running task that was held when the app stopped comes back held on the
  next `start`, without counting the attempt.

## 0.5.8

New features, no breaking changes. Storage adapters must persist the new
`StoredTask.expiresAt` field (see STORAGE.md); adapters that store the
`toJson` record, like `durable_queue_sqlite` and `durable_queue_hive`, need
no change.

* `register(timeout:)`: an attempt that runs longer fails with
  `TaskTimeoutException`, and the retry rules decide what happens next.
  `context.whenCancellationRequested` completes at the timeout, so handlers
  that listen for it stop their work. Hand-off time does not count.
* `enqueue(expiresAt:, expiresAfter:)` and the same on `enqueueChain`: a task
  that has not started by then is cancelled, with a `TaskExpiredException`
  as its `lastFailure`. `retry` clears an expiry that has passed.
* `counts({group})`: the number of tasks in each status.
* `waitFor(id, {timeout})`: waits until a task is completed, failed, or
  cancelled and returns its record.
* pubspec: screenshots for pub.dev.

## 0.5.7

Documentation only; no code changes.

* README: links to the 10-minute explainer and the documentation site.
* pubspec: `documentation` points to the documentation site, and the description matches the README.

## 0.5.6

Documentation only; no code changes.

* README: `durable_queue_hive`.
* README: links to the benchmarks.

## 0.5.5

Documentation only; no code changes.

* README: links to the guarantees page, the cookbook, and the demo app.
* README: a demo GIF and links to demo videos on real phones.
* README and STORAGE.md: `durable_queue_encryption` for encryption at rest, and the security policy.

## 0.5.4

New features, no breaking changes. Storage adapters and packages built for
0.5.0 keep working; adapters must persist the new `StoredTask` field (see
STORAGE.md).

* Hand-off for work that finishes outside the queue, such as an upload run
  by the operating system: `TaskContext.handOff(reference)` keeps the task
  running after its handler returns, without a concurrency slot and across
  restarts, until `completeHandOff(id, reference:)` or
  `failHandOff(id, error, reference:)` reports the outcome. Failures follow
  the type's retry rules. `cancel` cancels a handed-off task at once.
* `StoredTask.handOffReference` and `StoredTask.isHandedOff`.
* Startup recovery skips handed-off tasks and still reads running tasks in
  bounded batches.
* README: hand-off section; links to `durable_queue_workmanager` and
  `durable_queue_transfer` in the package table, the package diagram, the
  `drain` section, and the Flutter section.

## 0.5.3

New features, no breaking changes. Storage adapters and packages built for
0.5.0 keep working.

* Scheduled tasks: `enqueue(runAt:)` and `enqueue(delay:)` set the earliest
  start; `enqueueChain` applies it to the first step. With `dependsOn`, the
  later of both conditions decides. `TaskEnqueued.runAt` reports it. Uses the
  existing `nextAttemptAt` field, so no storage change.
* `drain({timeLimit, waitForScheduled})`: start, run what is ready, stop when
  idle or at the time limit, and return a `DrainResult` (counts, reason,
  `nextWakeAt`). For background jobs with a time budget.
* `retryFailed({group, retryPolicy})`: retry every failed task, oldest first,
  skipping those `retry` refuses.
* `holdRateLimit(name, key:, duration:)` and `hasRateLimit(name)`: make every
  task in a rate-limit bucket wait, for example after an HTTP 429. Slots
  reserved before the hold are dropped.
* README: "How the packages fit together", a diagram of where each package
  plugs into the engine (gates, storage, handlers, lifecycle and UI); sections
  for scheduled tasks, `drain`, `retryFailed`, and rate-limit holds.

## 0.5.2

Documentation only. No API or behavior changes.

* README: links to `durable_queue_flutter` (app lifecycle and widgets) and
  uses it in the Flutter section.

## 0.5.1

Documentation only. No API or behavior changes.

* README: links to `durable_queue_connectivity` (a ready-made network gate for
  Flutter) and to the network gate and per-host limit support in
  `durable_queue_dio` 0.2.0.

## 0.5.0

Rate limits and gates.

* Add rate limits. `register(rateLimit: RateLimit(10, per: ...))` limits one
  task type; `addRateLimit(name, limit, types:, keyOf:)` adds a limit shared
  by several types, optionally split into buckets by a key such as an API
  host. Every attempt counts, retries included. A task that must wait gets a
  reserved slot and is rescheduled once; other task types keep running.
  Limits are kept in memory.
* Add gates. `addGate(name, gate)` adds a `QueueGate` (for example network
  access) and `register(requires: {...})` makes a task type wait while that
  gate is closed. Deferred tasks keep their status and attempt count and start
  as soon as the gate opens. Running tasks are not cancelled when a gate
  closes. `ManualQueueGate` is a gate the app opens and closes itself.
* Add `register(deferOn:)`: maps a handler error, such as a connection error,
  to a required gate. The task is deferred instead of failing, the attempt
  does not count, and the gate's `recheck` is called.
* Add `DurableQueue(gateRecheckInterval:, gateErrorDelay:)`.
* Add example `example/rate_limits_and_gates.dart`.
* README: a table of the `durable_queue` packages;
  rate limit and gate sections.

### Breaking changes

* New `QueueEvent` subclass `TaskDeferred` (with `DeferralReason`). Exhaustive
  `switch` statements over `QueueEvent` need a case for it.
* No storage contract change. Deferral only moves `nextAttemptAt` of a
  `pending` or `retryScheduled` task, which adapters already honor. Storage
  adapters written for 0.3 and 0.4 keep working.

## 0.4.0

* Add `retryDelay` to `register`: a `RetryDelayResolver` that lets an error
  choose the wait before the next attempt, for example from an HTTP
  `Retry-After` header. Returning null keeps the `RetryPolicy` delay. The
  attempt still counts toward `maxAttempts`; negative waits become zero and
  long ones are capped at `RetryPolicy.maxSchedulableDelay`. A resolver that
  throws falls back to the policy.
* Add `TaskContext.whenCancellationRequested`, a future that completes when a
  running task is cancelled (by `cancel` or `cancelGroup`), so handlers can
  abort requests without polling. `TaskContext` takes it as a new optional
  constructor argument.
* README: `retryDelay`, cancellation signals, and links to
  `durable_queue_sqlite` and `durable_queue_dio`. The storage contract tests now
  live in the repository's `durable_queue_test_support` package.
* No breaking changes and no storage contract changes. Storage adapters for
  0.3.x work unchanged (`durable_queue_sqlite` needs 0.2.0 for its version
  constraint).

## 0.3.1

Documentation and examples only. No API or behavior changes.

* Add `example/image_upload.dart`, a photo-upload flow with selective retries,
  idempotency and deduplication keys, and tasks that survive a restart.
* Add `example/json_file_storage.dart`, a file-backed reference adapter that
  passes the full storage contract suite.
* README: problem statement, quick start, architecture diagram, use cases,
  Flutter integration, storage options, and a capability comparison.
* CONTRIBUTING: discussion, issue, pull request, and adapter guidelines.

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
