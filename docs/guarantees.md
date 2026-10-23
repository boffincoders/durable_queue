# What durable_queue guarantees

This page says what the durable_queue packages promise, what they do not, and how that is tested. If you find a case where the code does not do what this page says, that is a bug: please [open an issue](https://github.com/boffincoders/durable_queue/issues).

## What we promise

**A task is stored before `enqueue` returns.** With `durable_queue_sqlite` the write is a committed SQLite transaction (write-ahead log, `synchronous = FULL`), so it survives the app being killed or the device losing power right after. `MemoryQueueStorage` keeps tasks in memory only, for tests and for work that may be lost.

**Every task runs at least once, until it succeeds or its attempts run out.** A task whose attempt was interrupted by a crash or a kill is found when the queue next starts: the attempt counts, and the retry policy decides whether it runs again or fails.

**Attempts are bounded.** A task runs at most `maxAttempts` times. Waiting for a closed gate (for example, no network) or for a rate-limit slot does not use attempts. A task that fails for good stays stored with its last error until you retry, delete, or purge it.

**Order.** Among tasks that are ready, higher `priority` starts first, then the oldest. With `maxConcurrentTasks: 1` tasks run one at a time in that order. With more than one slot, tasks can finish in any order; use `dependsOn` or `enqueueChain` when one must wait for another.

**Dependencies.** A task never starts before every task it depends on has completed. If a dependency fails or is cancelled, `onDependencyFailure` decides: cancel (default), fail, or run anyway. A crash between a task finishing and its result being written never leaves its dependents waiting forever.

**Deduplication.** While a task with a given `deduplicationKey` is pending, waiting, running, or scheduled to retry, `enqueue` with the same key returns the existing task's id and adds nothing. Finished tasks do not block the key.

**Cancellation.** Cancelling a queued, waiting, or scheduled task marks it cancelled at once; it will not start. A running handler is asked to stop (`context.isCancellationRequested`, `context.whenCancellationRequested`) and is not interrupted: if it returns normally, the task is completed. A task handed off to outside work is cancelled at once, and a `TaskCancelled` event tells whoever runs the outside work to stop it.

**Work that finishes outside the queue.** A handed-off task (`TaskContext.handOff`) keeps waiting across restarts until its outcome is reported. Its reference is stored as soon as `handOff` is called, so a crash before the handler returns does not start the outside work a second time. An outcome is applied only to the attempt it belongs to, and only once.

## What we do not promise

**Exactly once.** If the app dies after a handler's side effect but before the result is written, the task runs again. Make handlers safe to repeat, for example by sending the task's `idempotencyKey` to your server so it can drop a repeat. In the device test below, uploads cut off by a kill were sent again; the step after each upload ran exactly once.

**Running while the app is closed.** The core queue runs while your app's process runs. Two packages extend that, within what the operating system allows:

* `durable_queue_transfer` hands uploads and downloads to the system's background transfer service. They continue while the app is in the background or closed. If the user force-quits the app (swipes it away), the system may stop them until the app is opened again; nothing is lost, they are restarted or retried then.
* `durable_queue_workmanager` runs the queue in Workmanager jobs. Android runs them at most every 15 minutes, for up to about 10 minutes; iOS decides by itself when, and gives each about 30 seconds.

**Results of transfers while the app is closed.** A transfer that finishes while the app is closed is recorded by the system; its queue task completes, and the tasks after it run, the next time the app runs.

**Rate limits across restarts.** Rate limits are kept in memory and start empty after a restart.

**Exact timing.** A scheduled task or retry starts at or after its time, when the queue is running. Times use the device clock.

**Encryption.** Task payloads and error messages are stored as plain data. Do not put secrets in them.

**Several runners on one storage.** Only one queue should run a storage at a time: a starting queue treats running tasks as interrupted. Other connections may read and enqueue. `durable_queue_workmanager`'s lease lets the app and a background job take turns. On the web, open a queue in one tab at a time.

## How we test it

* Every package has its own test suite, run in CI on every push: the Dart packages with the oldest supported Dart (3.13) and the latest stable (SQLite and Dio also on macOS), the Flutter packages with the latest stable Flutter. Time-dependent behaviour is tested with `FakeQueueClock`, without waiting.
* New features are mutation-tested: we change the code on purpose (drop a check, flip a condition) and make sure a test fails.
* Storage adapters run a shared storage contract suite. `durable_queue_sqlite` runs it natively and in a browser (WebAssembly and IndexedDB), and is tested with several isolates writing to one file at once.
* The [demo app](../examples/durable_queue_demo) is killed in the middle of its uploads on an Android emulator and an iOS simulator ([device tests](../.github/workflows/device-tests.yml)). The test passes only if every photo is uploaded and the step after each upload ran exactly once.

| Device | What the test does | Result |
| --- | --- | --- |
| Android 14 emulator (API 34) | Kills the app process with 6 uploads in flight; relaunches it offline (nothing may be sent), then online | All 6 uploaded, each follow-up exactly once; the 6 cut-off uploads were sent again (at least once) |
| iOS simulator (iPhone 17 Pro) | Terminates the app just as its 6 uploads finish; relaunches it | All 6 uploaded once, each follow-up exactly once |

These are emulators and simulators; real phones can differ, for example in how aggressively they stop apps in the background. Results of every run are on the [Actions page](https://github.com/boffincoders/durable_queue/actions/workflows/device-tests.yml).
