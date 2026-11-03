# Roadmap

What durable_queue does today, what comes next, and what is only considered. Plans change with what users need: say what you need in [GitHub Discussions](https://github.com/boffincoders/durable_queue/discussions), and give a 👍 to the ideas you want most.

Nothing here has a date. Each release says what it adds in the package's CHANGELOG.

## Available now

**Storing and running work**

* Tasks stored before `enqueue` returns, in SQLite, drift, Hive, or memory, optionally encrypted with AES-256-GCM.
* Retries with backoff and jitter, `Retry-After`, timeouts per task type, and tasks that expire.
* Priorities, dependencies and chains, groups, deduplication and idempotency keys, batches (`enqueueAll`).
* Concurrency limits for the queue and per task type, and rate limits per type or per key.
* Recurring tasks that survive restarts.
* Task versions, so tasks stored by an older app release run after an update.

**Waiting and recovering**

* Gates: tasks wait while offline, or for any condition, without using attempts.
* Recovery of tasks interrupted by a crash, and a dead-letter queue of failed tasks with their errors.
* Hold and release one task, for example to let a user pause an upload.

**Outside the app's process**

* Uploads and downloads the operating system finishes after the app is closed, with pause and resume.
* Background jobs on Android and iOS through Workmanager.

**Seeing what happens**

* Events, counts, `waitFor`, progress reporting, metrics, and logging.
* Flutter widgets that rebuild from stored tasks and progress.

## Next

* **A stable 1.0.** A review of the API names and defaults, then 1.0 releases of every package, after which breaking changes wait for 2.0.
* **Encryption guidance.** Clear documentation of key storage, what happens when a key is lost, and which stored fields stay readable.
* **A DevTools panel.** A `durable_queue_devtools` extension that shows tasks, their failures, progress, and metrics while you debug, with payloads hidden by default.

## Considered

These wait until users ask for them:

* **Several runners on one storage at once**, with a lease per task. Today `durable_queue_workmanager`'s lease lets the app and a background job take turns, which covers the common case.
* **Adaptive concurrency** that follows failure rates or the network, after the fixed per-type limits have been in use for a while.
* **More storage adapters.** Ask for the database you use.

## Not planned

* **Exactly-once execution.** The queue runs each task at least once; handlers send an idempotency key so a server can drop a repeat. See [what durable_queue guarantees](docs/guarantees.md).
