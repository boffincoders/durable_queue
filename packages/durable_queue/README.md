# durable_queue

Persistent, retryable task execution for Dart and Flutter.

## The problem

Apps constantly start work that must not be lost: uploading a photo, sending a message, syncing an edit. The first attempt often fails because the network drops, the server returns 503, or the user closes the app mid-request. A `try`/`catch` around the call handles one failure, but then you have to decide:

* where the work lives until it can be retried, and whether it survives a restart;
* how long to wait between attempts, and when to give up;
* how to stop a double tap from uploading the same photo twice;
* how many uploads may run at once, and which ones go first;
* what happens to step 3 when step 2 fails.

`durable_queue` answers those questions in one small, framework-free engine. Work is described as data (a serializable task), stored through a pluggable storage interface, and executed by a worker that owns retries, concurrency, ordering, and recovery.

## Features

* Tasks are data, so they can be stored and rebuilt after a restart
* Fixed and exponential retry with optional jitter, and a `retryIf` filter
* Concurrency limits and priorities
* Scheduled tasks: start at a given time or after a delay
* Rate limits per task type, or per key such as an API host
* Gates: tasks that need the network wait while offline, without using attempts
* Task dependencies, chains, and groups
* Deduplication and idempotency keys
* Cooperative cancellation, pause, resume, and stop
* `drain`: run what is ready within a time limit, for background jobs
* Lifecycle events and queries by status or group
* Startup recovery of tasks interrupted by a crash
* An injectable clock, so retry logic is testable without waiting
* Pure Dart: no HTTP client, database, Flutter, or state-management dependency

## Install

```yaml
dependencies:
  durable_queue: ^0.5.3
```

## Packages

`durable_queue` is the core: pure Dart, with no database, HTTP client, or Flutter dependency. Integrations are separate packages from the same [repository](https://github.com/boffincoders/durable_queue), so you add only what you use.

| Package | What it adds |
|---|---|
| [`durable_queue`](https://pub.dev/packages/durable_queue) | The engine: tasks, retries, priorities, dependencies, rate limits, gates. |
| [`durable_queue_sqlite`](https://pub.dev/packages/durable_queue_sqlite) | SQLite storage that survives restarts, on Android, iOS, macOS, Windows, Linux, and the web. |
| [`durable_queue_dio`](https://pub.dev/packages/durable_queue_dio) | Dio integration: retry rules for HTTP errors, `Retry-After`, idempotency headers, cancellation, queued HTTP requests. |
| [`durable_queue_flutter`](https://pub.dev/packages/durable_queue_flutter) | Flutter integration: start the queue with the app, pause it in the background, and rebuild widgets from stored tasks. |
| [`durable_queue_connectivity`](https://pub.dev/packages/durable_queue_connectivity) | A network gate for Flutter on `connectivity_plus`, with an optional reachability check against your server. |

Need an integration that is not listed? Ask in [Discussions](https://github.com/boffincoders/durable_queue/discussions).

## Quick start

```dart
import 'package:durable_queue/durable_queue.dart';

final class UploadPhotoTask extends DurableTask {
  UploadPhotoTask({required this.path});

  final String path;

  @override
  String get type => 'upload_photo';

  @override
  Map<String, dynamic> toJson() => {'path': path};

  static UploadPhotoTask fromJson(Map<String, dynamic> json) =>
      UploadPhotoTask(path: json['path'] as String);
}

Future<void> main() async {
  final queue = DurableQueue(
    storage: MemoryQueueStorage(), // use a persistent adapter in an app
    maxConcurrentTasks: 3,
  );

  queue.register<UploadPhotoTask>(
    type: 'upload_photo',
    decoder: UploadPhotoTask.fromJson,
    handler: (task, context) async {
      await photoApi.upload(task.path); // your code
    },
  );

  await queue.start();

  await queue.enqueue(
    UploadPhotoTask(path: '/storage/avatar.jpg'),
    retryPolicy: RetryPolicy.exponential(
      maxAttempts: 5,
      initialDelay: Duration(seconds: 1),
    ),
  );
}
```

`enqueue` returns once the task is stored. The worker runs it in the background of your process and retries it if the handler throws.

## How the packages fit together

The core engine decides *when* each task runs. Everything else plugs into one of three slots: a **gate** decides whether a task may start now, **storage** decides where tasks live, and a **handler** does the actual work. `durable_queue_flutter` connects the queue to your app's lifecycle and UI.

```text
                    Your Flutter or Dart app
         enqueue tasks · show status · react to results
                               │
  durable_queue_flutter ──────▶│  starts the queue with the app,
  (optional)                   │  pauses it in the background,
                               │  rebuilds widgets from tasks
                               ▼
 ┌─────────────────────────────────────────────────────────────┐
 │                 durable_queue · core engine                 │
 │  retries · priorities · dependencies · rate limits · events │
 ├───────────────────┬───────────────────┬─────────────────────┤
 │ Gates             │ Storage           │ Handlers            │
 │ may a task start? │ where tasks live  │ what a task does    │
 └─────────▲─────────┴─────────▲─────────┴──────────▲──────────┘
           │                   │                    │
           │                   │                    │
 durable_queue_        durable_queue_      durable_queue_dio
 connectivity          sqlite              HTTP requests, retry
 waits while offline   survives restarts   rules, Retry-After

 built in:             built in:           built in:
 ManualQueueGate       MemoryQueueStorage  any async function
```

Every package is optional. Start with the core and its built-in pieces, then swap in a package for the slot you need: SQLite when tasks must survive restarts, the connectivity gate when work needs the network, Dio for HTTP. Your own gates, storage adapters, and handlers fit the same slots.

## How it works

```text
            Your app (Flutter or Dart)
                       │
         enqueue(task, retry, priority, dependsOn, group)
                       │
                       ▼
           ┌───────────────────────┐
           │     QueueStorage      │  memory, file, SQLite, ...
           │ pending · waiting ·   │  (survives restarts with a
           │ retryScheduled · ...  │   persistent adapter)
           └───────────┬───────────┘
                       │  worker claims the next ready task:
                       │  • dependencies completed
                       │  • retry time reached
                       │  • highest priority, then oldest
                       │  • a concurrency slot is free
                       ▼
           ┌───────────────────────┐
           │   your task handler   │
           └───────────┬───────────┘
              returns  │  throws
          ┌────────────┴──────────────┐
          ▼                           ▼
     completed              retryIf + RetryPolicy
          │                  ┌────────┴─────────┐
          │                  ▼                  ▼
          │           retryScheduled         failed
          │          (backoff, then           (or cancelled
          │           claimed again)           if requested)
          │                                     │
          └────────────────┬────────────────────┘
                           ▼
          tasks waiting on it are released, cancelled,
          failed, or run, per their DependencyFailurePolicy
```

Every state change is written to storage before the matching event is emitted. On the next launch, `start` recovers tasks that were left `running` when the process died.

## Real-world use cases

* **Media upload.** Retry network failures with backoff, fail fast on a rejected file, and send an idempotency key so the server never stores a photo twice. See `example/image_upload.dart`.
* **Offline-first edits.** Queue each change while offline, deduplicate repeated saves of the same record, and let the queue drain when connectivity returns.
* **Multi-step workflows.** Upload, then process, then notify, as a chain where a failed step cancels the rest. See `example/orchestration.dart`.
* **Background sync.** Group related sync tasks so they can be listed or cancelled together on logout.
* **Priority work.** Let a chat message jump ahead of analytics uploads that are already waiting.

## Tasks

A task is a JSON payload plus a type name, like `UploadPhotoTask` above. The queue stores `toJson()`, never a closure, and rebuilds the task with the registered decoder. Register each type before enqueuing it; the `type` passed to `register` must match the task's `type` getter.

The handler's `context` carries the task id, attempt number, attempt budget, idempotency key, and whether cancellation was requested. Handlers that do not need it can ignore it.

## Retry

```dart
RetryPolicy.none();

RetryPolicy.fixed(
  maxAttempts: 3,
  delay: Duration(seconds: 2),
);

RetryPolicy.exponential(
  maxAttempts: 5,
  initialDelay: Duration(seconds: 1),
  maxDelay: Duration(minutes: 1),
  jitter: true,
);
```

`maxAttempts` includes the first run. `retryIf` chooses which errors are eligible:

```dart
queue.register<UploadPhotoTask>(
  type: 'upload_photo',
  decoder: UploadPhotoTask.fromJson,
  handler: upload,
  retryIf: (error, stackTrace) => error is TemporaryUploadException,
);
```

The default retries handler errors until the attempt budget is spent. Decode failures are not retried.

When the error itself says how long to wait, such as an HTTP 429 or 503 with a `Retry-After` header, return that wait from `retryDelay`. It replaces the policy's delay for that retry; return `null` to let the policy decide:

```dart
queue.register<UploadPhotoTask>(
  type: 'upload_photo',
  decoder: UploadPhotoTask.fromJson,
  handler: upload,
  retryDelay: (error, stackTrace) =>
      error is RateLimitedException ? error.retryAfter : null,
);
```

The attempt still counts toward `maxAttempts`. Negative waits count as zero, and waits are capped at `RetryPolicy.maxSchedulableDelay`.

The core does not speak HTTP. [`durable_queue_dio`](https://pub.dev/packages/durable_queue_dio) provides ready-made `retryIf` and `retryDelay` rules for Dio, including `Retry-After`, plus a `deferOn` rule for network gates and per-host rate limits for queued requests.

## Deduplication and idempotency

```dart
await queue.enqueue(
  SyncUserTask(userId: '123'),
  deduplicationKey: 'sync-user:123',
  idempotencyKey: '123',
);
```

If a task with that deduplication key is already `pending`, `waiting`, `running`, or `retryScheduled`, `enqueue` returns the existing id and leaves it unchanged. A completed, failed, or cancelled task does not block the key.

The idempotency key is stored and exposed on `TaskContext`. The handler decides how to send it. The same key does not, by itself, collapse two tasks.

## Scheduled tasks

Give a task an earliest start time with `runAt` or `delay`:

```dart
await queue.enqueue(SendReminderTask(id), runAt: DateTime(2026, 11, 1, 9));
await queue.enqueue(SyncTask(), delay: Duration(minutes: 15));
```

The task is stored at once and starts when its time comes and a slot is free. While it waits it is `pending`, counts as active, and holds its deduplication key. A time in the past means "now". With `dependsOn`, the task starts once its dependencies have completed and its time has come. `enqueueChain` applies the time to the first step.

The queue only runs while your app runs: a task scheduled for 3 a.m. starts at 3 a.m. if the app is running then, or on the next launch otherwise.

## Priorities

```dart
await queue.enqueue(SendMessageTask(text: 'hi'), priority: 10);
await queue.enqueue(UploadLogsTask(), priority: -5);
```

Among tasks that are ready to start, higher `priority` goes first; equal priorities keep enqueue order. The default is `0`. Priority decides which task gets the next free slot. It never interrupts a handler that is already running.

## Dependencies and chains

A task can wait for other tasks:

```dart
final upload = await queue.enqueue(UploadPhotoTask(path: path));
final resize = await queue.enqueue(
  ResizePhotoTask(path: path),
  dependsOn: [upload],
);
```

Until every dependency is `completed`, the dependent task is `waiting` and does not use a concurrency slot. If a dependency fails, is cancelled, or no longer exists, `onDependencyFailure` decides what happens:

| `DependencyFailurePolicy` | Effect on the waiting task |
|---|---|
| `cancel` (default) | Becomes `cancelled` without running. |
| `fail` | Becomes `failed` without running. |
| `run` | Runs anyway once every dependency has finished. Good for cleanup. |

The outcome cascades: if `resize` is cancelled, tasks waiting on `resize` follow their own policy. The task's `lastFailure` names the dependency that blocked it.

`enqueueChain` stores steps that run one after another, even when `maxConcurrentTasks` is greater than one:

```dart
final ids = await queue.enqueueChain([
  UploadPhotoTask(path: path),
  ResizePhotoTask(path: path),
  NotifyFriendsTask(path: path),
], group: 'photo:$path');
```

With the default policy, a failed step cancels the rest of the chain. Dependencies must already be stored when you enqueue, so cycles cannot be created.

## Groups

```dart
await queue.enqueue(SyncContactsTask(), group: 'sync');
await queue.enqueue(SyncCalendarTask(), group: 'sync');

final syncTasks = await queue.getTasks(group: 'sync');
final cancelled = await queue.cancelGroup('sync');
```

A group is a label. `cancelGroup` cancels every active task in it, using the same rules as `cancel`, and returns how many tasks it affected.

## Rate limits

Limit how often tasks start. Every attempt counts, retries included, because each one reaches the service you are protecting.

```dart
// At most 10 uploads per minute.
queue.register<UploadPhotoTask>(
  type: 'upload_photo',
  decoder: UploadPhotoTask.fromJson,
  handler: upload,
  rateLimit: RateLimit(10, per: Duration(minutes: 1)),
);

// At most 5 requests per second to each API host, across task types.
queue.addRateLimit(
  'per-host',
  RateLimit(5, per: Duration(seconds: 1)),
  types: {'upload_photo', 'send_message'},
  keyOf: (task) => Uri.parse(task.payload['url'] as String).host,
);
```

Up to `count` tasks may start back to back, then starts are spaced by `per / count`. A task that has to wait is deferred: it keeps its status, attempt count, and place in line, and gets a reserved slot, so it is rescheduled once rather than polled. Other task types keep running meanwhile. A task must satisfy every limit that applies to it. Limits live in memory and start empty after a restart.

When a service asks you to slow down, hold a limit so every task in that bucket waits, not only the one that got the answer:

```dart
queue.holdRateLimit('per-host', key: 'api.example.com', duration: Duration(seconds: 30));
```

After the hold, starts resume at the steady rate. `durable_queue_dio` does this for you when a server answers 429 or 503 with `Retry-After`.

## Gates: waiting for the network and other conditions

A gate is a condition a task type needs before it starts, such as network access. While a required gate is closed, matching tasks are deferred without using attempts, other work keeps running, and the deferred tasks start as soon as the gate opens.

```dart
final network = ManualQueueGate();
queue.addGate('network', network); // add gates before registering types that need them

queue.register<UploadPhotoTask>(
  type: 'upload_photo',
  decoder: UploadPhotoTask.fromJson,
  handler: upload,
  requires: {'network'},
  // A connection error means "offline", not "this upload failed".
  deferOn: (error, stackTrace) =>
      error is SocketException ? 'network' : null,
);

// Open and close it from whatever tracks connectivity in your app:
connectivity.onConnectivityChanged.listen((results) {
  network.set(!results.contains(ConnectivityResult.none));
});
```

* Closing a gate does not cancel tasks that are already running.
* `deferOn` maps a handler error to one of the type's gates. The task is deferred instead of failing, the attempt does not count, and the gate's `recheck` is called. The task tries again after `gateErrorDelay` (default 30 seconds), or as soon as the gate closes and reopens.
* A gate that never reports opening is checked again every `gateRecheckInterval` (default five minutes).
* Implement `QueueGate` for other conditions, such as Wi-Fi only, charging, or signed in.

A connection-type report (Wi-Fi, mobile) does not prove the internet is reachable, for example behind a captive portal. `deferOn` covers that case: the request fails, the task waits, and the gate can probe.

On Flutter, [`durable_queue_connectivity`](https://pub.dev/packages/durable_queue_connectivity) provides this gate ready-made: `ConnectivityGate.create()` on `connectivity_plus`, an optional `HttpProbe` against your own server, and `deferOnConnectionError('network')`.

## Cancellation and controls

```dart
await queue.cancel(taskId);
await queue.pause();
await queue.resume();
await queue.stop();
```

Cancelling queued or waiting work marks it `cancelled` immediately. Cancelling a running task does not abort the future. The handler can check `context.isCancellationRequested`, or wait on `context.whenCancellationRequested`, and throw `TaskCancelledException` to stop. If it returns normally, the task is completed.

`whenCancellationRequested` completes the moment the task is cancelled, so it can abort work that accepts a cancel signal:

```dart
handler: (task, context) async {
  final token = CancelToken();
  context.whenCancellationRequested.then((_) => token.cancel());
  await dio.post(task.url, data: task.body, cancelToken: token);
},
```

`pause` finishes handlers that have already started and does not start new ones. `stop` waits for those handlers and leaves everything else stored.

## Retry, delete, and purge finished tasks

Completed, failed, and cancelled tasks stay in storage until you remove them.

```dart
// Run a failed or cancelled task again with a fresh attempt budget.
await queue.retry(taskId);

// Remove one finished task.
await queue.delete(taskId);

// Remove completed tasks older than a week. Returns the number removed.
final removed = await queue.purge(
  statuses: {TaskStatus.completed},
  olderThan: Duration(days: 7),
);
```

Failed tasks are the queue's dead-letter queue: they stay stored, with their last error in `lastFailure`, until you retry, delete, or purge them. To retry them all at once, for example after a server outage:

```dart
final retried = await queue.retryFailed(); // or retryFailed(group: 'uploads')
```

`retry` keeps the task id, payload, keys, priority, group, and dependencies, and can take a new `retryPolicy`. It refuses tasks that are still active, refuses when another active task already holds the same deduplication key, and refuses when a dependency did not complete (retry that dependency first). Tasks cancelled because a dependency failed are not revived automatically. `delete` and `purge` only touch terminal tasks, and never remove a task that a waiting task still depends on.

When the queue is no longer needed, `close` stops it and closes the `events` and `storageErrors` streams:

```dart
await queue.close();
```

## Running in a limited time window

`drain` starts the queue, runs what is ready, and stops when nothing is left to start or a time limit passes. Use it where the app gets a short window to work, such as a background job started by the operating system:

```dart
final result = await queue.drain(timeLimit: Duration(seconds: 25));
print('${result.completed} done, ${result.failed} failed, stopped: ${result.reason.name}');
// result.nextWakeAt: when the next scheduled task, retry, or deferral is due.
```

* Tasks scheduled for later, retries, and tasks held by a gate or rate limit do not keep the drain going. Pass `waitForScheduled: true` to also wait for timed work due before the limit.
* At the limit no new task starts; tasks already running finish first, so leave headroom for your longest task.
* The queue must be idle when `drain` is called, and is idle again when it returns.

## Observation

```dart
queue.events.listen((event) {
  // TaskEnqueued (with runAt for scheduled tasks), TaskStarted,
  // TaskRetryScheduled, TaskDeferred,
  // TaskCompleted, TaskFailed, TaskCancelled
});

final failed = await queue.getTasks(status: TaskStatus.failed);
```

`TaskDeferred` says why a task waits (`rateLimit`, `gateClosed`, or `gateError`), which limit or gate is responsible, and until when.

The event stream is broadcast and does not replay events that happened before the listener subscribed.

Worker storage errors are reported as values on `queue.storageErrors` and retried after `storageRetryDelay` (default one second). A failed result write retries persistence without running the handler again. Storage outages hold serialized operations, so enqueue, queries, cancellation, pause, and stop can wait until storage recovers. Failed startup recovery leaves the queue idle and allows another `start()` call.

## Using it in Flutter

The core package does not depend on Flutter. [`durable_queue_flutter`](https://pub.dev/packages/durable_queue_flutter) connects a queue to the app:

```dart
Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final queue = DurableQueue(storage: await openStorage()); // e.g. durable_queue_sqlite
  queue.register<UploadPhotoTask>(
    type: 'upload_photo',
    decoder: UploadPhotoTask.fromJson,
    handler: (task, context) => api.upload(task.path),
  );
  // Starts the queue, pauses it in the background, resumes in the foreground.
  runApp(DurableQueueScope(queue: queue, child: const MyApp()));
}

// Anywhere in the UI:
QueueTaskBuilder(
  taskId: uploadId,
  builder: (context, task) => Text(task?.status.name ?? '...'),
);
```

Without that package, create the queue the same way, call `await queue.start()` before `runApp`, and build UI from `queue.events`, `getTask`, and `getTasks`.

The queue runs while your app's process runs. It does not wake a terminated app; it starts again on the next launch.

## Storage

The engine talks to storage only through `QueueStorage`, so you choose where tasks live.

| Storage | Survives restart | Status |
|---|---|---|
| `MemoryQueueStorage` | No | Included. For tests and process-lifetime queues. |
| `JsonFileStorage` | Yes | Example in `example/json_file_storage.dart`, checked against the full storage contract. Rewrites one file per change, so it suits small queues. |
| [`durable_queue_sqlite`](https://pub.dev/packages/durable_queue_sqlite) | Yes | Separate package. Indexed, transactional SQLite storage for Android, iOS, macOS, Windows, Linux, and the web. Use 0.2.1 or later with `durable_queue` 0.5. |
| Hive, Isar, others | Yes | Adapters are welcome as separate packages; say what you need in Discussions. |

Adapters stay in separate packages so the core never forces a database on you.

To write an adapter, implement `QueueStorage` and run the shared contract tests against it:

```dart
final class MyStorage implements QueueStorage {
  // save, get, update, delete, getNextReady, getWaitingDependents, ...
}

// test/my_storage_test.dart
void main() => queueStorageContractTests(MyStorage.new);
```

`queueStorageContractTests` lives in [`storage_contract.dart`](https://github.com/boffincoders/durable_queue/blob/main/packages/durable_queue_test_support/lib/src/storage_contract.dart) in the repository's test-support package; copy it into your adapter's tests. [STORAGE.md](STORAGE.md) lists every method, ordering rule, and atomicity requirement.

## When to use durable_queue

| | Manual `try`/`catch` + retry loop | Simple in-memory queue | durable_queue | OS background scheduler (e.g. Android WorkManager) |
|---|---|---|---|---|
| Work survives an app restart | No | No | Yes, with a persistent storage adapter | Yes |
| Runs while the app is terminated | No | No | No | Yes, within OS limits |
| Retry with backoff and a retry filter | You write it | Usually you write it | Built in | Varies by platform |
| Deduplication and idempotency keys | You write it | Usually you write it | Built in | Varies by platform |
| Dependencies, chains, priorities | You write it | Rarely | Built in | Varies by platform |
| Rate limits, waiting for the network | You write it | Rarely | Built in | Network constraints only |
| Testable without real waiting | Depends on your code | Depends | Yes (`FakeQueueClock`) | Limited |
| Platforms | Any | Any | Any Dart platform | One platform per API |

Use durable_queue when work must outlive a failed request or an app restart, and the app is allowed to finish it the next time it runs. Use an OS scheduler when the work must run while the app is closed; the two can be combined, with the OS job calling `start`. A plain `try`/`catch` is enough for work that is fine to lose. This table compares capabilities only; no performance benchmarks have been published.

## Guarantees

Execution is **at least once**. A crash after a side effect but before the completion write can run the task again.

`start` recovers a task left in `running`: that attempt counts, then the retry policy either schedules another try or fails the task. Handlers should be idempotent when a repeated side effect would be harmful.

When a task finishes, the tasks waiting on it are updated before its own result is written. A crash in between never leaves a task waiting forever behind a dependency that already finished. The worst case is that dependents were released or cancelled for an outcome that recovery then records again.

Persistence is not background execution. A terminated app stays terminated until something else launches it. After the next launch, call `start` again.

Do not put secrets in task payloads or exception text. The queue stores them as plain data. `MemoryQueueStorage` does not encrypt anything.

## Examples

```sh
dart run example/main.dart           # one task, one retry
dart run example/image_upload.dart   # uploads that survive a restart
dart run example/orchestration.dart  # chains, dependencies, priorities
dart run example/rate_limits_and_gates.dart  # rate limits, offline gate
```

## Contributing and feedback

* Questions, feature ideas, and storage-integration requests: [GitHub Discussions](https://github.com/boffincoders/durable_queue/discussions).
* Bugs: [issue tracker](https://github.com/boffincoders/durable_queue/issues).
* Code and adapters: see [CONTRIBUTING.md](https://github.com/boffincoders/durable_queue/blob/main/CONTRIBUTING.md).
