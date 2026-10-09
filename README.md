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
* Task dependencies, chains, and groups
* Deduplication and idempotency keys
* Cooperative cancellation, pause, resume, and stop
* Lifecycle events and queries by status or group
* Startup recovery of tasks interrupted by a crash
* An injectable clock, so retry logic is testable without waiting
* Pure Dart: no HTTP client, database, Flutter, or state-management dependency

## Install

```yaml
dependencies:
  durable_queue: ^0.3.0
```

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

The default retries handler errors until the attempt budget is spent. Decode failures are not retried. This package does not inspect HTTP status codes.

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

## Cancellation and controls

```dart
await queue.cancel(taskId);
await queue.pause();
await queue.resume();
await queue.stop();
```

Cancelling queued or waiting work marks it `cancelled` immediately. Cancelling a running task does not abort the future. The handler can read `context.isCancellationRequested` and throw `TaskCancelledException` to stop. If it returns normally, the task is completed.

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

`retry` keeps the task id, payload, keys, priority, group, and dependencies, and can take a new `retryPolicy`. It refuses tasks that are still active, refuses when another active task already holds the same deduplication key, and refuses when a dependency did not complete (retry that dependency first). Tasks cancelled because a dependency failed are not revived automatically. `delete` and `purge` only touch terminal tasks, and never remove a task that a waiting task still depends on.

When the queue is no longer needed, `close` stops it and closes the `events` and `storageErrors` streams:

```dart
await queue.close();
```

## Observation

```dart
queue.events.listen((event) {
  // TaskEnqueued, TaskStarted, TaskRetryScheduled,
  // TaskCompleted, TaskFailed, TaskCancelled
});

final failed = await queue.getTasks(status: TaskStatus.failed);
```

The event stream is broadcast and does not replay events that happened before the listener subscribed.

Worker storage errors are reported as values on `queue.storageErrors` and retried after `storageRetryDelay` (default one second). A failed result write retries persistence without running the handler again. Storage outages hold serialized operations, so enqueue, queries, cancellation, pause, and stop can wait until storage recovers. Failed startup recovery leaves the queue idle and allows another `start()` call.

## Using it in Flutter

The core package does not depend on Flutter. A typical app creates one queue at startup, starts it, and listens to its events:

```dart
late final DurableQueue queue;

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final dir = await getApplicationSupportDirectory(); // path_provider
  queue = DurableQueue(storage: JsonFileStorage('${dir.path}/queue.json'));
  queue.register<UploadPhotoTask>(
    type: 'upload_photo',
    decoder: UploadPhotoTask.fromJson,
    handler: (task, context) => api.upload(task.path),
  );
  await queue.start();
  runApp(const MyApp());
}

// Anywhere in the UI:
StreamBuilder<QueueEvent>(
  stream: queue.events,
  builder: (context, snapshot) => Text('${snapshot.data ?? 'idle'}'),
);
```

`JsonFileStorage` is the reference adapter from `example/json_file_storage.dart`; copy it into your app or write your own (see below). The queue runs while your app's process runs. It does not wake a terminated app; call `start` again on the next launch. A `durable_queue_flutter` package for app lifecycle integration is planned.

## Storage

The engine talks to storage only through `QueueStorage`, so you choose where tasks live.

| Storage | Survives restart | Status |
|---|---|---|
| `MemoryQueueStorage` | No | Included. For tests and process-lifetime queues. |
| `JsonFileStorage` | Yes | Example in `example/json_file_storage.dart`, checked against the full storage contract. Rewrites one file per change, so it suits small queues. |
| SQLite (`durable_queue_drift`) | Yes | Planned as a separate package. |
| Hive, Isar, others | Yes | Not planned yet. Adapters are welcome as separate packages; say what you need in Discussions. |

Adapters stay in separate packages so the core never forces a database on you.

To write an adapter, implement `QueueStorage` and run the shared contract tests against it:

```dart
final class MyStorage implements QueueStorage {
  // save, get, update, delete, getNextReady, getWaitingDependents, ...
}

// test/my_storage_test.dart
void main() => queueStorageContractTests(MyStorage.new);
```

`queueStorageContractTests` lives in this repository's `test/storage_contract.dart`; copy it into your adapter's tests. [STORAGE.md](STORAGE.md) lists every method, ordering rule, and atomicity requirement.

## When to use durable_queue

| | Manual `try`/`catch` + retry loop | Simple in-memory queue | durable_queue | OS background scheduler (e.g. Android WorkManager) |
|---|---|---|---|---|
| Work survives an app restart | No | No | Yes, with a persistent storage adapter | Yes |
| Runs while the app is terminated | No | No | No | Yes, within OS limits |
| Retry with backoff and a retry filter | You write it | Usually you write it | Built in | Varies by platform |
| Deduplication and idempotency keys | You write it | Usually you write it | Built in | Varies by platform |
| Dependencies, chains, priorities | You write it | Rarely | Built in | Varies by platform |
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
```

## Contributing and feedback

* Questions, feature ideas, and storage-integration requests: [GitHub Discussions](https://github.com/boffincoders/durable_queue/discussions).
* Bugs: [issue tracker](https://github.com/boffincoders/durable_queue/issues).
* Code and adapters: see [CONTRIBUTING.md](CONTRIBUTING.md).
