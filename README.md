# durable_queue

Persistent, retryable task execution for Dart and Flutter.

* Tasks are data, so they can be stored and rebuilt after a restart
* Fixed and exponential retry, with optional jitter
* Concurrency limits and priorities
* Task dependencies, chains, and groups
* Deduplication and idempotency metadata
* Cancellation
* A lifecycle event stream
* No HTTP client, database, or state-management dependency

```dart
final queue = DurableQueue(
  storage: MemoryQueueStorage(),
  maxConcurrentTasks: 3,
);

queue.register<UploadPhotoTask>(
  type: 'upload_photo',
  decoder: UploadPhotoTask.fromJson,
  handler: (task, context) async {
    await photoRepository.upload(task.path);
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
```

## Install

```yaml
dependencies:
  durable_queue: ^0.3.0
```

`MemoryQueueStorage` keeps tasks in the process. It is the right storage for tests and for work that does not need to survive a restart. A storage adapter implements `QueueStorage`; the core package does not ship a database.

## Tasks

A task is a JSON payload plus a type name. The queue cannot persist a closure.

```dart
final class UploadPhotoTask extends DurableTask {
  UploadPhotoTask({required this.path});

  final String path;

  @override
  String get type => 'upload_photo';

  @override
  Map<String, dynamic> toJson() => {'path': path};

  static UploadPhotoTask fromJson(Map<String, dynamic> json) {
    return UploadPhotoTask(path: json['path'] as String);
  }
}
```

Register the type before `enqueue`. `type` must match `UploadPhotoTask.type`.

`context` carries the task id, attempt count, idempotency key, and whether cancellation was requested. Handlers that do not care can ignore it.

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

If a task with that deduplication key is already `pending`, `running`, or `retryScheduled`, `enqueue` returns the existing id and leaves it unchanged. A completed, failed, or cancelled task does not block the key.

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

## Guarantees

Execution is **at least once**. A crash after a side effect but before the completion write can run the task again.

`start` recovers a task left in `running`: that attempt counts, then the retry policy either schedules another try or fails the task. Handlers should be idempotent when a repeated side effect would be harmful.

When a task finishes, the tasks waiting on it are updated before its own result is written. A crash in between never leaves a task waiting forever behind a dependency that already finished. The worst case is that dependents were released or cancelled for an outcome that recovery then records again.

Persistence is not background execution. A terminated app stays terminated until something else launches it. After the next launch, call `start` again.

Do not put secrets in task payloads or exception text. The queue stores them as plain data. `MemoryQueueStorage` does not encrypt anything.

Writing your own storage adapter? Follow the [storage contract](STORAGE.md).

## Example

```sh
dart run example/main.dart
dart run example/orchestration.dart
```
