# durable_queue cookbook

New to durable_queue? Start with the [quick start](quickstart.md).

Recipes for common jobs. Each one is a complete file in [`examples/cookbook`](../examples/cookbook) or, for recipes that use Firebase, Supabase, Riverpod, or Bloc, [`examples/cookbook_stacks`](../examples/cookbook_stacks). CI analyses both on every push against the code in this repository, so the recipes keep compiling as the packages change. For what the queue promises in each case, see [Guarantees](guarantees.md).

| Recipe | Packages |
| --- | --- |
| [Submit a form now, or when back online](#submit-a-form-now-or-when-back-online) | core, dio, connectivity |
| [Send chat messages in order](#send-chat-messages-in-order) | core, dio |
| [Upload a file, then tell your server](#upload-a-file-then-tell-your-server) | core, transfer, dio |
| [Sync with a rate-limited API](#sync-with-a-rate-limited-api) | core, dio |
| [Keep working in the background](#keep-working-in-the-background) | core, sqlite, workmanager |
| [Show progress in the UI](#show-progress-in-the-ui) | core, flutter |
| [Handle work that failed for good](#handle-work-that-failed-for-good) | core |
| [Test retries without waiting](#test-retries-without-waiting) | core |
| [Upload to Firebase Storage](#upload-to-firebase-storage) | core, `firebase_storage` |
| [Sync records to Supabase](#sync-records-to-supabase) | core, `supabase_flutter` |
| [Use the queue with Riverpod](#use-the-queue-with-riverpod) | core, flutter, `flutter_riverpod` |
| [Use the queue with Bloc](#use-the-queue-with-bloc) | core, flutter, `flutter_bloc` |

In every recipe, open the queue on persistent storage (`durable_queue_sqlite`) in a real app, register task types before `queue.start()`, and keep secrets out of task payloads.

## Submit a form now, or when back online

[`offline_form.dart`](../examples/cookbook/lib/offline_form.dart)

The request is stored as data (`HttpRequestTask`) before `enqueue` returns. While the device is offline it waits without using attempts, and it goes out as soon as the network is back.

```dart
queue.addGate('network', await ConnectivityGate.create());
queue.registerHttpRequests(
  api,
  requires: {'network'},
  deferOn: const DioRetryRules().deferOn('network'),
);

await queue.enqueue(
  HttpRequestTask(method: 'POST', url: '/feedback', body: answers),
  deduplicationKey: 'feedback:$formId', // a second tap adds nothing
  idempotencyKey: formId, // sent as Idempotency-Key
);
```

The deduplication key stops a double tap from queuing the form twice. The idempotency key lets your server drop a repeat if the app dies after sending but before recording success.

## Send chat messages in order

[`ordered_chat.dart`](../examples/cookbook/lib/ordered_chat.dart)

Each message depends on the previous unsent message in its conversation, so they arrive in the order written, across restarts and offline periods, and one slow conversation does not hold up the others.

```dart
final pending = [
  for (final task in await queue.getTasks(group: group))
    if (task.status.isActive) task,
];
await queue.enqueue(
  HttpRequestTask(method: 'POST', url: '/conversations/$id/messages', body: message),
  group: group,
  dependsOn: [if (pending.isNotEmpty) pending.last.id],
  onDependencyFailure: DependencyFailurePolicy.run,
  deduplicationKey: 'message:$clientMessageId',
  idempotencyKey: clientMessageId,
);
```

Call `send` for one conversation one at a time (await it), so each message sees the one before it. With `DependencyFailurePolicy.run`, a message that fails for good does not block the rest; use `cancel` if later messages must not go out without it.

## Upload a file, then tell your server

[`upload_then_notify.dart`](../examples/cookbook/lib/upload_then_notify.dart) · full app: [`examples/durable_queue_demo`](../examples/durable_queue_demo)

The operating system runs the upload, so it continues if the user closes the app. The request after it runs only once the upload succeeded.

```dart
handler: (task, context) async {
  await transfers.start(
    context,
    UploadTask(
      url: 'https://api.example.com/files',
      filename: task.fileName,
      baseDirectory: BaseDirectory.applicationSupport,
      headers: {'Idempotency-Key': context.taskId},
    ),
  );
},

await queue.enqueueChain([
  UploadFile(fileName),
  HttpRequestTask(method: 'POST', url: '/files/$fileName/uploaded'),
]);
```

Call `transfers.attach()` once per launch, so uploads that finished while the app was closed are recorded. Uploads must come from a file on disk.

## Sync with a rate-limited API

[`rate_limited_sync.dart`](../examples/cookbook/lib/rate_limited_sync.dart)

The task stores only the record id and reads the latest data when it runs, so edits made while it waits are not lost. The type is limited to 10 starts per second, and a 429 with `Retry-After` pauses every queued sync, not only the one that got it.

```dart
queue.register<SyncRecord>(
  type: SyncRecord.taskType,
  decoder: SyncRecord.fromJson,
  rateLimit: RateLimit(10, per: const Duration(seconds: 1)),
  retryIf: rules.shouldRetry,
  retryDelay: rules.retryDelay,
  handler: (task, context) async {
    final record = await readRecord(task.recordId);
    try {
      await api.put<void>('/records/${task.recordId}', data: record);
    } on DioException catch (error) {
      final wait = parseRetryAfter(error.response?.headers.value('retry-after'));
      if (error.response?.statusCode == 429 && wait != null) {
        queue.holdRateLimit('type:${SyncRecord.taskType}', duration: wait);
      }
      rethrow;
    }
  },
);
```

For queued `HttpRequestTask`s, `queue.addHttpHostRateLimit(...)` does the same per host without code in a handler.

## Keep working in the background

[`background_sync.dart`](../examples/cookbook/lib/background_sync.dart)

Workmanager runs the queue in background jobs on Android and iOS. The app and the job build the queue the same way and share a lease file, so they never run it at the same time.

```dart
const background = QueueBackgroundWork(
  openQueue: openQueue,
  openLease: openLease,
  taskName: 'com.example.app.queue',
);

@pragma('vm:entry-point')
void callbackDispatcher() {
  Workmanager().executeTask((taskName, inputData) async {
    if (background.handles(taskName)) return background.run(taskName);
    return true;
  });
}

// In main:
await Workmanager().initialize(callbackDispatcher);
await background.schedule();
LeasedQueueLifecycle.attach(LeasedQueueRunner(queue, await openLease()));
```

Background jobs run when the operating system decides, briefly (about 30 seconds on iOS). Use them for work that can wait; use the transfer recipe for uploads and downloads. Setup for iOS: see the [`durable_queue_workmanager` README](../packages/durable_queue_workmanager/README.md).

## Show progress in the UI

[`task_status_ui.dart`](../examples/cookbook/lib/task_status_ui.dart)

`QueueGroupBuilder` and `QueueTaskBuilder` rebuild when the tasks they show change.

```dart
QueueGroupBuilder(
  group: 'uploads',
  builder: (context, tasks) {
    final done = tasks.where((t) => t.status == TaskStatus.completed);
    return Text('${done.length} of ${tasks.length} done');
  },
)
```

Put a `DurableQueueScope` above them, or pass `queue:`.

## Handle work that failed for good

[`failed_tasks.dart`](../examples/cookbook/lib/failed_tasks.dart)

Failed tasks stay stored with their last error: a dead-letter queue you can inspect and retry.

```dart
final failed = await queue.getTasks(status: TaskStatus.failed);
await queue.retryFailed(); // fresh attempt budget for each
await queue.purge(
  statuses: {TaskStatus.completed, TaskStatus.cancelled},
  olderThan: const Duration(days: 7),
);
```

## Test retries without waiting

[`testing_handlers_test.dart`](../examples/cookbook/test/testing_handlers_test.dart)

Give the queue a `FakeQueueClock` and move time yourself. `drain` runs what is ready and returns, which makes each step of a test explicit.

```dart
final clock = FakeQueueClock(DateTime.utc(2026, 1, 1));
final queue = DurableQueue(storage: MemoryQueueStorage(), clock: clock);
// ...register a handler that fails once, enqueue with a 30 s retry delay...

await queue.drain(); // first attempt fails
clock.advance(const Duration(seconds: 30));
final result = await queue.drain(); // the retry runs
expect(result.completed, 1);
```

## Upload to Firebase Storage

[`firebase_storage_upload.dart`](../examples/cookbook_stacks/lib/firebase_storage_upload.dart)

Each upload is a task with the file's path and its place in the bucket. It is retried across restarts, reports progress, and stops when the task is cancelled or held, so a user can pause it. Uploading to the same path again overwrites the object, so a repeated attempt adds no copy.

```dart
handler: (task, context) async {
  final file = File(task.localPath);
  if (!file.existsSync()) throw MissingUploadFile(task.localPath);

  final upload = storage.ref(task.storagePath).putFile(file);
  final progress = upload.snapshotEvents.listen((snapshot) {
    if (snapshot.totalBytes > 0) {
      context.reportProgress(snapshot.bytesTransferred / snapshot.totalBytes);
    }
  }, onError: (Object _) {});
  unawaited(context.whenCancellationRequested.then((_) => upload.cancel()));
  try {
    await upload;
  } finally {
    await progress.cancel();
  }
},
```

`maxConcurrent: 2` keeps two uploads going at a time, and `retryIf` stops retrying errors another attempt cannot fix, such as `unauthorized`. The upload runs in the app, so it stops while the app is closed and continues on the next launch. For uploads that must go on while the app is closed, upload to a signed URL with [`durable_queue_transfer`](../packages/durable_queue_transfer/README.md).

## Sync records to Supabase

[`supabase_sync.dart`](../examples/cookbook_stacks/lib/supabase_sync.dart)

Save the record in the app's own database first, then enqueue a task with only its id. When the task runs, it reads the latest version and upserts it, so edits made offline reach the server, and a repeated attempt writes the same row again.

```dart
handler: (task, context) async {
  final note = await loadNote(task.noteId);
  if (note == null) {
    await supabase.from('notes').delete().eq('id', task.noteId);
  } else {
    await supabase.from('notes').upsert(note);
  }
},
retryIf: (error, stackTrace) => error is! PostgrestException,
```

A `PostgrestException` means Supabase rejected the row, which another attempt will not change; network errors are retried. Add the network gate from [Submit a form now, or when back online](#submit-a-form-now-or-when-back-online) so syncs wait while offline without using attempts.

## Use the queue with Riverpod

[`riverpod_state.dart`](../examples/cookbook_stacks/lib/riverpod_state.dart)

Create the queue in `main()`, hand it to Riverpod with an override, and expose the queue's streams as providers. `DurableQueueScope` still starts the queue with the app.

```dart
final queueProvider = Provider<DurableQueue>(
  (ref) => throw UnimplementedError('Override queueProvider in main()'),
);

final uploadsProvider = StreamProvider<List<StoredTask>>(
  (ref) => ref.watch(queueProvider).watchGroup('uploads'),
);

final progressProvider = StreamProvider.family<TaskProgress?, String>(
  (ref, taskId) => ref.watch(queueProvider).watchProgress(taskId),
);

runApp(
  ProviderScope(
    overrides: [queueProvider.overrideWithValue(queue)],
    child: DurableQueueScope(queue: queue, child: app),
  ),
);
```

## Use the queue with Bloc

[`bloc_state.dart`](../examples/cookbook_stacks/lib/bloc_state.dart)

A cubit that follows one group, with actions the UI can call:

```dart
class QueueGroupCubit extends Cubit<List<StoredTask>> {
  QueueGroupCubit(this.queue, String group) : super(const []) {
    _subscription = queue.watchGroup(group).listen(emit);
  }

  final DurableQueue queue;
  late final StreamSubscription<List<StoredTask>> _subscription;

  Future<void> pause(String id) => queue.hold(id);
  Future<void> resume(String id) => queue.release(id);
  Future<void> retry(String id) => queue.retry(id);

  @override
  Future<void> close() async {
    await _subscription.cancel();
    return super.close();
  }
}
```

Use `BlocProvider` and `BlocBuilder<QueueGroupCubit, List<StoredTask>>` as usual; `QueueProgressBuilder` adds a progress bar per task.

