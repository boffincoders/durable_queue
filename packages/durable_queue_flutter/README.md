# durable_queue_flutter

Flutter integration for [`durable_queue`](https://pub.dev/packages/durable_queue): start the queue with the app, pause it while the app is in the background, and rebuild widgets from stored tasks.

## Install

```yaml
dependencies:
  durable_queue: ^0.5.0
  durable_queue_flutter: ^0.1.1
```

Requires Flutter 3.13 or later.

## Start the queue with the app

```dart
Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final queue = DurableQueue(storage: await openStorage()); // e.g. durable_queue_sqlite
  queue.register<UploadTask>(
    type: 'upload',
    decoder: UploadTask.fromJson,
    handler: upload,
  );

  runApp(DurableQueueScope(queue: queue, child: const MyApp()));
}

// Anywhere below the scope:
final queue = DurableQueueScope.of(context);
await queue.enqueue(UploadTask(path));
```

`DurableQueueScope`:

* starts the queue when it is inserted (startup recovery runs then);
* pauses the queue while the app is hidden or paused, and resumes it when the app is back in the foreground (`pauseInBackground`, default true);
* never resumes a queue that your app paused itself;
* when removed, stops following the lifecycle, and closes the queue if `closeOnDispose` is true.

With `runQueue: false` the scope only provides the queue to the widgets below it and leaves starting and pausing to something else, such as `LeasedQueueRunner` from [`durable_queue_workmanager`](https://pub.dev/packages/durable_queue_workmanager).

Startup errors, such as a failed recovery, go to `onStartError`, or to `FlutterError.reportError` when it is not set.

Without a widget, use `DurableQueueLifecycle.attach(queue)` and `dispose()` it when done.

### Why pause in the background?

Mobile operating systems suspend or kill apps in the background, often within seconds. A task started then can be cut off mid-attempt and, after the next launch, is counted as an interrupted attempt. Pausing stops new tasks from starting while the app is hidden; tasks that already started keep running. Set `pauseInBackground: false` if your tasks are short or your app keeps running in the background.

The queue runs while your app runs. This package does not wake a closed app; [`durable_queue_workmanager`](https://pub.dev/packages/durable_queue_workmanager) runs the queue from background jobs.

## Show task status

```dart
QueueTaskBuilder(
  taskId: uploadId,
  builder: (context, task) => switch (task?.status) {
    null => const Text('…'),
    TaskStatus.completed => const Text('Uploaded'),
    TaskStatus.failed => Text('Failed: ${task!.lastFailure?.error}'),
    final TaskStatus status => Text(status.name),
  },
)

QueueGroupBuilder(
  group: 'album:holiday',
  builder: (context, tasks) {
    final done = tasks.where((t) => t.status == TaskStatus.completed).length;
    return Text('$done of ${tasks.length} uploaded');
  },
)
```

Both rebuild when a task they show changes. They use the streams `queue.watchTask(id)` and `queue.watchGroup(group, status:)`, which you can also use directly, for example with your own state management. `watchGroup` reloads the group on changes, so keep groups to the size of a screen.

## With other durable_queue packages

* [`durable_queue_sqlite`](https://pub.dev/packages/durable_queue_sqlite): storage that survives restarts, on mobile, desktop, and the web.
* [`durable_queue_connectivity`](https://pub.dev/packages/durable_queue_connectivity): network tasks wait while offline and start when connectivity returns.
* [`durable_queue_dio`](https://pub.dev/packages/durable_queue_dio): HTTP retry rules, `Retry-After`, and queued requests.
* [`durable_queue_workmanager`](https://pub.dev/packages/durable_queue_workmanager): run the queue from Workmanager background jobs on Android and iOS.
* [`durable_queue_transfer`](https://pub.dev/packages/durable_queue_transfer): uploads and downloads that keep going after the app is closed.

## Contributing

Issues and pull requests: [github.com/boffincoders/durable_queue](https://github.com/boffincoders/durable_queue). See [CONTRIBUTING.md](https://github.com/boffincoders/durable_queue/blob/main/CONTRIBUTING.md).
