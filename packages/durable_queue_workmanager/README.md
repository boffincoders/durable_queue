# durable_queue_workmanager

Run [`durable_queue`](https://pub.dev/packages/durable_queue) tasks in [Workmanager](https://pub.dev/packages/workmanager) background jobs on Android and iOS, so queued work keeps going after the user leaves the app.

The app and the background job open the same queue storage. A small lease makes them take turns: the app runs the queue while it is in the foreground, the background job runs it while the app is away, and a task is never started by both.

## Why a lease?

A starting `DurableQueue` treats every task still marked running as interrupted by a crash, and runs it again. If the background job started the queue while the app's queue was running a task, that task would run twice. On Android the job runs in a separate isolate of the same process, so in-process locks do not help either.

`durable_queue_workmanager` keeps one runner at a time:

* `LeasedQueueRunner` runs the app's queue only while it holds the lease. It stops the queue and releases the lease when the app goes to the background.
* `QueueBackgroundWork` takes the lease in the background job, drains the queue for a limited time, and gives the lease back. If the app holds it, the job does nothing.
* `FileQueueLease` keeps the lease in a file that both isolates see. A holder renews it while it runs; a lease that is not renewed in time (its holder was killed) can be taken over.

## Install

```yaml
dependencies:
  durable_queue: ^0.5.3
  durable_queue_sqlite: ^0.2.2
  durable_queue_workmanager: ^0.1.0
  workmanager: ^0.10.10
  path_provider: ^2.1.0
```

Requires Flutter 3.38 or later (as Workmanager does). Use storage that several isolates can open, such as `durable_queue_sqlite` 0.2.2 or later; `MemoryQueueStorage` is not shared between isolates.

Then follow Workmanager's [platform setup](https://docs.page/fluttercommunity/flutter_workmanager/quickstart):

* **Android:** no extra setup.
* **iOS:** enable the Background Modes capability with *Background fetch* and list the job name under `BGTaskSchedulerPermittedIdentifiers` in `Info.plist`. Use an identifier for your app, such as `com.example.app.queue`, and pass it as `taskName`.

## Usage

Build the queue and the lease the same way in the app and in the background job:

```dart
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:durable_queue_workmanager/durable_queue_workmanager.dart';
import 'package:path_provider/path_provider.dart';
import 'package:workmanager/workmanager.dart' hide TaskStatus;

Future<DurableQueue> openQueue() async {
  final dir = await getApplicationSupportDirectory();
  final queue = DurableQueue(
    storage: SqliteQueueStorage.open('${dir.path}/queue.db'),
  );
  queue.register<UploadTask>(
    type: 'upload',
    decoder: UploadTask.fromJson,
    handler: (task, context) => api.upload(task.path),
  );
  return queue;
}

Future<QueueLease> openLease() async {
  final dir = await getApplicationSupportDirectory();
  return FileQueueLease('${dir.path}/queue.lease');
}

final background = QueueBackgroundWork(
  openQueue: openQueue,
  openLease: openLease,
  taskName: 'com.example.app.queue', // must match Info.plist on iOS
  constraints: Constraints(networkType: NetworkType.connected),
);

@pragma('vm:entry-point')
void callbackDispatcher() {
  Workmanager().executeTask((taskName, inputData) async {
    if (background.handles(taskName)) return background.run(taskName);
    return true; // your other jobs
  });
}
```

`workmanager` exports its own `TaskStatus`, hence `hide TaskStatus` when you also use durable_queue's.

In `main`, register the job and run the app's queue through a `LeasedQueueRunner`:

```dart
Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await Workmanager().initialize(callbackDispatcher);
  await background.schedule(); // periodic; safe to call on every start

  final queue = await openQueue();
  final runner = LeasedQueueRunner(queue, await openLease());
  LeasedQueueLifecycle.attach(runner);

  runApp(MyApp(queue: queue));
}
```

Enqueue from anywhere in the app with `queue.enqueue(...)`, in the foreground or not. Tasks wait in storage until whoever holds the lease runs them.

If you use `DurableQueueScope` from [`durable_queue_flutter`](https://pub.dev/packages/durable_queue_flutter) (0.1.1 or later) for its builders, give it `runQueue: false` so it leaves starting and pausing to the runner:

```dart
runApp(DurableQueueScope(queue: queue, runQueue: false, child: const MyApp()));
```

## What runs when

| App state | Who runs the queue |
| --- | --- |
| Foreground | The app, through `LeasedQueueRunner`. A background job that starts now finds the lease held and returns. |
| Going to the background | The runner stops starting tasks, waits for running handlers, and releases the lease. |
| Background or closed | The periodic job, when the operating system runs it: it drains for up to `timeLimit`, then closes the queue. |
| Back in the foreground | The runner takes the lease again. If a job is still draining, it waits for it (checking every `retryInterval`, 5 seconds by default). |

`background.run` returns `true` when the job did its part, including when the lease was held elsewhere, and `false` after an error (passed to `onError`), which lets Android retry the job with backoff.

### Time limits

A run starts tasks for up to `timeLimit`, then lets running handlers finish. The default is 20 seconds on iOS, where a background refresh gets about 30, and 8 minutes on Android, where a worker gets 10. Keep your longest task well inside the remaining headroom, or set `timeLimit` yourself.

### Waking up for scheduled work (Android)

After a run, if stored tasks become due before the next periodic run (a delayed task, a retry, or a task deferred by a gate or a rate limit), `run` registers a one-off job (`wakeTaskName`) for that time, at least `minWakeDelay` (1 minute) away. Work that was cut off by the time limit is picked up by that job too.

On iOS a one-off delay is honored only while the app is alive, so this is off there by default and the periodic job does the work. Set `scheduleWakeUps` to change it.

### When the operating system runs jobs

Android runs the periodic job at most every 15 minutes, when its constraints are met, and may wait longer on a device that is idle or saving battery. iOS decides by itself when to run a background refresh, based on how the app is used; it can be hours. Background jobs are for work that can wait. Work the user is waiting for belongs in the foreground.

**Uploads and downloads** should not run inside these jobs: iOS gives a job about 30 seconds. Use [`durable_queue_transfer`](https://pub.dev/packages/durable_queue_transfer), which hands each transfer to the operating system's background transfer service, so it keeps going while the app is minimised or closed.

### Longer runs on Android (foreground service)

```dart
final background = QueueBackgroundWork(
  openQueue: openQueue,
  openLease: openLease,
  foregroundService: ForegroundServiceConfig(
    notificationTitle: 'Syncing',
    notificationText: 'Sending your changes',
  ),
  timeLimit: const Duration(minutes: 30),
);
```

With `foregroundService`, the periodic and wake-up jobs run as Android foreground services: a notification is shown and the run may last longer than a normal background job, so raise `timeLimit` to match. While a run holds the lease the app's queue waits for it, so keep runs to minutes rather than hours and let wake-up jobs continue the work. Workmanager declares the `dataSync` service type; on Android 15 such services may run at most 6 hours a day, and on Android 13 and later the notification is shown only with the notification permission. iOS ignores it.

## Guarantees and limits

* Delivery stays at least once, as everywhere in durable_queue: a task interrupted by a crash or by the operating system runs again. Make handlers safe to repeat.
* The lease is renewed every quarter of its `ttl` (default 2 minutes). If a holder is frozen for longer than `ttl` while it runs a task, another runner can take over and run that task again. `LeasedDrainResult.leaseLost` reports this for a background run. Choose a `ttl` longer than any pause you expect.
* The lease depends on the device clock and is a cooperation between parts of one app, not a security boundary.
* Deduplication keys are checked by each queue. Two isolates that enqueue the same key at the same moment can both succeed.
* Each `FileQueueLease` object is one holder. Create one per runner; do not share an object between isolates.

## Other leases

`QueueLease` is a small interface (`tryAcquire`, `renew`, `release`, `ttl`). Implement it to keep the lease somewhere else, for example in your own database. `drainWithLease` runs the background part without Workmanager, if you use another scheduler.

## With other durable_queue packages

* [`durable_queue_sqlite`](https://pub.dev/packages/durable_queue_sqlite): storage that the app and the background isolate can share.
* [`durable_queue_connectivity`](https://pub.dev/packages/durable_queue_connectivity): network tasks wait while offline. Add the same gate in `openQueue`, so the background job defers them too.
* [`durable_queue_dio`](https://pub.dev/packages/durable_queue_dio): HTTP retry rules, `Retry-After`, and queued requests.
* [`durable_queue_flutter`](https://pub.dev/packages/durable_queue_flutter): `DurableQueueScope` and widgets that rebuild from stored tasks.
* [`durable_queue_transfer`](https://pub.dev/packages/durable_queue_transfer): uploads and downloads that the operating system finishes while the app is closed.

## Contributing

Issues and pull requests: [github.com/boffincoders/durable_queue](https://github.com/boffincoders/durable_queue). See [CONTRIBUTING.md](https://github.com/boffincoders/durable_queue/blob/main/CONTRIBUTING.md).
