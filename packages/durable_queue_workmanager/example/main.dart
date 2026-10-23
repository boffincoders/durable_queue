// Wiring for an app whose queued uploads also run in Workmanager background
// jobs. The app and the job build the queue the same way (openQueue) and
// share one lease file (openLease), so they never run the queue at once.
import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_workmanager/durable_queue_workmanager.dart';
import 'package:flutter/widgets.dart';
import 'package:workmanager/workmanager.dart' show Workmanager;

final class UploadTask extends DurableTask {
  UploadTask(this.path);

  final String path;

  @override
  String get type => 'upload';

  @override
  Map<String, dynamic> toJson() => {'path': path};

  static UploadTask fromJson(Map<String, dynamic> json) =>
      UploadTask(json['path'] as String);
}

// In a real app: `getApplicationSupportDirectory()` from path_provider.
Future<Directory> appDirectory() async => Directory.systemTemp;

// In a real app the storage must be shared between isolates, for example
// `SqliteQueueStorage.open('${dir.path}/queue.db')` from durable_queue_sqlite.
QueueStorage openStorage(Directory dir) => MemoryQueueStorage();

Future<DurableQueue> openQueue() async {
  final queue = DurableQueue(storage: openStorage(await appDirectory()));
  queue.register<UploadTask>(
    type: 'upload',
    decoder: UploadTask.fromJson,
    handler: (task, context) async {
      // Upload task.path. Make it safe to repeat: delivery is at least once.
    },
  );
  return queue;
}

Future<QueueLease> openLease() async =>
    FileQueueLease('${(await appDirectory()).path}/queue.lease');

const background = QueueBackgroundWork(
  openQueue: openQueue,
  openLease: openLease,
);

@pragma('vm:entry-point')
void callbackDispatcher() {
  Workmanager().executeTask((taskName, inputData) async {
    if (background.handles(taskName)) return background.run(taskName);
    return true;
  });
}

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await Workmanager().initialize(callbackDispatcher);
  await background.schedule();

  final queue = await openQueue();
  final runner = LeasedQueueRunner(queue, await openLease());
  LeasedQueueLifecycle.attach(runner);

  await queue.enqueue(UploadTask('/photos/1.jpg'));
  runApp(
    const Directionality(
      textDirection: TextDirection.ltr,
      child: Text('Uploads run now, and in the background later.'),
    ),
  );
}
