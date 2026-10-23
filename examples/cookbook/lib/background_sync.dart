// Recipe: keep working through the queue while the app is in the
// background or closed, with Workmanager (Android and iOS).
//
// The app and the background job build the queue the same way and share a
// lease file, so they never run it at the same time.
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:durable_queue_workmanager/durable_queue_workmanager.dart';
import 'package:flutter/widgets.dart';
import 'package:path_provider/path_provider.dart';
import 'package:workmanager/workmanager.dart' show Workmanager;

Future<DurableQueue> openQueue() async {
  final dir = await getApplicationSupportDirectory();
  final queue = DurableQueue(
    storage: SqliteQueueStorage.open('${dir.path}/queue.db'),
  );
  // Register the same task types here as everywhere else.
  return queue;
}

Future<QueueLease> openLease() async {
  final dir = await getApplicationSupportDirectory();
  return FileQueueLease('${dir.path}/queue.lease');
}

const background = QueueBackgroundWork(
  openQueue: openQueue,
  openLease: openLease,
  taskName: 'com.example.app.queue', // also in Info.plist on iOS
);

@pragma('vm:entry-point')
void callbackDispatcher() {
  Workmanager().executeTask((taskName, inputData) async {
    if (background.handles(taskName)) return background.run(taskName);
    return true;
  });
}

Future<void> startQueueForApp() async {
  WidgetsFlutterBinding.ensureInitialized();
  await Workmanager().initialize(callbackDispatcher);
  await background.schedule();
  final queue = await openQueue();
  LeasedQueueLifecycle.attach(LeasedQueueRunner(queue, await openLease()));
}
