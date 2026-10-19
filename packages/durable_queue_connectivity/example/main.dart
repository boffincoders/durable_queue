// Run in a Flutter app (connectivity_plus is a Flutter plugin).
//
// Uploads wait while the device is offline and start when it is back online.
// A connection error defers the upload instead of using an attempt.
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_connectivity/durable_queue_connectivity.dart';
import 'package:http/http.dart' as http;

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

Future<DurableQueue> startQueue(QueueStorage storage) async {
  final api = Uri.parse('https://api.example.com');

  final network = await ConnectivityGate.create(
    probe: HttpProbe(api.resolve('/health')).call,
  );
  network.changes.listen((online) => print(online ? 'online' : 'offline'));

  final queue = DurableQueue(storage: storage);
  queue.addGate('network', network);
  queue.register<UploadTask>(
    type: 'upload',
    decoder: UploadTask.fromJson,
    requires: {'network'},
    deferOn: deferOnConnectionError('network'),
    handler: (task, context) async {
      final response = await http.post(
        api.resolve('/uploads'),
        body: {'path': task.path},
      );
      if (response.statusCode >= 500) {
        throw StateError('Server error ${response.statusCode}');
      }
    },
  );
  queue.events.listen((event) {
    if (event is TaskDeferred) {
      print('${event.taskId} waits: ${event.reason.name} (${event.name})');
    }
  });
  await queue.start();
  return queue;
}

Future<void> main() async {
  // Use durable_queue_sqlite (or another persistent storage) in a real app.
  final queue = await startQueue(MemoryQueueStorage());
  await queue.enqueue(
    UploadTask('/photos/beach.jpg'),
    retryPolicy: RetryPolicy.exponential(
      maxAttempts: 5,
      initialDelay: const Duration(seconds: 2),
    ),
  );
}
