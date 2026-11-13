// Quick start: an outbox whose requests survive the app being closed and
// wait while the device is offline. This is the whole app; docs/quickstart.md
// walks through it.
//
// Packages: flutter pub add durable_queue durable_queue_sqlite
//   durable_queue_flutter durable_queue_dio durable_queue_connectivity dio
//   path_provider
import 'package:dio/dio.dart';
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_connectivity/durable_queue_connectivity.dart';
import 'package:durable_queue_dio/durable_queue_dio.dart';
import 'package:durable_queue_flutter/durable_queue_flutter.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:material_ui/material_ui.dart';
import 'package:path_provider/path_provider.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();

  // 1. Store tasks in SQLite, so they survive the app being closed.
  final directory = await getApplicationSupportDirectory();
  final queue = DurableQueue(
    storage: SqliteQueueStorage.open('${directory.path}/queue.db'),
  );

  // 2. Requests wait while the device is offline, without using attempts.
  queue.addGate('network', await ConnectivityGate.create());

  // 3. Send queued HTTP requests with Dio. Use your own API here.
  queue.registerHttpRequests(
    Dio(BaseOptions(baseUrl: 'https://httpbin.org')),
    requires: {'network'},
    deferOn: const DioRetryRules().deferOn('network'),
  );

  // 4. DurableQueueScope starts the queue and pauses it in the background.
  runApp(
    DurableQueueScope(
      queue: queue,
      child: const MaterialApp(home: OutboxPage()),
    ),
  );
}

class OutboxPage extends StatelessWidget {
  const OutboxPage({super.key});

  Future<void> _send(BuildContext context) {
    // 5. Stored before this returns; sent now, or when the device is online.
    return DurableQueueScope.of(context).enqueue(
      HttpRequestTask(
        method: 'POST',
        url: '/post',
        body: {'sentAt': DateTime.now().toIso8601String()},
      ),
      group: 'outbox',
      retryPolicy: RetryPolicy.exponential(
        maxAttempts: 5,
        initialDelay: const Duration(seconds: 2),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Outbox')),
      floatingActionButton: FloatingActionButton(
        onPressed: () => _send(context),
        child: const Icon(Icons.send),
      ),
      // 6. Rebuilds whenever a task in the group changes.
      body: QueueGroupBuilder(
        group: 'outbox',
        builder: (context, tasks) => ListView(
          children: [
            for (final task in tasks.reversed)
              ListTile(
                title: Text('POST ${task.payload['url']}'),
                subtitle: Text(
                  task.lastFailure == null
                      ? task.status.name
                      : '${task.status.name}: ${task.lastFailure!.error}',
                  maxLines: 2,
                  overflow: TextOverflow.ellipsis,
                ),
              ),
          ],
        ),
      ),
    );
  }
}
