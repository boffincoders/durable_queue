# Quick start

An outbox in one file: requests you send are stored first, wait while the device is offline, retry when they fail, and still go out after the app was closed. It takes about five minutes.

## 1. Add the packages

```
flutter pub add durable_queue durable_queue_sqlite durable_queue_flutter durable_queue_dio durable_queue_connectivity dio path_provider
```

| Package | Why |
| --- | --- |
| `durable_queue` | the queue: storing, retrying, ordering |
| `durable_queue_sqlite` | keeps tasks in SQLite, so they survive the app being closed |
| `durable_queue_flutter` | starts the queue with the app and rebuilds widgets from it |
| `durable_queue_dio` | sends queued HTTP requests with Dio |
| `durable_queue_connectivity` | makes requests wait while the device is offline |

## 2. Replace `lib/main.dart`

```dart
import 'package:dio/dio.dart';
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_connectivity/durable_queue_connectivity.dart';
import 'package:durable_queue_dio/durable_queue_dio.dart';
import 'package:durable_queue_flutter/durable_queue_flutter.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:flutter/material.dart';
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
```

The example posts to `https://httpbin.org/post`, which echoes what it receives. Point `baseUrl` and `url` at your own API.

## 3. Run it and try to lose a request

1. Tap the send button. The row goes from `pending` to `running` to `completed`.
2. Turn on airplane mode and tap send a few times. The rows stay `pending`: they wait for the network without using attempts.
3. Close the app completely, then open it again. The rows are still there.
4. Turn airplane mode off. The waiting requests go out, oldest first.

## What happened

* `enqueue` stored the request in SQLite before it returned, so closing the app could not lose it.
* The `network` gate kept requests waiting while offline, and `deferOn` turned a dropped connection into waiting instead of a failed attempt.
* Server errors such as a 503 are retried with exponential backoff, up to `maxAttempts`; a 400 or 404 fails at once.
* `DurableQueueScope` started the queue, paused it in the background, and `QueueGroupBuilder` rebuilt the list from stored tasks.

## Next

* Send the same request only once, however often the user taps: [deduplication and idempotency](../packages/durable_queue/README.md#deduplication-and-idempotency).
* Uploads that finish after the app is closed: [`durable_queue_transfer`](../packages/durable_queue_transfer/README.md).
* Your own task types, retries, priorities, and dependencies: the [guide](../packages/durable_queue/README.md).
* More complete examples: the [cookbook](cookbook.md).
