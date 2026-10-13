// An offline-first message queue: requests are stored in SQLite, survive an
// app restart, honor Retry-After, and carry an idempotency key.
//
// Run with `dart run example/main.dart` from packages/durable_queue_dio.
// A local server stands in for your API so the example runs offline.
import 'dart:convert';
import 'dart:io';

import 'package:dio/dio.dart';
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_dio/durable_queue_dio.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';

Future<void> main() async {
  final server = await _fakeApi();
  final api = 'http://127.0.0.1:${server.port}';
  final dir = await Directory.systemTemp.createTemp('messages_');
  final path = '${dir.path}/queue.db';

  // First launch, offline: the message is stored, then the app is closed.
  var storage = SqliteQueueStorage.open(path);
  var queue = _buildQueue(storage);
  await queue.enqueue(
    HttpRequestTask(
      method: 'POST',
      url: '$api/messages',
      body: {'text': 'See you at 7'},
    ),
    idempotencyKey: 'message-1001',
    retryPolicy: RetryPolicy.exponential(
      maxAttempts: 5,
      initialDelay: const Duration(seconds: 1),
    ),
  );
  print('stored 1 message; app closed');
  await queue.close();
  await storage.close();

  // Next launch, online: the queue sends it.
  storage = SqliteQueueStorage.open(path);
  queue = _buildQueue(storage);
  final done = queue.events
      .where((event) => event is TaskCompleted || event is TaskFailed)
      .first;
  await queue.start();
  await done;

  await queue.close();
  await storage.close();
  await server.close(force: true);
  await dir.delete(recursive: true);
}

DurableQueue _buildQueue(QueueStorage storage) {
  final queue = DurableQueue(storage: storage);
  queue.registerHttpRequests(
    Dio(BaseOptions(connectTimeout: const Duration(seconds: 5))),
    onResponse: (task, response, context) =>
        print('sent on attempt ${context.attempt}: ${response.statusCode}'),
  );
  queue.events.listen((event) {
    if (event is TaskRetryScheduled) {
      print('server busy, retrying in ${event.delay.inSeconds} s');
    }
  });
  return queue;
}

/// Answers 503 with Retry-After once, then accepts the message.
Future<HttpServer> _fakeApi() async {
  final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  final seen = <String>{};
  var busy = true;
  server.listen((request) async {
    final body = await utf8.decoder.bind(request).join();
    final key = request.headers.value('idempotency-key');
    if (busy) {
      busy = false;
      request.response
        ..statusCode = 503
        ..headers.set('Retry-After', '1');
    } else if (key != null && !seen.add(key)) {
      request.response.statusCode = 200; // duplicate: already stored
    } else {
      print('server stored $body (key $key)');
      request.response.statusCode = 201;
    }
    await request.response.close();
  });
  return server;
}
