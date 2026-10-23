// Recipe: submit a form now, or as soon as the device is back online.
//
// The request is stored before `submitFeedback` returns, waits while the
// device is offline without using attempts, and is sent once even if the
// user taps "Send" twice.
import 'package:dio/dio.dart';
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_connectivity/durable_queue_connectivity.dart';
import 'package:durable_queue_dio/durable_queue_dio.dart';

Future<DurableQueue> openFormQueue(QueueStorage storage, Dio api) async {
  final queue = DurableQueue(storage: storage);
  queue.addGate('network', await ConnectivityGate.create());
  const rules = DioRetryRules();
  queue.registerHttpRequests(
    api, // with baseUrl and an auth interceptor; no secrets in the task
    requires: {'network'},
    deferOn: rules.deferOn('network'),
  );
  await queue.start();
  return queue;
}

Future<String> submitFeedback(
  DurableQueue queue, {
  required String formId,
  required Map<String, Object?> answers,
}) {
  return queue.enqueue(
    HttpRequestTask(method: 'POST', url: '/feedback', body: answers),
    // A second tap while the first is still queued returns the same task.
    deduplicationKey: 'feedback:$formId',
    // Sent as the Idempotency-Key header, so the server can drop a repeat
    // after a crash between "sent" and "recorded".
    idempotencyKey: formId,
    retryPolicy: RetryPolicy.exponential(
      maxAttempts: 10,
      initialDelay: const Duration(seconds: 5),
      maxDelay: const Duration(minutes: 10),
      jitter: true,
    ),
  );
}
