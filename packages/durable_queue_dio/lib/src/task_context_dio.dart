import 'package:dio/dio.dart';
import 'package:durable_queue/durable_queue.dart';

/// Default header for [DurableQueueDioContext.withIdempotencyKey].
const defaultIdempotencyHeader = 'Idempotency-Key';

/// Dio helpers for a running task.
extension DurableQueueDioContext on TaskContext {
  /// A [CancelToken] that is cancelled when this task's cancellation is
  /// requested.
  ///
  /// Pass it to a Dio call so `DurableQueue.cancel` aborts the request. The
  /// aborted call throws a [DioException] of type
  /// [DioExceptionType.cancel], and the queue marks the task cancelled.
  CancelToken dioCancelToken() {
    final token = CancelToken();
    whenCancellationRequested.then((_) {
      if (!token.isCancelled) token.cancel('durable_queue task cancelled');
    });
    return token;
  }

  /// [options] with this task's idempotency key added as the [header].
  ///
  /// The key is the same on every attempt, so a server that supports
  /// idempotency keys can ignore a retried request it already handled.
  /// Returns [options] unchanged (or empty options) when the task has no
  /// idempotency key.
  Options withIdempotencyKey(
    Options? options, {
    String header = defaultIdempotencyHeader,
  }) {
    final base = options ?? Options();
    final key = idempotencyKey;
    if (key == null) return base;
    return base.copyWith(headers: {...?base.headers, header: key});
  }
}
