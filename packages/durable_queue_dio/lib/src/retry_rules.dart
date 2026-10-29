import 'package:dio/dio.dart';
import 'package:durable_queue/durable_queue.dart';

import 'retry_after.dart';

/// Decides which Dio errors are worth retrying, and how long to wait.
///
/// Pass [shouldRetry] as `retryIf` and [retryDelay] as `retryDelay` when
/// registering a handler that uses Dio:
///
/// ```dart
/// const rules = DioRetryRules();
/// queue.register<UploadTask>(
///   type: 'upload',
///   decoder: UploadTask.fromJson,
///   handler: upload,
///   retryIf: rules.shouldRetry,
///   retryDelay: rules.retryDelay,
/// );
/// ```
///
/// The task's `RetryPolicy` still limits the number of attempts.
final class DioRetryRules {
  /// Creates rules. The defaults retry timeouts, connection failures, and the
  /// status codes in [defaultRetryStatusCodes], and honor `Retry-After` up to
  /// one hour.
  const DioRetryRules({
    this.retryStatusCodes = defaultRetryStatusCodes,
    this.retryTimeouts = true,
    this.retryConnectionErrors = true,
    this.useRetryAfter = true,
    this.maxRetryAfter = const Duration(hours: 1),
    this.now,
  });

  /// 408 Request Timeout, 425 Too Early, 429 Too Many Requests, 500, 502,
  /// 503, and 504.
  static const defaultRetryStatusCodes = {408, 425, 429, 500, 502, 503, 504};

  /// HTTP status codes that are retried. Every other status fails the task.
  final Set<int> retryStatusCodes;

  /// Whether connect, send, and receive timeouts are retried, and attempts
  /// that ran past the `timeout` given to `DurableQueue.register`
  /// ([TaskTimeoutException]).
  final bool retryTimeouts;

  /// Whether connection failures (DNS, refused, reset, offline) are retried.
  final bool retryConnectionErrors;

  /// Whether a `Retry-After` response header sets the next delay.
  final bool useRetryAfter;

  /// Longest `Retry-After` delay that is honored; longer values are capped.
  final Duration maxRetryAfter;

  /// Clock for HTTP-date `Retry-After` values. Defaults to [DateTime.now].
  final DateTime Function()? now;

  /// Whether [error] should be retried.
  ///
  /// Only [DioException]s and, with [retryTimeouts], [TaskTimeoutException]
  /// are retried. Cancellation, certificate errors,
  /// response-transform timeouts, unknown errors, and status codes outside
  /// [retryStatusCodes] are not. Combine this with your own predicate if your
  /// handler throws other retryable errors.
  bool shouldRetry(Object error, StackTrace stackTrace) {
    if (error is TaskTimeoutException) return retryTimeouts;
    if (error is! DioException) return false;
    final type = error.type;
    if (type == DioExceptionType.connectionTimeout ||
        type == DioExceptionType.sendTimeout ||
        type == DioExceptionType.receiveTimeout) {
      return retryTimeouts;
    }
    if (type == DioExceptionType.connectionError) return retryConnectionErrors;
    if (type == DioExceptionType.badResponse) {
      final status = error.response?.statusCode;
      return status != null && retryStatusCodes.contains(status);
    }
    return false;
  }

  /// Whether [error] means the server could not be reached at all: a
  /// [DioException] of type `connectionError` (DNS failure, refused, reset,
  /// offline) or `connectionTimeout`.
  ///
  /// Send and receive timeouts are not included: the connection was made, so
  /// the network was there.
  static bool isConnectionError(Object error) {
    if (error is! DioException) return false;
    return error.type == DioExceptionType.connectionError ||
        error.type == DioExceptionType.connectionTimeout;
  }

  /// A `register(deferOn:)` rule that sends connection errors to [gate].
  ///
  /// The task then waits for the gate instead of using an attempt, and the
  /// gate is asked to check again. [gate] must be one of the task type's
  /// `requires`:
  ///
  /// ```dart
  /// queue.registerHttpRequests(
  ///   dio,
  ///   requires: {'network'},
  ///   deferOn: rules.deferOn('network'),
  /// );
  /// ```
  DeferOnError deferOn(String gate) {
    if (gate.isEmpty) {
      throw ArgumentError.value(gate, 'gate', 'Must not be empty');
    }
    return (error, stackTrace) => isConnectionError(error) ? gate : null;
  }

  /// The delay a response asked for with `Retry-After`, or null to use the
  /// task's retry policy.
  Duration? retryDelay(Object error, StackTrace stackTrace) {
    if (!useRetryAfter || error is! DioException) return null;
    final values = error.response?.headers['retry-after'];
    if (values == null || values.isEmpty) return null;
    final wait = parseRetryAfter(values.first, now: now?.call());
    if (wait == null) return null;
    return wait > maxRetryAfter ? maxRetryAfter : wait;
  }
}
