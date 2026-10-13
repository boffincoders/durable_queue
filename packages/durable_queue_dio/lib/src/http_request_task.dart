import 'dart:async';

import 'package:dio/dio.dart';
import 'package:durable_queue/durable_queue.dart';

import 'retry_rules.dart';
import 'task_context_dio.dart';

/// A stored HTTP request: method, URL, headers, query, and a JSON or text
/// body.
///
/// Enqueue it to send the request later, with retries, even after an app
/// restart:
///
/// ```dart
/// queue.registerHttpRequests(dio);
/// await queue.enqueue(
///   HttpRequestTask(method: 'POST', url: '/messages', body: {'text': 'hi'}),
///   idempotencyKey: messageId,
/// );
/// ```
///
/// Everything here is persisted as plain JSON. Do not put credentials in
/// [headers]; add them at send time with a Dio interceptor instead.
final class HttpRequestTask extends DurableTask {
  /// Creates a request. [method] is upper-cased.
  ///
  /// [url] may be relative when the `Dio` instance has a `baseUrl`. [body]
  /// must be JSON-encodable (map, list, string, number, bool, or null);
  /// strings are sent as-is. [queryParameters] values must be
  /// JSON-encodable too.
  HttpRequestTask({
    required String method,
    required this.url,
    Map<String, String> headers = const {},
    Map<String, Object?> queryParameters = const {},
    this.body,
    this.contentType,
  }) : method = method.toUpperCase(),
       headers = Map.unmodifiable(headers),
       queryParameters = Map.unmodifiable(queryParameters) {
    if (method.trim().isEmpty) {
      throw ArgumentError.value(method, 'method', 'Must not be empty');
    }
    if (url.trim().isEmpty) {
      throw ArgumentError.value(url, 'url', 'Must not be empty');
    }
  }

  /// Rebuilds a request stored by [toJson].
  factory HttpRequestTask.fromJson(Map<String, dynamic> json) {
    return HttpRequestTask(
      method: json['method'] as String,
      url: json['url'] as String,
      headers: Map<String, String>.from(json['headers'] as Map? ?? const {}),
      queryParameters: Map<String, Object?>.from(
        json['query'] as Map? ?? const {},
      ),
      body: json['body'],
      contentType: json['contentType'] as String?,
    );
  }

  /// Task type used for registration and storage.
  static const taskType = 'durable_queue_dio/http_request';

  /// HTTP method, upper case.
  final String method;

  /// Absolute URL, or a path relative to the Dio `baseUrl`.
  final String url;

  /// Request headers. Persisted; keep secrets out.
  final Map<String, String> headers;

  /// Query parameters.
  final Map<String, Object?> queryParameters;

  /// JSON-encodable body, or null.
  final Object? body;

  /// Content type, for example `text/plain`. Null uses Dio's default.
  final String? contentType;

  @override
  String get type => taskType;

  @override
  Map<String, dynamic> toJson() => {
    'method': method,
    'url': url,
    'headers': headers,
    'query': queryParameters,
    'body': body,
    'contentType': contentType,
  };
}

/// Called after a stored request succeeds.
typedef HttpResponseCallback = FutureOr<void> Function(
  HttpRequestTask task,
  Response<Object?> response,
  TaskContext context,
);

/// Registers [HttpRequestTask] on a queue.
extension DurableQueueDio on DurableQueue {
  /// Sends every [HttpRequestTask] with [dio].
  ///
  /// Each attempt carries the task's idempotency key in [idempotencyHeader]
  /// when one was given at enqueue, can be cancelled with
  /// `DurableQueue.cancel`, and is retried according to [rules] and the
  /// task's `RetryPolicy`, honoring `Retry-After`. Responses outside Dio's
  /// accepted statuses (2xx by default) are failures.
  ///
  /// [onResponse] runs after each successful request; it is not persisted,
  /// so register it again on every launch. If it throws, the attempt fails.
  void registerHttpRequests(
    Dio dio, {
    DioRetryRules rules = const DioRetryRules(),
    String idempotencyHeader = defaultIdempotencyHeader,
    HttpResponseCallback? onResponse,
  }) {
    register<HttpRequestTask>(
      type: HttpRequestTask.taskType,
      decoder: HttpRequestTask.fromJson,
      handler: (task, context) async {
        final response = await dio.request<Object?>(
          task.url,
          data: task.body,
          queryParameters: task.queryParameters,
          options: context.withIdempotencyKey(
            Options(
              method: task.method,
              headers: task.headers,
              contentType: task.contentType,
            ),
            header: idempotencyHeader,
          ),
          cancelToken: context.dioCancelToken(),
        );
        await onResponse?.call(task, response, context);
      },
      retryIf: rules.shouldRetry,
      retryDelay: rules.retryDelay,
    );
  }
}
