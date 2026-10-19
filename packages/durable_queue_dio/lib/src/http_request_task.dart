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

  /// The host of a stored [HttpRequestTask], with the port when the URL
  /// names one, for example `api.example.com` or `127.0.0.1:8080`.
  ///
  /// A relative URL is resolved against [baseUrl] (usually
  /// `dio.options.baseUrl`). Returns null for tasks of other types, or when
  /// no host can be determined.
  static String? hostOf(StoredTask task, {String? baseUrl}) {
    if (task.type != taskType) return null;
    final url = task.payload['url'];
    if (url is! String) return null;
    final Uri resolved;
    try {
      final uri = Uri.parse(url);
      resolved = uri.hasScheme || baseUrl == null || baseUrl.isEmpty
          ? uri
          : Uri.parse(baseUrl).resolveUri(uri);
    } on FormatException {
      return null;
    }
    final host = resolved.host.toLowerCase();
    if (host.isEmpty) return null;
    return resolved.hasPort ? '$host:${resolved.port}' : host;
  }

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
  ///
  /// [requires], [deferOn], and [rateLimit] are passed to
  /// `DurableQueue.register`: list gates such as `network` in [requires] so
  /// requests wait while it is closed, use `rules.deferOn('network')` so a
  /// connection error waits for the gate instead of using an attempt, and set
  /// [rateLimit] to limit all queued requests together. For a limit per host,
  /// use [addHttpHostRateLimit].
  void registerHttpRequests(
    Dio dio, {
    DioRetryRules rules = const DioRetryRules(),
    String idempotencyHeader = defaultIdempotencyHeader,
    HttpResponseCallback? onResponse,
    Set<String> requires = const {},
    DeferOnError? deferOn,
    RateLimit? rateLimit,
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
      requires: requires,
      deferOn: deferOn,
      rateLimit: rateLimit,
    );
  }

  /// Limits queued [HttpRequestTask]s per host: each host gets its own
  /// [limit], for example `RateLimit(5, per: Duration(seconds: 1))`.
  ///
  /// Relative URLs are resolved against [dio]'s `baseUrl`, read when each
  /// request is about to start. Requests whose host cannot be determined
  /// are not limited by this limit. Every attempt counts. [name] identifies
  /// the limit in `TaskDeferred` events.
  ///
  /// Throws `StateError` if a limit with [name] was already added.
  void addHttpHostRateLimit(
    RateLimit limit, {
    Dio? dio,
    String name = 'http-host',
  }) {
    addRateLimit(
      name,
      limit,
      types: {HttpRequestTask.taskType},
      keyOf: (task) =>
          HttpRequestTask.hostOf(task, baseUrl: dio?.options.baseUrl),
    );
  }
}
