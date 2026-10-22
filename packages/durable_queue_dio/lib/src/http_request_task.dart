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
    return hostOfUrl(url, baseUrl: baseUrl);
  }

  /// The host of [url], resolved against [baseUrl] when relative, as
  /// [hostOf] reports it. Returns null when no host can be determined.
  static String? hostOfUrl(String url, {String? baseUrl}) {
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

/// Name of the per-host rate limit added by
/// `DurableQueueDio.addHttpHostRateLimit` unless another is given.
const defaultHostRateLimit = 'http-host';

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
  /// When a response is a 429 or 503 with a `Retry-After` header that
  /// [rules] honors, and a per-host limit named [hostRateLimit] exists (see
  /// [addHttpHostRateLimit]), that host's bucket is held for the
  /// `Retry-After` time, so every queued request to the host waits, not only
  /// the one that got the answer. Pass `hostRateLimit: null` to turn this
  /// off.
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
    String? hostRateLimit = defaultHostRateLimit,
  }) {
    register<HttpRequestTask>(
      type: HttpRequestTask.taskType,
      decoder: HttpRequestTask.fromJson,
      handler: (task, context) async {
        final Response<Object?> response;
        try {
          response = await dio.request<Object?>(
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
        } on DioException catch (error, stackTrace) {
          _holdHost(dio, rules, hostRateLimit, task, error, stackTrace);
          rethrow;
        }
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
    String name = defaultHostRateLimit,
  }) {
    addRateLimit(
      name,
      limit,
      types: {HttpRequestTask.taskType},
      keyOf: (task) =>
          HttpRequestTask.hostOf(task, baseUrl: dio?.options.baseUrl),
    );
  }

  void _holdHost(
    Dio dio,
    DioRetryRules rules,
    String? limitName,
    HttpRequestTask task,
    DioException error,
    StackTrace stackTrace,
  ) {
    if (limitName == null || !hasRateLimit(limitName)) return;
    final status = error.response?.statusCode;
    if (status != 429 && status != 503) return;
    final wait = rules.retryDelay(error, stackTrace);
    if (wait == null || wait <= Duration.zero) return;
    final host = HttpRequestTask.hostOfUrl(
      task.url,
      baseUrl: dio.options.baseUrl,
    );
    if (host == null) return;
    holdRateLimit(limitName, key: host, duration: wait);
  }
}
