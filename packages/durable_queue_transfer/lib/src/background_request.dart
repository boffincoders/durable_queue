import 'dart:convert';

import 'package:background_downloader/background_downloader.dart' show DataTask;
import 'package:durable_queue/durable_queue.dart' show DurableTask;

/// A stored HTTP request that the operating system sends, so it goes out
/// even after the app is minimised or closed.
///
/// Register it with `QueueTransfers.registerRequests` and enqueue it like
/// any task:
///
/// ```dart
/// transfers.registerRequests();
/// await queue.enqueue(
///   BackgroundRequest(
///     method: 'POST',
///     url: 'https://api.example.com/messages',
///     body: {'text': 'hi'},
///   ),
///   idempotencyKey: messageId,
/// );
/// ```
///
/// Everything here is stored as plain JSON. Do not put credentials in
/// [headers]; pass them with `registerRequests(headersAtSend:)` instead.
final class BackgroundRequest extends DurableTask {
  /// Creates a request. [method] is upper-cased.
  ///
  /// [url] must be absolute. [body] is sent as JSON when it is a map or a
  /// list, and as text when it is a string; it must be JSON-encodable.
  /// [query] is added to the URL, encoded. With [requiresWiFi], the request
  /// waits for a Wi-Fi or other unmetered connection.
  BackgroundRequest({
    required String method,
    required this.url,
    Map<String, String> headers = const {},
    Map<String, String> query = const {},
    this.body,
    this.contentType,
    this.requiresWiFi = false,
  }) : method = method.toUpperCase(),
       headers = Map.unmodifiable(headers),
       query = Map.unmodifiable(query) {
    if (method.trim().isEmpty) {
      throw ArgumentError.value(method, 'method', 'Must not be empty');
    }
    final uri = Uri.tryParse(url);
    if (uri == null || !uri.hasScheme || uri.host.isEmpty) {
      throw ArgumentError.value(url, 'url', 'Must be an absolute URL');
    }
    if (body != null && body is! String && body is! Map && body is! List) {
      throw ArgumentError.value(
        body,
        'body',
        'Must be a string, a map, a list, or null',
      );
    }
  }

  /// Rebuilds a request stored by [toJson].
  factory BackgroundRequest.fromJson(Map<String, dynamic> json) {
    return BackgroundRequest(
      method: json['method'] as String,
      url: json['url'] as String,
      headers: Map<String, String>.from(json['headers'] as Map? ?? const {}),
      query: Map<String, String>.from(json['query'] as Map? ?? const {}),
      body: json['body'],
      contentType: json['contentType'] as String?,
      requiresWiFi: json['requiresWiFi'] as bool? ?? false,
    );
  }

  /// Task type used for registration and storage.
  static const taskType = 'durable_queue_transfer/request';

  /// HTTP method, upper case.
  final String method;

  /// Absolute URL.
  final String url;

  /// Request headers. Stored; keep secrets out.
  final Map<String, String> headers;

  /// Query parameters added to [url].
  final Map<String, String> query;

  /// JSON body (map or list), text body (string), or null.
  final Object? body;

  /// Content type. Null means `application/json` for a JSON body and
  /// `text/plain; charset=utf-8` for a text body.
  final String? contentType;

  /// Whether the request waits for Wi-Fi or another unmetered connection.
  final bool requiresWiFi;

  @override
  String get type => taskType;

  @override
  Map<String, dynamic> toJson() => {
    'method': method,
    'url': url,
    'headers': headers,
    'query': query,
    'body': body,
    'contentType': contentType,
    'requiresWiFi': requiresWiFi,
  };

  /// The background_downloader task that sends this request, with
  /// [extraHeaders] added to [headers].
  DataTask toDataTask({Map<String, String> extraHeaders = const {}}) {
    final base = Uri.parse(url);
    final target = query.isEmpty
        ? base
        : base.replace(queryParameters: {...base.queryParameters, ...query});
    final value = body;
    final String? post;
    String? type = contentType;
    if (value == null) {
      post = null;
    } else if (value is String) {
      post = value;
    } else {
      post = jsonEncode(value);
      type ??= 'application/json';
    }
    return DataTask(
      url: target.toString(),
      headers: {...headers, ...extraHeaders},
      httpRequestMethod: method,
      post: post,
      contentType: type,
      requiresWiFi: requiresWiFi,
    );
  }
}
