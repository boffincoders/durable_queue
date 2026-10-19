import 'dart:async';

import 'package:http/http.dart' as http;

/// A reachability probe that sends one HTTP request to [url].
///
/// Point it at your own API, not a third-party site: the question that
/// matters is whether the app can reach the server its tasks talk to, and
/// no traffic goes anywhere else.
///
/// The probe succeeds when the server answers with a status that
/// [isReachable] accepts within [timeout]. By default that is any status
/// from 200 to 299 or 400 to 499: a 401 or 404 still proves the server is
/// reachable, while a 5xx means it cannot do the work. Redirects are not
/// followed and count as unreachable, because a captive portal (hotel or
/// airport Wi-Fi) usually answers with a redirect to its login page. If
/// your endpoint redirects, pass your own [isReachable]. For the most
/// reliable check, use an endpoint that returns a fixed status such as 204.
///
/// On the web the endpoint must allow the request with CORS.
///
/// Use it as `NetworkGate(probe: HttpProbe(url).call)` or
/// `ConnectivityGate.create(probe: HttpProbe(url).call)`.
final class HttpProbe {
  /// Creates a probe for [url]. [timeout] must be positive.
  HttpProbe(
    this.url, {
    this.method = 'HEAD',
    this.timeout = const Duration(seconds: 5),
    this.headers = const {},
    bool Function(int statusCode)? isReachable,
    http.Client Function()? clientFactory,
  }) : _isReachable = isReachable ?? _defaultReachable,
       _clientFactory = clientFactory ?? http.Client.new {
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, 'timeout', 'Must be positive');
    }
  }

  /// Address the probe requests.
  final Uri url;

  /// HTTP method, `HEAD` by default.
  final String method;

  /// Longest wait for a response.
  final Duration timeout;

  /// Extra request headers, for example an API key the endpoint requires.
  final Map<String, String> headers;

  final bool Function(int statusCode) _isReachable;
  final http.Client Function() _clientFactory;

  static bool _defaultReachable(int status) =>
      (status >= 200 && status < 300) || (status >= 400 && status < 500);

  /// Runs the probe. Never throws.
  Future<bool> call() async {
    final client = _clientFactory();
    try {
      final request = http.Request(method, url)
        ..followRedirects = false
        ..headers.addAll(headers);
      final response = await client.send(request).timeout(timeout);
      // Release the connection without reading a body we do not need.
      unawaited(response.stream.drain<void>().catchError((_) {}));
      return _isReachable(response.statusCode);
    } on Object {
      return false;
    } finally {
      client.close();
    }
  }

  @override
  String toString() => 'HttpProbe($method $url)';
}
