// A small HTTP server for the durable_queue demo app and its kill tests.
//
//   dart run tool/demo_server.dart [--port 8080]
//
// It accepts photo uploads and "done" notifications, counts them per photo,
// and can slow down or fail uploads on request. The kill test also uses it
// to tell the app what to do (scenario) and to read the app's queue state
// (report). Only dart:io, so it runs without `pub get`.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

/// The demo server.
final class DemoServer {
  DemoServer._(this._server) {
    _server.listen((request) {
      _handle(request).catchError((Object error, StackTrace stackTrace) {
        stderr.writeln('demo_server: $error\n$stackTrace');
        try {
          request.response.statusCode = HttpStatus.internalServerError;
          request.response.close();
        } on Object {
          // The response was already sent.
        }
      });
    });
  }

  /// Starts a server on [port] (0 picks a free port) on all interfaces, so
  /// emulators and devices on the network can reach it.
  static Future<DemoServer> start({int port = 8080}) async {
    final server = await HttpServer.bind(InternetAddress.anyIPv4, port);
    return DemoServer._(server);
  }

  final HttpServer _server;

  /// The port the server listens on.
  int get port => _server.port;

  /// Completed uploads per photo name.
  final uploads = <String, int>{};

  /// Upload requests per photo name, including failed and cut-off ones.
  final uploadAttempts = <String, int>{};

  /// Idempotency keys seen per photo name.
  final uploadKeys = <String, Set<String>>{};

  /// "Done" notifications per photo name.
  final done = <String, int>{};

  /// Uploads currently being received or held.
  var uploadsInFlight = 0;

  /// How long each upload is held before it is answered.
  var uploadDelay = Duration.zero;

  /// Answer every n-th upload with 503 (0: never).
  var failEvery = 0;

  /// What the app should do on its next start (see the kill test).
  Map<String, Object?> scenario = {'action': 'none'};

  /// The app's latest report of its queue.
  Map<String, Object?>? report;

  var _requests = 0;
  final _random = Random();

  /// Counts as JSON.
  Map<String, Object?> stats() => {
    'uploads': uploads,
    'uploadAttempts': uploadAttempts,
    'uploadKeys': {
      for (final entry in uploadKeys.entries) entry.key: entry.value.toList(),
    },
    'done': done,
    'uploadsInFlight': uploadsInFlight,
  };

  /// Forgets all counts and settings.
  void reset() {
    uploads.clear();
    uploadAttempts.clear();
    uploadKeys.clear();
    done.clear();
    uploadsInFlight = 0;
    uploadDelay = Duration.zero;
    failEvery = 0;
    scenario = {'action': 'none'};
    report = null;
    _requests = 0;
  }

  /// Stops the server.
  Future<void> close() => _server.close(force: true);

  Future<void> _handle(HttpRequest request) async {
    final path = request.uri.path;
    final method = request.method;
    switch ((method, path)) {
      case ('GET', '/health'):
        return _json(request, {'ok': true});
      case ('HEAD', '/health'):
        request.response.statusCode = HttpStatus.noContent;
        return request.response.close();
      case ('POST', '/upload'):
        return _upload(request);
      case ('GET', '/stats'):
        return _json(request, stats());
      case ('POST', '/reset'):
        reset();
        return _json(request, {'ok': true});
      case ('POST', '/config'):
        final body = await _readJson(request);
        if (body['uploadDelayMs'] case final int ms) {
          uploadDelay = Duration(milliseconds: ms);
        }
        if (body['failEvery'] case final int n) failEvery = n;
        return _json(request, {'ok': true});
      case ('GET', '/scenario'):
        return _json(request, scenario);
      case ('POST', '/scenario'):
        scenario = await _readJson(request);
        return _json(request, {'ok': true});
      case ('GET', '/report'):
        return _json(request, report ?? {});
      case ('POST', '/report'):
        report = await _readJson(request);
        return _json(request, {'ok': true});
    }
    final segments = request.uri.pathSegments;
    if (method == 'POST' &&
        segments.length == 3 &&
        segments[0] == 'photos' &&
        segments[2] == 'done') {
      await request.drain<void>();
      done.update(segments[1], (n) => n + 1, ifAbsent: () => 1);
      return _json(request, {'ok': true});
    }
    await request.drain<void>();
    request.response.statusCode = HttpStatus.notFound;
    await request.response.close();
  }

  Future<void> _upload(HttpRequest request) async {
    final name = request.uri.queryParameters['name'];
    if (name == null || name.isEmpty) {
      await request.drain<void>();
      request.response.statusCode = HttpStatus.badRequest;
      return request.response.close();
    }
    uploadAttempts.update(name, (n) => n + 1, ifAbsent: () => 1);
    final key = request.headers.value('idempotency-key');
    if (key != null) uploadKeys.putIfAbsent(name, () => {}).add(key);
    uploadsInFlight++;
    try {
      var bytes = 0;
      await for (final chunk in request) {
        bytes += chunk.length;
      }
      _requests++;
      if (failEvery > 0 && _requests % failEvery == 0) {
        request.response.statusCode = HttpStatus.serviceUnavailable;
        return await request.response.close();
      }
      if (uploadDelay > Duration.zero) await Future<void>.delayed(uploadDelay);
      uploads.update(name, (n) => n + 1, ifAbsent: () => 1);
      request.response.statusCode = HttpStatus.created;
      request.response.headers.contentType = ContentType.json;
      request.response.write(
        jsonEncode({
          'photoId': '${name}_${_random.nextInt(1 << 30)}',
          'bytes': bytes,
        }),
      );
      await request.response.close();
    } finally {
      uploadsInFlight--;
    }
  }

  static Future<Map<String, Object?>> _readJson(HttpRequest request) async {
    final text = await utf8.decoder.bind(request).join();
    if (text.trim().isEmpty) return {};
    final decoded = jsonDecode(text);
    return decoded is Map<String, Object?> ? decoded : {};
  }

  static Future<void> _json(HttpRequest request, Object? body) async {
    request.response.headers.contentType = ContentType.json;
    request.response.write(jsonEncode(body));
    await request.response.close();
  }
}

Future<void> main(List<String> args) async {
  var port = 8080;
  final index = args.indexOf('--port');
  if (index >= 0 && index + 1 < args.length) port = int.parse(args[index + 1]);
  final server = await DemoServer.start(port: port);
  stdout.writeln('durable_queue demo server on port ${server.port}');
  stdout.writeln('Android emulator: http://10.0.2.2:${server.port}');
  stdout.writeln('iOS simulator:    http://localhost:${server.port}');
  ProcessSignal.sigint.watch().listen((_) async {
    await server.close();
    exit(0);
  });
}
