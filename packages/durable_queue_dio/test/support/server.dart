import 'dart:async';
import 'dart:convert';
import 'dart:io';

/// One request the server received.
final class Received {
  Received(this.method, this.uri, this.headers, this.body);

  final String method;
  final Uri uri;
  final HttpHeaders headers;
  final String body;
}

/// Answers one request.
typedef Responder = Future<void> Function(HttpRequest request);

/// Local HTTP server that answers each path from a script of responders.
final class ScriptedServer {
  ScriptedServer._(this._server) {
    _server.listen(_handle);
  }

  static Future<ScriptedServer> start() async =>
      ScriptedServer._(await HttpServer.bind(InternetAddress.loopbackIPv4, 0));

  final HttpServer _server;
  final _scripts = <String, List<Responder>>{};
  final received = <Received>[];

  String url(String path) => 'http://127.0.0.1:${_server.port}$path';

  /// Responders for [path], used in order; the last one repeats.
  void script(String path, List<Responder> responders) =>
      _scripts[path] = [...responders];

  Future<void> close() => _server.close(force: true);

  Future<void> _handle(HttpRequest request) async {
    final body = await utf8.decoder.bind(request).join();
    received.add(Received(request.method, request.uri, request.headers, body));
    final script = _scripts[request.uri.path];
    if (script == null || script.isEmpty) {
      request.response.statusCode = 404;
      await request.response.close();
      return;
    }
    final responder = script.length > 1 ? script.removeAt(0) : script.first;
    await responder(request);
  }
}

/// Responds with [status], optional [headers], and a JSON [body].
Responder respond(
  int status, {
  Map<String, String> headers = const {},
  Object? body,
}) {
  return (request) async {
    request.response.statusCode = status;
    headers.forEach(request.response.headers.set);
    if (body != null) {
      request.response.headers.contentType = ContentType.json;
      request.response.write(jsonEncode(body));
    }
    await request.response.close();
  };
}

/// Waits for [release] before responding 200.
Responder holdUntil(Future<void> release) {
  return (request) async {
    await release;
    request.response.statusCode = 200;
    await request.response.close();
  };
}
