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
      case ('GET', '/'):
        request.response.headers.contentType = ContentType.html;
        request.response.write(_page);
        return request.response.close();
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

/// A page that shows what the server received, updated every second. Open
/// it next to the app when recording a demo.
const _page = '''<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Demo server</title>
<style>
body{font:16px system-ui,sans-serif;margin:24px;color:#1b1b1f;background:#f7f7fa}
h1{font-size:22px;margin:0 0 4px}p{color:#5e5e66;margin:0 0 16px}
table{border-collapse:collapse;width:100%;max-width:640px}
td,th{text-align:left;padding:8px;border-bottom:1px solid #ddd}
.ok{color:#1a7f37;font-weight:600}.wait{color:#9a6700}
</style></head><body>
<h1>What the server received</h1>
<p id="summary">Waiting for the app...</p>
<table><thead><tr><th>Photo</th><th>Upload</th><th>Server told</th></tr></thead>
<tbody id="rows"></tbody></table>
<script>
async function refresh(){
  try{
    const s=await (await fetch('/stats')).json();
    const names=[...new Set([...Object.keys(s.uploadAttempts),...Object.keys(s.done)])].sort();
    const rows=names.map(n=>{
      const up=s.uploads[n]||0, tries=s.uploadAttempts[n]||0, done=s.done[n]||0;
      const upText=up?'<span class="ok">received</span>'+(tries>1?' ('+tries+' tries)':''):'<span class="wait">in progress</span>';
      const doneText=done?'<span class="ok">yes'+(done>1?' ('+done+' times)':'')+'</span>':'';
      return '<tr><td>'+n+'</td><td>'+upText+'</td><td>'+doneText+'</td></tr>';
    });
    document.getElementById('rows').innerHTML=rows.join('');
    const told=Object.keys(s.done).length;
    document.getElementById('summary').textContent=names.length
      ? told+' of '+names.length+' photos complete, '+s.uploadsInFlight+' uploads in flight'
      : 'Waiting for the app...';
  }catch(e){}
}
refresh();setInterval(refresh,1000);
</script></body></html>''';

Future<void> main(List<String> args) async {
  var port = 8080;
  final index = args.indexOf('--port');
  if (index >= 0 && index + 1 < args.length) port = int.parse(args[index + 1]);
  final server = await DemoServer.start(port: port);
  stdout.writeln('durable_queue demo server on port ${server.port}');
  stdout.writeln('Live view:        http://localhost:${server.port}/');
  stdout.writeln('Android emulator: http://10.0.2.2:${server.port}');
  stdout.writeln('iOS simulator:    http://localhost:${server.port}');
  ProcessSignal.sigint.watch().listen((_) async {
    await server.close();
    exit(0);
  });
}
