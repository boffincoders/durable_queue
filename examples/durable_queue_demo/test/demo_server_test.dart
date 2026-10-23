import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';

import '../tool/demo_server.dart';

void main() {
  late DemoServer server;
  late HttpClient client;

  setUp(() async {
    server = await DemoServer.start(port: 0);
    client = HttpClient();
  });

  tearDown(() async {
    client.close(force: true);
    await server.close();
  });

  Future<(int, Map<String, Object?>)> call(
    String method,
    String path, {
    Object? json,
    List<int>? bytes,
    Map<String, String> headers = const {},
  }) async {
    final request = await client.openUrl(
      method,
      Uri.parse('http://127.0.0.1:${server.port}$path'),
    );
    headers.forEach(request.headers.set);
    if (json != null) request.write(jsonEncode(json));
    if (bytes != null) request.add(bytes);
    final response = await request.close();
    final text = await utf8.decoder.bind(response).join();
    final body = text.isEmpty ? <String, Object?>{} : jsonDecode(text);
    return (
      response.statusCode,
      body is Map<String, Object?> ? body : <String, Object?>{},
    );
  }

  test('counts uploads, keys, and announcements per photo', () async {
    final (status, body) = await call(
      'POST',
      '/upload?name=a.jpg',
      bytes: List.filled(1000, 1),
      headers: {'Idempotency-Key': 'task-1'},
    );
    expect(status, 201);
    expect(body['bytes'], 1000);
    await call(
      'POST',
      '/upload?name=a.jpg',
      bytes: [1],
      headers: {'Idempotency-Key': 'task-1'},
    );
    await call('POST', '/photos/a.jpg/done');
    expect(server.uploads, {'a.jpg': 2});
    expect(server.uploadKeys['a.jpg'], {'task-1'});
    expect(server.done, {'a.jpg': 1});
    final (_, stats) = await call('GET', '/stats');
    expect(stats['done'], {'a.jpg': 1});
    expect(stats['uploadsInFlight'], 0);
  });

  test('failEvery answers 503 and the upload does not count', () async {
    await call('POST', '/config', json: {'failEvery': 2});
    expect((await call('POST', '/upload?name=a', bytes: [1])).$1, 201);
    expect((await call('POST', '/upload?name=a', bytes: [1])).$1, 503);
    expect(server.uploads, {'a': 1});
    expect(server.uploadAttempts, {'a': 2});
  });

  test('a held upload is in flight until answered', () async {
    await call('POST', '/config', json: {'uploadDelayMs': 300});
    final pending = call('POST', '/upload?name=a', bytes: [1]);
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(server.uploadsInFlight, 1);
    await pending;
    expect(server.uploadsInFlight, 0);
  });

  test('scenario and report round-trip; reset clears', () async {
    await call('POST', '/scenario', json: {'action': 'enqueue', 'count': 2});
    expect((await call('GET', '/scenario')).$2['count'], 2);
    await call('POST', '/report', json: {'total': 4});
    expect(server.report, {'total': 4});
    expect((await call('GET', '/report')).$2, {'total': 4});
    await call('POST', '/reset');
    expect(server.report, isNull);
    expect((await call('GET', '/scenario')).$2, {'action': 'none'});
  });

  test('bad requests', () async {
    expect((await call('POST', '/upload', bytes: [1])).$1, 400);
    expect((await call('GET', '/nope')).$1, 404);
    expect((await call('GET', '/health')).$2, {'ok': true});
  });
}
