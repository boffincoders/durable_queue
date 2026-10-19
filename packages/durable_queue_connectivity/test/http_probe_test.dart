import 'dart:async';
import 'dart:io';

import 'package:durable_queue_connectivity/durable_queue_connectivity.dart';
import 'package:test/test.dart';

void main() {
  late HttpServer server;
  late Uri base;
  final seen = <String>[];

  setUpAll(() async {
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    base = Uri.parse('http://127.0.0.1:${server.port}');
    server.listen((request) async {
      seen.add(
        '${request.method} ${request.uri.path} '
        '${request.headers.value('x-key') ?? '-'}',
      );
      final response = request.response;
      switch (request.uri.path) {
        case '/ok':
          response.statusCode = 204;
        case '/missing':
          response.statusCode = 404;
        case '/broken':
          response.statusCode = 503;
        case '/portal':
          response.statusCode = 302;
          response.headers.set('location', '$base/login');
        case '/slow':
          await Future<void>.delayed(const Duration(seconds: 2));
          response.statusCode = 200;
        default:
          response.statusCode = 200;
      }
      await response.close();
    });
  });

  tearDownAll(() => server.close(force: true));

  test('2xx and 4xx mean reachable; 5xx and redirects do not', () async {
    expect(await HttpProbe(base.resolve('/ok')).call(), isTrue);
    expect(await HttpProbe(base.resolve('/missing')).call(), isTrue);
    expect(await HttpProbe(base.resolve('/broken')).call(), isFalse);
    expect(await HttpProbe(base.resolve('/portal')).call(), isFalse);
    // The redirect was not followed.
    expect(seen.where((line) => line.contains('/login')), isEmpty);
  });

  test('uses HEAD by default and sends headers', () async {
    seen.clear();
    await HttpProbe(base.resolve('/ok'), headers: {'x-key': 'k1'}).call();
    await HttpProbe(base.resolve('/ok'), method: 'GET').call();
    expect(seen, ['HEAD /ok k1', 'GET /ok -']);
  });

  test('a custom isReachable decides', () async {
    final strict = HttpProbe(
      base.resolve('/missing'),
      isReachable: (status) => status == 204,
    );
    expect(await strict.call(), isFalse);
    final lenient = HttpProbe(
      base.resolve('/portal'),
      isReachable: (status) => status < 400,
    );
    expect(await lenient.call(), isTrue);
  });

  test('timeouts and connection failures return false', () async {
    final slow = HttpProbe(
      base.resolve('/slow'),
      timeout: const Duration(milliseconds: 100),
    );
    expect(await slow.call(), isFalse);

    final closed = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final port = closed.port;
    await closed.close();
    expect(
      await HttpProbe(Uri.parse('http://127.0.0.1:$port/')).call(),
      isFalse,
    );
  });

  test('validates timeout', () {
    expect(() => HttpProbe(base, timeout: Duration.zero), throwsArgumentError);
    expect(HttpProbe(base).toString(), 'HttpProbe(HEAD $base)');
  });
}
