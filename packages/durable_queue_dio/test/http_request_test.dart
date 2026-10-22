import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dio/dio.dart';
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_dio/durable_queue_dio.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:test/test.dart';

import 'support/server.dart';

/// A queue that sends [HttpRequestTask]s with a real Dio client.
final class Harness {
  Harness(
    this.server, {
    QueueStorage? storage,
    DioRetryRules rules = const DioRetryRules(),
    Dio? dio,
    HttpResponseCallback? onResponse,
    void Function(DurableQueue queue)? setup,
    Set<String> requires = const {},
    DeferOnError? deferOn,
    RateLimit? rateLimit,
  }) {
    queue = DurableQueue(
      storage: storage ?? MemoryQueueStorage(),
      clock: clock,
      randomFraction: () => 0.5,
    );
    setup?.call(queue);
    queue.registerHttpRequests(
      dio ?? Dio(),
      rules: rules,
      onResponse: onResponse,
      requires: requires,
      deferOn: deferOn,
      rateLimit: rateLimit,
    );
    queue.events.listen(events.add);
  }

  final ScriptedServer server;
  final clock = FakeQueueClock(DateTime.utc(2026, 10, 13, 12));
  late final DurableQueue queue;
  final events = <QueueEvent>[];

  List<T> of<T>() => events.whereType<T>().toList();

  /// Real network I/O: poll on wall-clock time.
  Future<void> until(bool Function() condition) async {
    for (var i = 0; i < 500 && !condition(); i++) {
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
    expect(condition(), isTrue, reason: 'events: $events');
  }
}

final retries = RetryPolicy.exponential(
  maxAttempts: 4,
  initialDelay: const Duration(seconds: 1),
);

void main() {
  late ScriptedServer server;

  setUp(() async => server = await ScriptedServer.start());
  tearDown(() => server.close());

  test('503 with Retry-After waits that long, then succeeds', () async {
    server.script('/messages', [
      respond(503, headers: {'Retry-After': '7'}),
      respond(201, body: {'id': 1}),
    ]);
    final h = Harness(server);
    await h.queue.start();
    await h.queue.enqueue(
      HttpRequestTask(
        method: 'post',
        url: server.url('/messages'),
        body: {'text': 'hello'},
      ),
      idempotencyKey: 'message-42',
      retryPolicy: retries,
    );

    await h.until(() => h.of<TaskRetryScheduled>().isNotEmpty);
    expect(h.of<TaskRetryScheduled>().single.delay, const Duration(seconds: 7));
    h.clock.advance(const Duration(seconds: 6));
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(server.received, hasLength(1));

    h.clock.advance(const Duration(seconds: 1));
    await h.until(() => h.of<TaskCompleted>().isNotEmpty);
    expect(server.received, hasLength(2));
    for (final request in server.received) {
      expect(request.method, 'POST');
      expect(request.headers.value('idempotency-key'), 'message-42');
      expect(jsonDecode(request.body), {'text': 'hello'});
    }
  });

  test('429 with an HTTP-date Retry-After', () async {
    server.script('/limited', [
      respond(429, headers: {'Retry-After': 'Tue, 13 Oct 2026 12:01:30 GMT'}),
      respond(200),
    ]);
    final h = Harness(
      server,
      rules: DioRetryRules(now: () => DateTime.utc(2026, 10, 13, 12)),
    );
    await h.queue.start();
    await h.queue.enqueue(
      HttpRequestTask(method: 'GET', url: server.url('/limited')),
      retryPolicy: retries,
    );
    await h.until(() => h.of<TaskRetryScheduled>().isNotEmpty);
    expect(
      h.of<TaskRetryScheduled>().single.delay,
      const Duration(seconds: 90),
    );
  });

  test('a 5xx without Retry-After uses the retry policy', () async {
    server.script('/flaky', [respond(502), respond(200)]);
    final h = Harness(server);
    await h.queue.start();
    await h.queue.enqueue(
      HttpRequestTask(method: 'GET', url: server.url('/flaky')),
      retryPolicy: retries,
    );
    await h.until(() => h.of<TaskRetryScheduled>().isNotEmpty);
    expect(h.of<TaskRetryScheduled>().single.delay, const Duration(seconds: 1));
    h.clock.advance(const Duration(seconds: 1));
    await h.until(() => h.of<TaskCompleted>().isNotEmpty);
  });

  test('a 4xx fails at once', () async {
    server.script('/bad', [
      respond(422, body: {'error': 'invalid'}),
    ]);
    final h = Harness(server);
    await h.queue.start();
    final id = await h.queue.enqueue(
      HttpRequestTask(method: 'POST', url: server.url('/bad'), body: {}),
      retryPolicy: retries,
    );
    await h.until(() => h.of<TaskFailed>().isNotEmpty);
    expect(server.received, hasLength(1));
    final task = (await h.queue.getTask(id))!;
    expect(task.status, TaskStatus.failed);
    expect(task.lastFailure?.error, contains('422'));
  });

  test('a refused connection is retried', () async {
    final port = server.url('/').split(':').last.replaceAll('/', '');
    await server.close();
    final h = Harness(server);
    await h.queue.start();
    await h.queue.enqueue(
      HttpRequestTask(method: 'GET', url: 'http://127.0.0.1:$port/gone'),
      retryPolicy: retries,
    );
    await h.until(() => h.of<TaskRetryScheduled>().isNotEmpty);
    expect(
      h.of<TaskRetryScheduled>().single.error,
      contains('connection error'),
    );
  });

  test('a receive timeout is retried', () async {
    final never = Completer<void>();
    server.script('/slow', [holdUntil(never.future)]);
    final h = Harness(
      server,
      dio: Dio(BaseOptions(receiveTimeout: const Duration(milliseconds: 100))),
    );
    await h.queue.start();
    await h.queue.enqueue(
      HttpRequestTask(method: 'GET', url: server.url('/slow')),
      retryPolicy: retries,
    );
    await h.until(() => h.of<TaskRetryScheduled>().isNotEmpty);
    expect(
      h.of<TaskRetryScheduled>().single.error,
      contains('receive timeout'),
    );
    never.complete();
  });

  test('cancelling the task aborts the request in flight', () async {
    final release = Completer<void>();
    server.script('/hold', [holdUntil(release.future)]);
    final h = Harness(server);
    await h.queue.start();
    final id = await h.queue.enqueue(
      HttpRequestTask(method: 'GET', url: server.url('/hold')),
      retryPolicy: retries,
    );
    await h.until(() => server.received.isNotEmpty);

    await h.queue.cancel(id);
    await h.until(() => h.of<TaskCancelled>().isNotEmpty);
    expect(h.of<TaskCancelled>().single.wasRunning, isTrue);
    expect((await h.queue.getTask(id))?.status, TaskStatus.cancelled);
    expect(h.of<TaskRetryScheduled>(), isEmpty);
    release.complete();
  });

  test('headers, query, text bodies, and onResponse', () async {
    server.script('/echo', [
      respond(200, body: {'ok': true}),
    ]);
    Response<Object?>? seen;
    final h = Harness(
      server,
      onResponse: (task, response, context) => seen = response,
    );
    await h.queue.start();
    await h.queue.enqueue(
      HttpRequestTask(
        method: 'PUT',
        url: server.url('/echo'),
        headers: {'X-Client': 'app'},
        queryParameters: {'v': 2, 'tag': 'a'},
        body: 'plain text',
        contentType: 'text/plain',
      ),
    );
    await h.until(() => h.of<TaskCompleted>().isNotEmpty);

    final request = server.received.single;
    expect(request.method, 'PUT');
    expect(request.uri.queryParameters, {'v': '2', 'tag': 'a'});
    expect(request.headers.value('x-client'), 'app');
    expect(request.headers.contentType?.mimeType, 'text/plain');
    expect(request.body, 'plain text');
    expect(request.headers.value('idempotency-key'), isNull);
    expect(seen?.data, {'ok': true});
  });

  test('a queued request survives an app restart (SQLite)', () async {
    final dir = Directory.systemTemp.createTempSync('dq_dio_');
    addTearDown(() => dir.deleteSync(recursive: true));
    final path = '${dir.path}/queue.db';
    server.script('/later', [respond(204)]);

    final offline = SqliteQueueStorage.open(path);
    final first = Harness(server, storage: offline);
    await first.queue.enqueue(
      HttpRequestTask(method: 'DELETE', url: server.url('/later')),
      idempotencyKey: 'delete-7',
    );
    await first.queue.close();
    await offline.close();
    expect(server.received, isEmpty);

    final storage = SqliteQueueStorage.open(path);
    final second = Harness(server, storage: storage);
    await second.queue.start();
    await second.until(() => second.of<TaskCompleted>().isNotEmpty);
    expect(server.received.single.method, 'DELETE');
    expect(server.received.single.headers.value('idempotency-key'), 'delete-7');
    await second.queue.close();
    await storage.close();
  });

  group('HttpRequestTask', () {
    test('round-trips through JSON', () {
      final task = HttpRequestTask(
        method: 'patch',
        url: '/users/1',
        headers: {'A': 'b'},
        queryParameters: {'x': 1},
        body: {
          'name': 'Ada',
          'tags': ['x'],
        },
        contentType: 'application/json',
      );
      final copy = HttpRequestTask.fromJson(
        jsonDecode(jsonEncode(task.toJson())) as Map<String, dynamic>,
      );
      expect(copy.toJson(), task.toJson());
      expect(copy.method, 'PATCH');
      expect(copy.type, HttpRequestTask.taskType);
    });

    test('rejects an empty method or url', () {
      expect(
        () => HttpRequestTask(method: ' ', url: '/x'),
        throwsArgumentError,
      );
      expect(
        () => HttpRequestTask(method: 'GET', url: ''),
        throwsArgumentError,
      );
    });
  });

  group('TaskContext helpers', () {
    TaskContext context({String? key, Future<void>? cancel}) => TaskContext(
      taskId: 't',
      attempt: 1,
      maxAttempts: 1,
      idempotencyKey: key,
      deduplicationKey: null,
      isCancellationRequested: () => false,
      whenCancellationRequested: cancel,
    );

    test('withIdempotencyKey adds the header and keeps others', () {
      final options = context(key: 'k1').withIdempotencyKey(
        Options(headers: {'A': 'b'}),
        header: 'X-Request-Id',
      );
      expect(options.headers, {'A': 'b', 'X-Request-Id': 'k1'});
      expect(context().withIdempotencyKey(null).headers, isNull);
    });

    test('dioCancelToken follows the cancellation signal', () async {
      final cancel = Completer<void>();
      final token = context(cancel: cancel.future).dioCancelToken();
      expect(token.isCancelled, isFalse);
      cancel.complete();
      await pumpEventQueue();
      expect(token.isCancelled, isTrue);
    });
  });

  group('gates and limits', () {
    test('requests wait for the network gate, then go out', () async {
      server.script('/a', [respond(200)]);
      final network = ManualQueueGate(open: false);
      final h = Harness(
        server,
        setup: (queue) => queue.addGate('network', network),
        requires: {'network'},
      );
      await h.queue.start();
      await h.queue.enqueue(
        HttpRequestTask(method: 'GET', url: server.url('/a')),
      );
      await h.until(() => h.of<TaskDeferred>().isNotEmpty);
      expect(h.of<TaskDeferred>().single.reason, DeferralReason.gateClosed);
      expect(server.received, isEmpty);

      network.open();
      await h.until(() => h.of<TaskCompleted>().isNotEmpty);
      expect(server.received, hasLength(1));
      await h.queue.close();
    });

    test(
      'a connection error waits for the gate without using an attempt',
      () async {
        // A port with nothing listening: Dio reports connectionError.
        final closed = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
        final deadUrl = 'http://127.0.0.1:${closed.port}/x';
        await closed.close();

        const rules = DioRetryRules();
        final h = Harness(
          server,
          rules: rules,
          setup: (queue) => queue.addGate('network', ManualQueueGate()),
          requires: {'network'},
          deferOn: rules.deferOn('network'),
        );
        await h.queue.start();
        final id = await h.queue.enqueue(
          HttpRequestTask(method: 'POST', url: deadUrl),
        );
        await h.until(() => h.of<TaskDeferred>().isNotEmpty);
        final deferred = h.of<TaskDeferred>().single;
        expect(deferred.reason, DeferralReason.gateError);
        expect(deferred.name, 'network');
        final stored = (await h.queue.getTask(id))!;
        expect(stored.status, TaskStatus.pending);
        expect(stored.attempts, 0);
        expect(h.of<TaskFailed>(), isEmpty);
        await h.queue.close();
      },
    );

    test('per-host limits give each host its own budget', () async {
      final other = await ScriptedServer.start();
      server.script('/a', [respond(200)]);
      other.script('/b', [respond(200)]);
      final h = Harness(
        server,
        setup: (queue) => queue.addHttpHostRateLimit(
          RateLimit(1, per: const Duration(minutes: 1)),
        ),
      );
      await h.queue.start();
      for (final url in [server.url('/a'), server.url('/a'), other.url('/b')]) {
        await h.queue.enqueue(HttpRequestTask(method: 'GET', url: url));
      }
      await h.until(() => h.of<TaskCompleted>().length == 2);
      expect(server.received, hasLength(1));
      expect(other.received, hasLength(1));
      final deferred = h.of<TaskDeferred>().single;
      expect(deferred.name, 'http-host');
      expect(deferred.reason, DeferralReason.rateLimit);

      h.clock.advance(const Duration(minutes: 1));
      await h.until(() => h.of<TaskCompleted>().length == 3);
      expect(server.received, hasLength(2));
      await h.queue.close();
      await other.close();
    });

    test('relative URLs use the Dio baseUrl for the host', () async {
      server.script('/a', [respond(200)]);
      final dio = Dio(BaseOptions(baseUrl: server.url('')));
      final h = Harness(
        server,
        dio: dio,
        setup: (queue) => queue.addHttpHostRateLimit(
          RateLimit(1, per: const Duration(minutes: 1)),
          dio: dio,
        ),
      );
      await h.queue.start();
      await h.queue.enqueue(HttpRequestTask(method: 'GET', url: '/a'));
      await h.queue.enqueue(HttpRequestTask(method: 'GET', url: '/a'));
      await h.until(() => h.of<TaskDeferred>().isNotEmpty);
      expect(server.received, hasLength(1));
      await h.queue.close();
    });

    test('rateLimit limits all queued requests together', () async {
      server.script('/a', [respond(200)]);
      final h = Harness(
        server,
        rateLimit: RateLimit(1, per: const Duration(seconds: 30)),
      );
      await h.queue.start();
      await h.queue.enqueue(
        HttpRequestTask(method: 'GET', url: server.url('/a')),
      );
      await h.queue.enqueue(
        HttpRequestTask(method: 'GET', url: server.url('/a')),
      );
      await h.until(() => h.of<TaskDeferred>().isNotEmpty);
      expect(
        h.of<TaskDeferred>().single.name,
        'type:${HttpRequestTask.taskType}',
      );
      await h.queue.close();
    });
  });

  group('host hold after Retry-After', () {
    test('a 429 with Retry-After makes the whole host wait', () async {
      final other = await ScriptedServer.start();
      server.script('/a1', [
        respond(429, headers: {'Retry-After': '30'}),
        respond(200),
      ]);
      server.script('/a2', [respond(200)]);
      other.script('/b1', [respond(200)]);
      final h = Harness(
        server,
        setup: (queue) => queue.addHttpHostRateLimit(
          RateLimit(5, per: const Duration(seconds: 1)),
        ),
      );
      await h.queue.start();
      await h.queue.enqueue(
        HttpRequestTask(method: 'GET', url: server.url('/a1')),
        retryPolicy: retries,
      );
      await h.queue.enqueue(
        HttpRequestTask(method: 'GET', url: server.url('/a2')),
      );
      await h.queue.enqueue(
        HttpRequestTask(method: 'GET', url: other.url('/b1')),
      );

      await h.until(() => other.received.isNotEmpty);
      // a2 is held with the host; a1 waits for its retry.
      expect(server.received.map((r) => r.uri.path), ['/a1']);
      final held = h.of<TaskDeferred>().where((e) => e.name == 'http-host');
      expect(held, isNotEmpty);
      expect(
        held.first.until.difference(h.clock.now()),
        const Duration(seconds: 30),
      );

      h.clock.advance(const Duration(seconds: 31));
      await h.until(() => h.of<TaskCompleted>().length == 3);
      expect(server.received.map((r) => r.uri.path).toSet(), {'/a1', '/a2'});
      await h.queue.close();
      await other.close();
    });

    test('without a per-host limit nothing is held', () async {
      server.script('/a1', [
        respond(429, headers: {'Retry-After': '30'}),
        respond(200),
      ]);
      server.script('/a2', [respond(200)]);
      final h = Harness(server);
      await h.queue.start();
      await h.queue.enqueue(
        HttpRequestTask(method: 'GET', url: server.url('/a1')),
        retryPolicy: retries,
      );
      await h.queue.enqueue(
        HttpRequestTask(method: 'GET', url: server.url('/a2')),
      );
      await h.until(() => server.received.length == 2);
      expect(h.of<TaskDeferred>(), isEmpty);
      await h.queue.close();
    });

    test('a 500 does not hold the host', () async {
      server.script('/a1', [
        respond(500, headers: {'Retry-After': '30'}),
        respond(200),
      ]);
      server.script('/a2', [respond(200)]);
      final h = Harness(
        server,
        setup: (queue) => queue.addHttpHostRateLimit(
          RateLimit(5, per: const Duration(seconds: 1)),
        ),
      );
      await h.queue.start();
      await h.queue.enqueue(
        HttpRequestTask(method: 'GET', url: server.url('/a1')),
        retryPolicy: retries,
      );
      await h.queue.enqueue(
        HttpRequestTask(method: 'GET', url: server.url('/a2')),
      );
      await h.until(() => server.received.length == 2);
      expect(h.of<TaskDeferred>(), isEmpty);
      await h.queue.close();
    });
  });
}
