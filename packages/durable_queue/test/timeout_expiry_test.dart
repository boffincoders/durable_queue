import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  late QueueHarness h;

  setUp(() => h = QueueHarness());
  tearDown(() => h.queue.close());

  group('register(timeout:)', () {
    test('fails an attempt that runs too long, then retries it', () async {
      final release = Completer<void>();
      var calls = 0;
      var sawCancel = false;
      h.register(
        timeout: const Duration(seconds: 30),
        handler: (task, context) async {
          calls++;
          if (calls == 1) {
            unawaited(
              context.whenCancellationRequested.then((_) => sawCancel = true),
            );
            await release.future;
          }
        },
      );
      final id = await h.queue.enqueue(
        ValueTask('slow'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 2,
          delay: const Duration(seconds: 1),
        ),
      );
      await h.queue.start();
      await h.until(() => calls == 1);

      h.clock.advance(const Duration(seconds: 29));
      await pumpEventQueue();
      expect(h.of<TaskRetryScheduled>(), isEmpty);

      h.clock.advance(const Duration(seconds: 1));
      await h.until(() => h.of<TaskRetryScheduled>().isNotEmpty);
      expect(sawCancel, isTrue);
      final failure = (await h.queue.getTask(id))!.lastFailure!;
      expect(failure.error, contains('TaskTimeoutException'));
      expect(failure.error, contains('0:00:30'));

      // The abandoned handler finishing late changes nothing.
      release.complete();
      await pumpEventQueue();
      expect(h.of<TaskCompleted>(), isEmpty);

      h.clock.advance(const Duration(seconds: 1));
      await h.until(() => h.of<TaskCompleted>().length == 1);
      expect(calls, 2);
    });

    test('a timed-out attempt with no retries left fails the task', () async {
      h.register(
        timeout: const Duration(seconds: 5),
        handler: (task, context) => Completer<void>().future,
      );
      final id = await h.queue.enqueue(ValueTask('stuck'));
      await h.queue.start();
      await pumpEventQueue();
      h.clock.advance(const Duration(seconds: 5));
      await h.until(() => h.of<TaskFailed>().length == 1);
      expect((await h.queue.getTask(id))!.status, TaskStatus.failed);
    });

    test('a handler that finishes in time is not affected', () async {
      h.register(
        timeout: const Duration(seconds: 5),
        handler: (task, context) async {},
      );
      await h.queue.enqueue(ValueTask('quick'));
      await h.queue.start();
      await h.until(() => h.of<TaskCompleted>().length == 1);
      h.clock.advance(const Duration(seconds: 10));
      await pumpEventQueue();
      expect(h.of<TaskFailed>(), isEmpty);
      expect(h.of<TaskRetryScheduled>(), isEmpty);
    });

    test('a handler error before the timeout is reported as is', () async {
      h.register(
        timeout: const Duration(seconds: 5),
        handler: (task, context) async => throw TemporaryFailure(),
      );
      final id = await h.queue.enqueue(ValueTask('x'));
      await h.queue.start();
      await h.until(() => h.of<TaskFailed>().length == 1);
      expect(
        (await h.queue.getTask(id))!.lastFailure!.error,
        'TemporaryFailure',
      );
    });

    test('cancelling a timed-out attempt cancels instead of failing', () async {
      h.register(
        timeout: const Duration(seconds: 5),
        handler: (task, context) => Completer<void>().future,
      );
      final id = await h.queue.enqueue(ValueTask('x'));
      await h.queue.start();
      await pumpEventQueue();
      await h.queue.cancel(id);
      h.clock.advance(const Duration(seconds: 5));
      await h.until(() => h.of<TaskCancelled>().length == 1);
      expect((await h.queue.getTask(id))!.status, TaskStatus.cancelled);
      expect(h.of<TaskFailed>(), isEmpty);
    });

    test('time spent handed off does not count', () async {
      h.register(
        timeout: const Duration(seconds: 5),
        handler: (task, context) async => context.handOff('ref'),
      );
      final id = await h.queue.enqueue(ValueTask('upload'));
      await h.queue.start();
      await pumpEventQueue();
      h.clock.advance(const Duration(minutes: 10));
      await pumpEventQueue();
      expect((await h.queue.getTask(id))!.status, TaskStatus.running);
      expect(await h.queue.completeHandOff(id, reference: 'ref'), isTrue);
      expect((await h.queue.getTask(id))!.status, TaskStatus.completed);
    });

    test('rejects a timeout that is not positive', () {
      expect(
        () => h.register(timeout: Duration.zero),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => h.register(timeout: const Duration(seconds: -1)),
        throwsA(isA<ArgumentError>()),
      );
    });
  });

  group('enqueue(expiresAt:)', () {
    late List<String> ran;

    setUp(() {
      ran = [];
      h.register(handler: (task, context) async => ran.add(task.value));
    });

    test('a task that has not started by its expiry is cancelled', () async {
      final expiry = h.clock.now().add(const Duration(minutes: 5));
      final id = await h.queue.enqueue(ValueTask('typing'), expiresAt: expiry);
      expect((await h.queue.getTask(id))!.expiresAt, expiry);

      h.clock.advance(const Duration(minutes: 5));
      await h.queue.start();
      await h.until(() => h.of<TaskCancelled>().length == 1);
      expect(ran, isEmpty);
      expect(h.of<TaskStarted>(), isEmpty);
      expect(h.of<TaskCancelled>().single.wasRunning, isFalse);
      final stored = (await h.queue.getTask(id))!;
      expect(stored.status, TaskStatus.cancelled);
      expect(stored.lastFailure!.error, contains('TaskExpiredException'));
      expect(stored.lastFailure!.attempt, 0);
    });

    test('a task that starts before its expiry runs', () async {
      await h.queue.enqueue(
        ValueTask('a'),
        expiresAfter: const Duration(minutes: 5),
      );
      h.clock.advance(const Duration(minutes: 4, seconds: 59));
      await h.queue.start();
      await h.until(() => ran.length == 1);
      expect(h.of<TaskCancelled>(), isEmpty);
    });

    test('a retry after the expiry is not started', () async {
      var calls = 0;
      final local = QueueHarness();
      addTearDown(local.queue.close);
      local.register(
        handler: (task, context) async {
          calls++;
          throw TemporaryFailure();
        },
      );
      final id = await local.queue.enqueue(
        ValueTask('a'),
        expiresAfter: const Duration(seconds: 30),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 5,
          delay: const Duration(minutes: 1),
        ),
      );
      await local.queue.start();
      await local.until(() => local.of<TaskRetryScheduled>().length == 1);
      local.clock.advance(const Duration(minutes: 1));
      await local.until(() => local.of<TaskCancelled>().length == 1);
      expect(calls, 1);
      expect((await local.queue.getTask(id))!.status, TaskStatus.cancelled);
    });

    test('dependents settle as for a cancellation', () async {
      final parent = await h.queue.enqueue(
        ValueTask('parent'),
        expiresAfter: const Duration(seconds: 1),
      );
      final child = await h.queue.enqueue(
        ValueTask('child'),
        dependsOn: [parent],
      );
      h.clock.advance(const Duration(seconds: 1));
      await h.queue.start();
      await h.until(() => h.of<TaskCancelled>().length == 2);
      expect((await h.queue.getTask(child))!.status, TaskStatus.cancelled);
      expect(ran, isEmpty);
    });

    test('expiry survives a round trip through JSON', () async {
      final expiry = h.clock.now().add(const Duration(hours: 1));
      final id = await h.queue.enqueue(ValueTask('a'), expiresAt: expiry);
      final copy = await reloadStorage(h.storage);
      expect((await copy.get(id))!.expiresAt, expiry);
    });

    test('retry clears an expiry that has passed', () async {
      final id = await h.queue.enqueue(
        ValueTask('a'),
        expiresAfter: const Duration(seconds: 1),
      );
      h.clock.advance(const Duration(seconds: 1));
      await h.queue.start();
      await h.until(() => h.of<TaskCancelled>().length == 1);
      await h.queue.retry(id);
      expect((await h.queue.getTask(id))!.expiresAt, isNull);
      await h.until(() => ran.length == 1);
    });

    test('retry keeps an expiry that has not passed', () async {
      final expiry = h.clock.now().add(const Duration(hours: 1));
      final id = await h.queue.enqueue(ValueTask('a'), expiresAt: expiry);
      await h.queue.cancel(id);
      await h.queue.retry(id);
      expect((await h.queue.getTask(id))!.expiresAt, expiry);
    });

    test('enqueueChain applies the expiry to every step', () async {
      final ids = await h.queue.enqueueChain([
        ValueTask('a'),
        ValueTask('b'),
      ], expiresAfter: const Duration(minutes: 1));
      final expected = h.clock.now().add(const Duration(minutes: 1));
      for (final id in ids) {
        expect((await h.queue.getTask(id))!.expiresAt, expected);
      }
    });

    test('rejects invalid expiries', () async {
      final now = h.clock.now();
      expect(
        () => h.queue.enqueue(
          ValueTask('a'),
          expiresAt: now.add(const Duration(minutes: 1)),
          expiresAfter: const Duration(minutes: 1),
        ),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => h.queue.enqueue(ValueTask('a'), expiresAt: now),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => h.queue.enqueue(ValueTask('a'), expiresAfter: Duration.zero),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => h.queue.enqueue(
          ValueTask('a'),
          delay: const Duration(hours: 1),
          expiresAfter: const Duration(minutes: 30),
        ),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => h.queue.enqueueChain([
          ValueTask('a'),
        ], expiresAt: now.subtract(const Duration(seconds: 1))),
        throwsA(isA<ArgumentError>()),
      );
      expect(await h.queue.getTasks(), isEmpty);
    });
  });

  group('counts', () {
    setUp(() => h.register(handler: (task, context) async {}));

    test('counts tasks in every status, with zeros', () async {
      final empty = await h.queue.counts();
      expect(empty.keys.toSet(), TaskStatus.values.toSet());
      expect(empty.values.every((count) => count == 0), isTrue);

      final a = await h.queue.enqueue(ValueTask('a'), group: 'photos');
      await h.queue.enqueue(ValueTask('b'), group: 'photos');
      await h.queue.enqueue(ValueTask('b2'), group: 'photos');
      await h.queue.enqueue(ValueTask('c'));
      await h.queue.enqueue(ValueTask('d'), dependsOn: [a]);
      await h.queue.cancel(a);

      final all = await h.queue.counts();
      expect(all[TaskStatus.pending], 3);
      expect(all[TaskStatus.cancelled], 2);
      expect(all[TaskStatus.completed], 0);

      final photos = await h.queue.counts(group: 'photos');
      expect(photos[TaskStatus.pending], 2);
      expect(photos[TaskStatus.cancelled], 1);
      expect(photos.values.fold<int>(0, (sum, n) => sum + n), 3);
    });
  });

  group('waitFor', () {
    test('returns the record once the task finishes', () async {
      h.register(handler: (task, context) async {});
      final id = await h.queue.enqueue(ValueTask('a'));
      final waiting = h.queue.waitFor(id);
      await h.queue.start();
      final done = await waiting;
      expect(done.id, id);
      expect(done.status, TaskStatus.completed);
    });

    test('returns at once for a finished task', () async {
      h.register(handler: (task, context) async {});
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.cancel(id);
      expect((await h.queue.waitFor(id)).status, TaskStatus.cancelled);
    });

    test('reports failure through the record', () async {
      h.register(handler: (task, context) async => throw TemporaryFailure());
      final id = await h.queue.enqueue(
        ValueTask('a'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 2,
          delay: const Duration(seconds: 1),
        ),
      );
      final waiting = h.queue.waitFor(id);
      await h.queue.start();
      await h.until(() => h.of<TaskRetryScheduled>().length == 1);
      h.clock.advance(const Duration(seconds: 1));
      final done = await waiting;
      expect(done.status, TaskStatus.failed);
      expect(done.attempts, 2);
    });

    test('throws for an unknown id', () {
      expect(h.queue.waitFor('missing'), throwsA(isA<TaskNotFoundException>()));
    });

    test('times out on the queue clock', () async {
      h.register(handler: (task, context) async {});
      final id = await h.queue.enqueue(ValueTask('a'));
      final waiting = h.queue.waitFor(id, timeout: const Duration(seconds: 5));
      final check = expectLater(waiting, throwsA(isA<TimeoutException>()));
      await pumpEventQueue();
      h.clock.advance(const Duration(seconds: 5));
      await check;
    });

    test('throws when the queue closes first', () async {
      h.register(handler: (task, context) async {});
      final id = await h.queue.enqueue(ValueTask('a'));
      final waiting = h.queue.waitFor(id);
      final check = expectLater(waiting, throwsA(isA<StateError>()));
      await pumpEventQueue();
      await h.queue.close();
      await check;
    });
  });
}
