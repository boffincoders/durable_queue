import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

/// An error that knows how long the caller should wait.
final class RateLimited implements Exception {
  RateLimited(this.wait);
  final Duration wait;
}

void main() {
  group('retryDelay', () {
    final policy = RetryPolicy.exponential(
      maxAttempts: 4,
      initialDelay: const Duration(seconds: 1),
      jitter: true,
    );

    Future<QueueHarness> failingOnce(
      RetryDelayResolver? resolver, {
      Object Function()? error,
      RetryPredicate? retryIf,
    }) async {
      final h = QueueHarness();
      var calls = 0;
      h.register(
        handler: (task, context) async {
          if (++calls == 1) throw (error ?? () => TemporaryFailure())();
        },
        retryIf: retryIf,
        retryDelay: resolver,
      );
      await h.queue.enqueue(ValueTask('a'), retryPolicy: policy);
      await h.queue.start();
      await h.until(
        () =>
            h.of<TaskRetryScheduled>().isNotEmpty ||
            h.of<TaskFailed>().isNotEmpty,
      );
      return h;
    }

    test('the error chooses the delay, without jitter', () async {
      final h = await failingOnce(
        (error, _) => error is RateLimited ? error.wait : null,
        error: () => RateLimited(const Duration(seconds: 30)),
      );
      final event = h.of<TaskRetryScheduled>().single;
      expect(event.delay, const Duration(seconds: 30));
      expect(event.nextAttemptAt, h.clock.now().add(event.delay));
      final stored = (await h.queue.getTasks()).single;
      expect(stored.nextAttemptAt, event.nextAttemptAt);

      h.clock.advance(const Duration(seconds: 29));
      await pumpEventQueue();
      expect(h.of<TaskCompleted>(), isEmpty);
      h.clock.advance(const Duration(seconds: 1));
      await h.until(() => h.of<TaskCompleted>().isNotEmpty);
    });

    test('null falls back to the retry policy', () async {
      final h = await failingOnce((error, _) => null);
      // Exponential, 1 s initial, jitter with a fixed 0.5 fraction: exact.
      expect(
        h.of<TaskRetryScheduled>().single.delay,
        policy.delayAfter(1, 0.5),
      );
    });

    test('a resolver that throws falls back to the retry policy', () async {
      final h = await failingOnce((error, _) => throw StateError('bug'));
      expect(
        h.of<TaskRetryScheduled>().single.delay,
        policy.delayAfter(1, 0.5),
      );
    });

    test('negative delays become zero and huge ones are capped', () async {
      final soon = await failingOnce((_, _) => const Duration(seconds: -5));
      expect(soon.of<TaskRetryScheduled>().single.delay, Duration.zero);
      await soon.until(() => soon.of<TaskCompleted>().isNotEmpty);

      final late = await failingOnce((_, _) => const Duration(days: 9999));
      expect(
        late.of<TaskRetryScheduled>().single.delay,
        RetryPolicy.maxSchedulableDelay,
      );
    });

    test('is not consulted when the task will not be retried', () async {
      var asked = 0;
      final h = await failingOnce((_, _) {
        asked++;
        return const Duration(seconds: 1);
      }, retryIf: (_, _) => false);
      expect(h.of<TaskFailed>(), hasLength(1));
      expect(asked, 0);
    });

    test('the attempt still counts toward maxAttempts', () async {
      final h = QueueHarness();
      h.register(
        handler: (_, _) async => throw TemporaryFailure(),
        retryDelay: (_, _) => Duration.zero,
      );
      await h.queue.enqueue(
        ValueTask('a'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 3,
          delay: const Duration(hours: 1),
        ),
      );
      await h.queue.start();
      await h.until(() => h.of<TaskFailed>().isNotEmpty);
      expect(h.of<TaskStarted>(), hasLength(3));
      expect(h.of<TaskRetryScheduled>().map((e) => e.delay).toSet(), {
        Duration.zero,
      });
    });
  });

  group('whenCancellationRequested', () {
    test('completes when a running task is cancelled', () async {
      final h = QueueHarness();
      final started = Completer<TaskContext>();
      bool? flagWhenSignalled;
      h.register(
        handler: (task, context) async {
          started.complete(context);
          await context.whenCancellationRequested;
          flagWhenSignalled = context.isCancellationRequested;
          throw const TaskCancelledException();
        },
      );
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      final context = await started.future;
      var signalled = false;
      unawaited(
        context.whenCancellationRequested.then((_) => signalled = true),
      );
      await pumpEventQueue();
      expect(signalled, isFalse);

      await h.queue.cancel(id);
      await h.until(() => h.of<TaskCancelled>().isNotEmpty);
      expect(signalled, isTrue);
      expect(flagWhenSignalled, isTrue);
    });

    test('cancelGroup signals running tasks too', () async {
      final h = QueueHarness();
      h.register(
        handler: (task, context) async {
          await context.whenCancellationRequested;
          throw const TaskCancelledException();
        },
      );
      await h.queue.enqueue(ValueTask('a'), group: 'sync');
      await h.queue.start();
      await h.until(() => h.of<TaskStarted>().isNotEmpty);
      expect(await h.queue.cancelGroup('sync'), 1);
      await h.until(() => h.of<TaskCancelled>().isNotEmpty);
    });

    test('never completes for a task that is not cancelled', () async {
      final h = QueueHarness();
      late Future<void> signal;
      h.register(
        handler: (task, context) async {
          signal = context.whenCancellationRequested;
        },
      );
      await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await h.until(() => h.of<TaskCompleted>().isNotEmpty);
      var completed = false;
      unawaited(signal.then((_) => completed = true));
      await pumpEventQueue();
      expect(completed, isFalse);
    });

    test('a context built by hand has a signal that never completes', () async {
      final context = TaskContext(
        taskId: 'a',
        attempt: 1,
        maxAttempts: 1,
        idempotencyKey: null,
        deduplicationKey: null,
        isCancellationRequested: () => false,
      );
      var completed = false;
      unawaited(
        context.whenCancellationRequested.then((_) => completed = true),
      );
      await pumpEventQueue();
      expect(completed, isFalse);
    });
  });
}
