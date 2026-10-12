import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  test('cancelling pending work prevents the handler from running', () async {
    final harness = QueueHarness();
    var calls = 0;
    harness.register(handler: (task, context) async => calls++);

    final id = await harness.queue.enqueue(ValueTask('a'));
    await harness.queue.cancel(id);
    await harness.queue.start();
    await harness.until(() => harness.of<TaskCancelled>().isNotEmpty);
    await Future<void>.delayed(Duration.zero);

    expect(calls, 0);
    expect((await harness.queue.getTask(id))?.status, TaskStatus.cancelled);
    expect(harness.of<TaskCancelled>().single.wasRunning, isFalse);
    expect(harness.of<TaskStarted>(), isEmpty);
  });

  test('cancelling a retry-scheduled task prevents the retry', () async {
    final harness = QueueHarness();
    var calls = 0;
    harness.register(
      handler: (task, context) async {
        calls++;
        throw TemporaryFailure();
      },
    );

    await harness.queue.start();
    final id = await harness.queue.enqueue(
      ValueTask('a'),
      retryPolicy: RetryPolicy.fixed(
        maxAttempts: 4,
        delay: const Duration(seconds: 5),
      ),
    );
    await harness.until(() => harness.of<TaskRetryScheduled>().isNotEmpty);
    await harness.queue.cancel(id);
    harness.clock.advance(const Duration(seconds: 5));
    await Future<void>.delayed(Duration.zero);

    expect(calls, 1);
    expect((await harness.queue.getTask(id))?.status, TaskStatus.cancelled);
  });

  test('a running task that succeeds after cancel is completed', () async {
    final harness = QueueHarness();
    final started = Completer<void>();
    final release = Completer<void>();
    bool? sawCancel;
    harness.register(
      handler: (task, context) async {
        started.complete();
        await release.future;
        sawCancel = context.isCancellationRequested;
      },
    );

    await harness.queue.start();
    final id = await harness.queue.enqueue(ValueTask('a'));
    await started.future;
    await harness.queue.cancel(id);
    release.complete();
    await harness.until(() => harness.of<TaskCompleted>().isNotEmpty);

    expect(sawCancel, isTrue);
    expect((await harness.queue.getTask(id))?.status, TaskStatus.completed);
    expect(harness.of<TaskCancelled>(), isEmpty);
  });

  test('a running task that throws after cancel is not retried', () async {
    final harness = QueueHarness();
    final started = Completer<void>();
    final release = Completer<void>();
    harness.register(
      handler: (task, context) async {
        started.complete();
        await release.future;
        throw TemporaryFailure();
      },
    );

    await harness.queue.start();
    final id = await harness.queue.enqueue(
      ValueTask('a'),
      retryPolicy: RetryPolicy.exponential(
        maxAttempts: 5,
        initialDelay: const Duration(seconds: 1),
      ),
    );
    await started.future;
    await harness.queue.cancel(id);
    release.complete();
    await harness.until(() => harness.of<TaskCancelled>().isNotEmpty);

    expect(harness.of<TaskCancelled>().single.wasRunning, isTrue);
    expect(harness.of<TaskRetryScheduled>(), isEmpty);
    final stored = await harness.queue.getTask(id);
    expect(stored?.status, TaskStatus.cancelled);
    expect(stored?.lastFailure?.error, contains('TemporaryFailure'));
  });

  test(
    'TaskCancelledException acknowledges cooperative cancellation',
    () async {
      final harness = QueueHarness();
      final release = Completer<void>();
      harness.register(
        handler: (task, context) async {
          await release.future;
          if (context.isCancellationRequested) {
            throw const TaskCancelledException('stopped');
          }
        },
      );

      await harness.queue.start();
      final id = await harness.queue.enqueue(
        ValueTask('a'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 3,
          delay: const Duration(seconds: 1),
        ),
      );
      await harness.until(() => harness.of<TaskStarted>().isNotEmpty);
      await harness.queue.cancel(id);
      release.complete();
      await harness.until(() => harness.of<TaskCancelled>().isNotEmpty);

      expect((await harness.queue.getTask(id))?.status, TaskStatus.cancelled);
      expect(harness.of<TaskRetryScheduled>(), isEmpty);
    },
  );

  test('cancel of an unknown id throws and terminal tasks stay put', () async {
    final harness = QueueHarness();
    harness.register(
      handler: (task, context) async {
        throw TemporaryFailure();
      },
    );
    await harness.queue.start();
    final id = await harness.queue.enqueue(ValueTask('a'));
    await harness.until(() => harness.of<TaskFailed>().isNotEmpty);

    expect(
      harness.queue.cancel('missing'),
      throwsA(isA<TaskNotFoundException>()),
    );
    await harness.queue.cancel(id);
    expect((await harness.queue.getTask(id))?.status, TaskStatus.failed);
    expect(harness.of<TaskCancelled>(), isEmpty);
  });
}
