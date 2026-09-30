import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  test('fixed retries wait for the configured delay and then stop', () async {
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
        maxAttempts: 3,
        delay: const Duration(seconds: 2),
      ),
    );

    await harness.until(() => harness.of<TaskRetryScheduled>().length == 1);
    expect(
      harness.of<TaskRetryScheduled>().single.delay,
      const Duration(seconds: 2),
    );
    expect(calls, 1);

    harness.clock.advance(const Duration(seconds: 2));
    await harness.until(() => harness.of<TaskRetryScheduled>().length == 2);
    expect(calls, 2);

    harness.clock.advance(const Duration(seconds: 2));
    await harness.until(() => harness.of<TaskFailed>().isNotEmpty);
    expect(calls, 3);
    expect(harness.of<TaskRetryScheduled>(), hasLength(2));

    final stored = await harness.queue.getTask(id);
    expect(stored?.status, TaskStatus.failed);
    expect(stored?.attempts, 3);
    expect(stored?.lastFailure?.attempt, 3);
  });

  test('exponential delays double until the task succeeds', () async {
    final harness = QueueHarness();
    var calls = 0;
    harness.register(
      handler: (task, context) async {
        calls++;
        if (calls < 3) throw TemporaryFailure();
      },
    );

    await harness.queue.start();
    await harness.queue.enqueue(
      ValueTask('a'),
      retryPolicy: RetryPolicy.exponential(
        maxAttempts: 5,
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(seconds: 10),
      ),
    );

    await harness.until(() => harness.of<TaskRetryScheduled>().length == 1);
    expect(
      harness.of<TaskRetryScheduled>().single.delay,
      const Duration(seconds: 1),
    );
    harness.clock.advance(const Duration(seconds: 1));

    await harness.until(() => harness.of<TaskRetryScheduled>().length == 2);
    expect(
      harness.of<TaskRetryScheduled>()[1].delay,
      const Duration(seconds: 2),
    );
    harness.clock.advance(const Duration(seconds: 2));

    await harness.until(() => harness.of<TaskCompleted>().isNotEmpty);
    expect(calls, 3);
    expect((await harness.queue.getTasks()).single.attempts, 3);
    expect((await harness.queue.getTasks()).single.lastFailure, isNull);
  });

  test('retryIf can reject a permanent error', () async {
    final harness = QueueHarness();
    var calls = 0;
    harness.register(
      handler: (task, context) async {
        calls++;
        throw StateError('invalid');
      },
      retryIf: (error, stackTrace) => error is TemporaryFailure,
    );

    await harness.queue.start();
    await harness.queue.enqueue(
      ValueTask('a'),
      retryPolicy: RetryPolicy.exponential(
        maxAttempts: 5,
        initialDelay: const Duration(seconds: 1),
      ),
    );
    await harness.until(() => harness.of<TaskFailed>().isNotEmpty);

    expect(calls, 1);
    expect(harness.of<TaskRetryScheduled>(), isEmpty);
  });

  test('a throwing retryIf does not retry', () async {
    final harness = QueueHarness();
    harness.register(
      handler: (task, context) async {
        throw TemporaryFailure();
      },
      retryIf: (error, stackTrace) => throw StateError('predicate failed'),
    );

    await harness.queue.start();
    await harness.queue.enqueue(
      ValueTask('a'),
      retryPolicy: RetryPolicy.fixed(
        maxAttempts: 4,
        delay: const Duration(seconds: 1),
      ),
    );
    await harness.until(() => harness.of<TaskFailed>().isNotEmpty);

    expect(harness.of<TaskStarted>(), hasLength(1));
    expect(
      (await harness.queue.getTasks()).single.lastFailure?.error,
      contains('TemporaryFailure'),
    );
  });
}
