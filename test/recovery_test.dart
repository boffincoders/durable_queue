import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  test(
    'an interrupted running task is scheduled with its retry policy',
    () async {
      final storage = MemoryQueueStorage();
      await storage.save(
        storedTask(
          id: 'upload',
          status: TaskStatus.running,
          attempts: 1,
          retryPolicy: RetryPolicy.exponential(
            maxAttempts: 5,
            initialDelay: const Duration(seconds: 2),
          ),
        ),
      );
      final harness = QueueHarness(storage: storage);
      var calls = 0;
      harness.register(handler: (task, context) async => calls++);

      await harness.queue.start();
      final scheduled = harness.of<TaskRetryScheduled>().single;
      expect(scheduled.attempt, 1);
      expect(scheduled.delay, const Duration(seconds: 2));
      expect(calls, 0);
      expect(
        (await harness.queue.getTask('upload'))?.status,
        TaskStatus.retryScheduled,
      );
      expect(
        (await harness.queue.getTask('upload'))?.lastFailure?.error,
        contains('TaskInterruptedException'),
      );

      harness.clock.advance(const Duration(seconds: 2));
      await harness.until(() => harness.of<TaskCompleted>().isNotEmpty);
      expect(calls, 1);
      expect((await harness.queue.getTask('upload'))?.attempts, 2);
    },
  );

  test('an interrupted final attempt is failed', () async {
    final storage = MemoryQueueStorage();
    await storage.save(
      storedTask(
        id: 'upload',
        status: TaskStatus.running,
        attempts: 1,
        retryPolicy: RetryPolicy.none(),
      ),
    );
    final harness = QueueHarness(storage: storage);
    var calls = 0;
    harness.register(handler: (task, context) async => calls++);

    await harness.queue.start();

    expect(harness.of<TaskFailed>(), hasLength(1));
    expect(calls, 0);
    final stored = await harness.queue.getTask('upload');
    expect(stored?.status, TaskStatus.failed);
    expect(stored?.lastFailure?.error, contains('TaskInterruptedException'));
  });

  test('a running task that was already cancelled is not retried', () async {
    final storage = MemoryQueueStorage();
    await storage.save(
      storedTask(
        id: 'upload',
        status: TaskStatus.running,
        attempts: 1,
        cancelRequested: true,
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 5,
          delay: const Duration(seconds: 1),
        ),
      ),
    );
    final harness = QueueHarness(storage: storage);
    var calls = 0;
    harness.register(handler: (task, context) async => calls++);

    await harness.queue.start();

    expect(calls, 0);
    expect(harness.of<TaskCancelled>().single.wasRunning, isTrue);
    expect(
      (await harness.queue.getTask('upload'))?.status,
      TaskStatus.cancelled,
    );
  });

  test(
    'a running record with zero attempts still consumes an attempt',
    () async {
      final storage = MemoryQueueStorage();
      await storage.save(
        storedTask(
          id: 'upload',
          status: TaskStatus.running,
          attempts: 0,
          retryPolicy: RetryPolicy.fixed(
            maxAttempts: 3,
            delay: const Duration(seconds: 4),
          ),
        ),
      );
      final harness = QueueHarness(storage: storage);
      harness.register();

      await harness.queue.start();

      final stored = await harness.queue.getTask('upload');
      expect(stored?.status, TaskStatus.retryScheduled);
      expect(stored?.attempts, 1);
      expect(
        harness.of<TaskRetryScheduled>().single.delay,
        const Duration(seconds: 4),
      );
    },
  );

  test(
    'restart through json continues a scheduled retry and then completes',
    () async {
      final harness = QueueHarness(randomFraction: () => 0);
      var calls = 0;
      harness.register(
        handler: (task, context) async {
          calls++;
          if (calls == 1) throw TemporaryFailure();
        },
      );

      await harness.queue.start();
      final id = await harness.queue.enqueue(
        ValueTask('avatar'),
        idempotencyKey: 'avatar',
        retryPolicy: RetryPolicy.exponential(
          maxAttempts: 5,
          initialDelay: const Duration(seconds: 1),
          jitter: true,
          jitterFactor: 0.2,
        ),
      );
      await harness.until(() => harness.of<TaskRetryScheduled>().isNotEmpty);
      expect(
        harness.of<TaskRetryScheduled>().single.delay,
        const Duration(milliseconds: 800),
      );
      await harness.queue.stop();

      final restored = await reloadStorage(harness.storage);
      final restarted = QueueHarness(
        storage: restored,
        clock: harness.clock,
        randomFraction: () => 0.5,
      );
      restarted.register(
        handler: (task, context) async {
          calls++;
          expect(context.idempotencyKey, 'avatar');
          expect(task.value, 'avatar');
        },
      );

      await restarted.queue.start();
      expect(calls, 1);
      restarted.clock.advance(const Duration(milliseconds: 800));
      await restarted.until(() => restarted.of<TaskCompleted>().isNotEmpty);

      final stored = await restarted.queue.getTask(id);
      expect(stored?.status, TaskStatus.completed);
      expect(stored?.attempts, 2);
      expect(calls, 2);
      expect(restarted.of<TaskEnqueued>(), isEmpty);
      expect(restarted.of<TaskStarted>().single.attempt, 2);
    },
  );

  test('a clean stop does not run a completed task again', () async {
    final harness = QueueHarness();
    var calls = 0;
    harness.register(handler: (task, context) async => calls++);
    await harness.queue.start();
    await harness.queue.enqueue(ValueTask('a'));
    await harness.until(() => harness.of<TaskCompleted>().isNotEmpty);
    await harness.queue.stop();

    final restarted = QueueHarness(
      storage: harness.storage,
      clock: harness.clock,
    );
    restarted.register(handler: (task, context) async => calls++);
    await restarted.queue.start();
    await Future<void>.delayed(Duration.zero);

    expect(calls, 1);
    expect(restarted.of<TaskStarted>(), isEmpty);
  });
}
