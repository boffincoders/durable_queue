import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  test('enqueue, fail, restart, retry, and complete', () async {
    final clock = FakeQueueClock(DateTime.utc(2026, 4, 1, 9));
    final harness = QueueHarness(clock: clock);
    var calls = 0;
    harness.register(
      handler: (task, context) async {
        calls++;
        expect(context.attempt, calls);
        if (calls == 1) throw TemporaryFailure();
      },
    );

    await harness.queue.start();
    final id = await harness.queue.enqueue(
      ValueTask('/storage/avatar.jpg'),
      retryPolicy: RetryPolicy.exponential(
        maxAttempts: 5,
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(minutes: 1),
      ),
    );

    await harness.until(() => harness.of<TaskRetryScheduled>().isNotEmpty);
    expect(
      (await harness.queue.getTask(id))?.status,
      TaskStatus.retryScheduled,
    );
    await harness.queue.stop();

    final restored = await reloadStorage(harness.storage);
    final restarted = QueueHarness(storage: restored, clock: clock);
    restarted.register(
      handler: (task, context) async {
        calls++;
        expect(task.value, '/storage/avatar.jpg');
      },
    );
    await restarted.queue.start();
    expect(
      (await restarted.queue.getTask(id))?.status,
      TaskStatus.retryScheduled,
    );

    clock.advance(const Duration(seconds: 1));
    await restarted.until(() => restarted.of<TaskCompleted>().isNotEmpty);

    final stored = await restarted.queue.getTask(id);
    expect(stored?.status, TaskStatus.completed);
    expect(stored?.attempts, 2);
    expect(calls, 2);
    expect(harness.of<TaskEnqueued>(), hasLength(1));
    expect(harness.of<TaskStarted>(), hasLength(1));
    expect(harness.of<TaskRetryScheduled>(), hasLength(1));
    expect(restarted.of<TaskStarted>(), hasLength(1));
    expect(restarted.of<TaskCompleted>(), hasLength(1));
    expect(restarted.of<TaskFailed>(), isEmpty);
  });
}
