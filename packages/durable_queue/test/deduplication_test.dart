import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  test('an active deduplication key returns the existing task', () async {
    final harness = QueueHarness();
    harness.register();

    final first = await harness.queue.enqueue(
      ValueTask('original'),
      deduplicationKey: 'sync-user:123',
    );
    final second = await harness.queue.enqueue(
      ValueTask('replacement'),
      deduplicationKey: 'sync-user:123',
    );

    expect(second, first);
    final tasks = await harness.queue.getTasks();
    expect(tasks, hasLength(1));
    expect(tasks.single.payload['value'], 'original');
    expect(harness.of<TaskEnqueued>(), hasLength(1));
  });

  test('parallel enqueues with the same key create one task', () async {
    final harness = QueueHarness();
    harness.register();

    final ids = await Future.wait([
      harness.queue.enqueue(ValueTask('a'), deduplicationKey: 'same'),
      harness.queue.enqueue(ValueTask('b'), deduplicationKey: 'same'),
    ]);

    expect(ids.first, ids.last);
    expect(await harness.queue.getTasks(), hasLength(1));
  });

  test('a terminal task does not block the same key', () async {
    final harness = QueueHarness();
    harness.register();
    await harness.queue.start();

    final first = await harness.queue.enqueue(
      ValueTask('a'),
      deduplicationKey: 'sync-user:123',
    );
    await harness.until(() => harness.of<TaskCompleted>().length == 1);

    final second = await harness.queue.enqueue(
      ValueTask('b'),
      deduplicationKey: 'sync-user:123',
    );
    await harness.until(() => harness.of<TaskCompleted>().length == 2);

    expect(second, isNot(first));
    expect(await harness.queue.getTasks(), hasLength(2));
  });

  test('failed and cancelled tasks can be enqueued again', () async {
    final harness = QueueHarness();
    harness.register(
      handler: (task, context) async {
        throw TemporaryFailure();
      },
    );

    final failed = await harness.queue.enqueue(
      ValueTask('a'),
      deduplicationKey: 'job',
    );
    await harness.queue.start();
    await harness.until(() => harness.of<TaskFailed>().isNotEmpty);
    await harness.queue.pause();

    final again = await harness.queue.enqueue(
      ValueTask('b'),
      deduplicationKey: 'job',
    );
    expect(again, isNot(failed));

    await harness.queue.cancel(again);
    final third = await harness.queue.enqueue(
      ValueTask('c'),
      deduplicationKey: 'job',
    );
    expect(third, isNot(again));
    expect(await harness.queue.getTasks(), hasLength(3));
  });

  test('an idempotency key is stored and does not deduplicate', () async {
    final harness = QueueHarness();
    String? seen;
    harness.register(
      handler: (task, context) async {
        seen = context.idempotencyKey;
      },
    );
    await harness.queue.start();

    final first = await harness.queue.enqueue(
      ValueTask('a'),
      idempotencyKey: 'order-1',
    );
    final second = await harness.queue.enqueue(
      ValueTask('b'),
      idempotencyKey: 'order-1',
    );
    await harness.until(() => harness.of<TaskCompleted>().length == 2);

    expect(first, isNot(second));
    expect(seen, 'order-1');
    expect((await harness.queue.getTask(first))?.idempotencyKey, 'order-1');
  });

  test('empty deduplication and idempotency keys are rejected', () async {
    final harness = QueueHarness();
    harness.register();
    expect(
      harness.queue.enqueue(ValueTask('a'), deduplicationKey: ''),
      throwsArgumentError,
    );
    expect(
      harness.queue.enqueue(ValueTask('a'), idempotencyKey: ''),
      throwsArgumentError,
    );
  });
}
