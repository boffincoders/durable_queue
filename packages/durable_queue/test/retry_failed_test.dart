import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  test('retryFailed puts failed tasks back, oldest first', () async {
    final h = QueueHarness();
    var fail = true;
    final ran = <String>[];
    h.register(
      handler: (task, context) async {
        ran.add(task.value);
        if (fail) throw TemporaryFailure();
      },
    );
    final a = await h.queue.enqueue(ValueTask('a'), group: 'g');
    final b = await h.queue.enqueue(ValueTask('b'));
    final c = await h.queue.enqueue(ValueTask('c'), group: 'g');
    await h.queue.start();
    await h.until(() => h.of<TaskFailed>().length == 3);

    fail = false;
    ran.clear();
    final inGroup = await h.queue.retryFailed(group: 'g');
    expect(inGroup, [a, c]);
    await h.until(() => ran.length == 2);
    expect((await h.queue.getTask(b))!.status, TaskStatus.failed);

    final rest = await h.queue.retryFailed(
      retryPolicy: RetryPolicy.fixed(maxAttempts: 3, delay: Duration.zero),
    );
    expect(rest, [b]);
    await h.until(() => ran.length == 3);
    expect((await h.queue.getTask(b))!.retryPolicy.maxAttempts, 3);
    await h.queue.close();
  });

  test('skips tasks retry refuses', () async {
    final h = QueueHarness();
    h.register(handler: (task, context) async => throw TemporaryFailure());
    await h.queue.enqueue(ValueTask('a'), deduplicationKey: 'k');
    await h.queue.start();
    await h.until(() => h.of<TaskFailed>().isNotEmpty);
    await h.queue.pause();
    // Another active task now holds the key, so retry refuses.
    await h.queue.enqueue(ValueTask('b'), deduplicationKey: 'k');
    expect(await h.queue.retryFailed(), isEmpty);
    await h.queue.close();
  });
}
