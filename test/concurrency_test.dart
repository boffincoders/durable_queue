import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  test('the concurrency limit bounds in-flight handlers', () async {
    final harness = QueueHarness(maxConcurrentTasks: 2);
    final entered = <String>[];
    final release = <String, Completer<void>>{};
    harness.register(
      handler: (task, context) async {
        entered.add(task.value);
        await (release[task.value] = Completer<void>()).future;
      },
    );

    await harness.queue.start();
    for (final value in ['a', 'b', 'c', 'd']) {
      await harness.queue.enqueue(ValueTask(value));
    }

    await harness.until(() => entered.length == 2);
    await Future<void>.delayed(Duration.zero);
    expect(entered, ['a', 'b']);
    expect(
      await harness.queue.getTasks(status: TaskStatus.running),
      hasLength(2),
    );
    expect(
      await harness.queue.getTasks(status: TaskStatus.pending),
      hasLength(2),
    );

    release['a']!.complete();
    await harness.until(() => entered.length == 3);
    expect(entered, ['a', 'b', 'c']);

    release['b']!.complete();
    release['c']!.complete();
    await harness.until(() => entered.length == 4);
    release['d']!.complete();
    await harness.until(() => harness.of<TaskCompleted>().length == 4);
    expect(entered, ['a', 'b', 'c', 'd']);
  });
}
