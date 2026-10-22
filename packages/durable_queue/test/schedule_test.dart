import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  late QueueHarness h;
  late List<String> ran;

  setUp(() {
    h = QueueHarness();
    ran = [];
    h.register(handler: (task, context) async => ran.add(task.value));
  });

  tearDown(() => h.queue.close());

  test('delay holds a task until its time', () async {
    final start = h.clock.now();
    final id = await h.queue.enqueue(
      ValueTask('later'),
      delay: const Duration(minutes: 10),
    );
    await h.queue.enqueue(ValueTask('now'));
    await h.queue.start();
    await h.until(() => ran.length == 1);
    expect(ran, ['now']);

    final stored = (await h.queue.getTask(id))!;
    expect(stored.status, TaskStatus.pending);
    expect(stored.nextAttemptAt, start.add(const Duration(minutes: 10)));
    expect(h.of<TaskEnqueued>().first.runAt, stored.nextAttemptAt);
    expect(h.of<TaskEnqueued>().last.runAt, isNull);

    h.clock.advance(const Duration(minutes: 9, seconds: 59));
    await pumpEventQueue();
    expect(ran, ['now']);
    h.clock.advance(const Duration(seconds: 1));
    await h.until(() => ran.length == 2);
    expect(ran, ['now', 'later']);
    expect(h.of<TaskStarted>().last.attempt, 1);
  });

  test('runAt in the past starts at once', () async {
    await h.queue.enqueue(
      ValueTask('a'),
      runAt: h.clock.now().subtract(const Duration(hours: 1)),
    );
    await h.queue.start();
    await h.until(() => ran.length == 1);
  });

  test('a scheduled task holds its deduplication key while it waits', () async {
    final first = await h.queue.enqueue(
      ValueTask('a'),
      deduplicationKey: 'reminder',
      delay: const Duration(hours: 1),
    );
    final second = await h.queue.enqueue(
      ValueTask('b'),
      deduplicationKey: 'reminder',
    );
    expect(second, first);
  });

  test('with dependencies, the later of both conditions wins', () async {
    final parent = await h.queue.enqueue(
      ValueTask('parent'),
      delay: const Duration(minutes: 1),
    );
    await h.queue.enqueue(
      ValueTask('child'),
      dependsOn: [parent],
      delay: const Duration(minutes: 5),
    );
    await h.queue.start();
    h.clock.advance(const Duration(minutes: 1));
    await h.until(() => ran.length == 1);
    expect(ran, ['parent']);
    // Dependency done, but the child's own time has not come.
    await pumpEventQueue();
    expect(ran, ['parent']);
    h.clock.advance(const Duration(minutes: 4));
    await h.until(() => ran.length == 2);
    expect(ran, ['parent', 'child']);
  });

  test(
    'a dependency finishing after the start time releases at once',
    () async {
      final parent = await h.queue.enqueue(
        ValueTask('parent'),
        delay: const Duration(minutes: 10),
      );
      await h.queue.enqueue(
        ValueTask('child'),
        dependsOn: [parent],
        delay: const Duration(minutes: 1),
      );
      await h.queue.start();
      h.clock.advance(const Duration(minutes: 10));
      await h.until(() => ran.length == 2);
      expect(ran, ['parent', 'child']);
    },
  );

  test('enqueueChain schedules the first step', () async {
    await h.queue.enqueueChain([
      ValueTask('one'),
      ValueTask('two'),
    ], delay: const Duration(seconds: 30));
    await h.queue.start();
    await pumpEventQueue();
    expect(ran, isEmpty);
    h.clock.advance(const Duration(seconds: 30));
    await h.until(() => ran.length == 2);
    expect(ran, ['one', 'two']);
  });

  test('scheduled tasks survive a restart', () async {
    final storage = MemoryQueueStorage();
    final first = QueueHarness(storage: storage, clock: h.clock);
    first.register();
    await first.queue.enqueue(ValueTask('a'), delay: const Duration(hours: 2));
    await first.queue.close();

    final second = QueueHarness(storage: storage, clock: h.clock);
    final seen = <String>[];
    second.register(handler: (task, context) async => seen.add(task.value));
    await second.queue.start();
    await pumpEventQueue();
    expect(seen, isEmpty);
    second.clock.advance(const Duration(hours: 2));
    await second.until(() => seen.length == 1);
    await second.queue.close();
  });

  test('validates runAt and delay', () async {
    await expectLater(
      h.queue.enqueue(
        ValueTask('a'),
        runAt: h.clock.now(),
        delay: Duration.zero,
      ),
      throwsArgumentError,
    );
    await expectLater(
      h.queue.enqueue(ValueTask('a'), delay: const Duration(seconds: -1)),
      throwsArgumentError,
    );
  });
}
