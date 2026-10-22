import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  late QueueHarness h;

  setUp(() => h = QueueHarness(maxConcurrentTasks: 2));
  tearDown(() => h.queue.close());

  test('runs ready work, then stops and reports', () async {
    var calls = 0;
    h.register(
      handler: (task, context) async {
        calls++;
        if (task.value == 'bad') throw TemporaryFailure();
      },
    );
    await h.queue.enqueue(ValueTask('a'));
    await h.queue.enqueue(ValueTask('b'));
    await h.queue.enqueue(ValueTask('bad'));
    await h.queue.enqueue(ValueTask('c'));
    await h.queue.enqueue(ValueTask('later'), delay: const Duration(hours: 1));

    final result = await h.queue.drain();
    expect(result.reason, DrainStopReason.idle);
    expect(h.queue.runState, QueueRunState.idle);
    expect(result.started, 4);
    expect(result.completed, 3);
    expect(result.failed, 1);
    expect(result.retryScheduled, 0);
    expect(result.nextWakeAt, h.clock.now().add(const Duration(hours: 1)));
    expect(calls, 4);
  });

  test('work released by a running task is run too', () async {
    final ran = <String>[];
    final release = Completer<void>();
    h.register(
      handler: (task, context) async {
        ran.add(task.value);
        if (task.value == 'one') await release.future;
      },
    );
    await h.queue.enqueueChain([ValueTask('one'), ValueTask('two')]);
    final done = h.queue.drain();
    await h.until(() => ran.isNotEmpty);
    // 'two' is not ready while 'one' runs; the drain must keep waiting.
    await pumpEventQueue();
    release.complete();
    final result = await done;
    expect(ran, ['one', 'two']);
    expect(result.completed, 2);
  });

  test('a retry due later does not keep the drain going', () async {
    var calls = 0;
    h.register(
      handler: (task, context) async {
        if (++calls == 1) throw TemporaryFailure();
      },
    );
    await h.queue.enqueue(
      ValueTask('a'),
      retryPolicy: RetryPolicy.fixed(
        maxAttempts: 2,
        delay: const Duration(seconds: 30),
      ),
    );
    final result = await h.queue.drain(timeLimit: const Duration(minutes: 5));
    expect(result.reason, DrainStopReason.idle);
    expect(result.retryScheduled, 1);
    expect(result.nextWakeAt, h.clock.now().add(const Duration(seconds: 30)));
  });

  test('waitForScheduled waits for work due before the limit', () async {
    var calls = 0;
    h.register(
      handler: (task, context) async {
        if (++calls == 1) throw TemporaryFailure();
      },
    );
    await h.queue.enqueue(
      ValueTask('a'),
      retryPolicy: RetryPolicy.fixed(
        maxAttempts: 2,
        delay: const Duration(seconds: 30),
      ),
    );
    await h.queue.enqueue(ValueTask('far'), delay: const Duration(hours: 1));
    final done = h.queue.drain(
      timeLimit: const Duration(minutes: 5),
      waitForScheduled: true,
    );
    await h.until(() => h.of<TaskRetryScheduled>().isNotEmpty);
    h.clock.advance(const Duration(seconds: 30));
    final result = await done;
    expect(result.reason, DrainStopReason.idle);
    expect(result.completed, 1);
    // The task due in an hour is beyond the limit and does not hold it.
    expect(result.nextWakeAt, isNotNull);
  });

  test(
    'the time limit stops new starts and lets running work finish',
    () async {
      final release = Completer<void>();
      final ran = <String>[];
      final single = QueueHarness();
      single.register(
        handler: (task, context) async {
          ran.add(task.value);
          if (task.value == 'long') await release.future;
        },
      );
      await single.queue.enqueue(ValueTask('long'));
      await single.queue.enqueue(ValueTask('next'));
      final done = single.queue.drain(timeLimit: const Duration(seconds: 10));
      await single.until(() => ran.isNotEmpty);
      single.clock.advance(const Duration(seconds: 10));
      await pumpEventQueue();
      release.complete();
      final result = await done;
      expect(result.reason, DrainStopReason.timeLimit);
      expect(ran, ['long']);
      expect(result.completed, 1);
      expect(single.queue.runState, QueueRunState.idle);
      final next = await single.queue.getTasks(status: TaskStatus.pending);
      expect(next.single.payload['value'], 'next');
      await single.queue.close();
    },
  );

  test('gated work does not keep the drain going', () async {
    h.queue.addGate('network', ManualQueueGate(open: false));
    h.register(requires: {'network'});
    await h.queue.enqueue(ValueTask('a'));
    final result = await h.queue.drain();
    expect(result.reason, DrainStopReason.idle);
    expect(result.started, 0);
    expect(result.nextWakeAt, isNotNull);
  });

  test('an empty queue drains at once and the queue can drain again', () async {
    h.register();
    final first = await h.queue.drain();
    expect(first.reason, DrainStopReason.idle);
    expect(first.nextWakeAt, isNull);
    await h.queue.enqueue(ValueTask('a'));
    final second = await h.queue.drain();
    expect(second.completed, 1);
  });

  test('requires an idle queue and a positive limit', () async {
    h.register();
    await expectLater(
      h.queue.drain(timeLimit: Duration.zero),
      throwsArgumentError,
    );
    await h.queue.start();
    await expectLater(h.queue.drain(), throwsStateError);
  });
}
