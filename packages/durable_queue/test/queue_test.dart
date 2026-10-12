import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  test('a registered task runs and completes', () async {
    final harness = QueueHarness();
    String? seenKey;
    var seenAttempt = 0;
    harness.register(
      handler: (task, context) async {
        seenKey = context.idempotencyKey;
        seenAttempt = context.attempt;
        expect(task.value, 'avatar');
      },
    );

    await harness.queue.start();
    final id = await harness.queue.enqueue(
      ValueTask('avatar'),
      idempotencyKey: 'avatar-1',
    );

    await harness.until(() => harness.of<TaskCompleted>().isNotEmpty);
    final stored = await harness.queue.getTask(id);
    expect(stored?.status, TaskStatus.completed);
    expect(stored?.attempts, 1);
    expect(stored?.idempotencyKey, 'avatar-1');
    expect(seenKey, 'avatar-1');
    expect(seenAttempt, 1);
    expect(harness.of<TaskEnqueued>(), hasLength(1));
    expect(harness.of<TaskStarted>().single.attempt, 1);
    expect(harness.queue.runState, QueueRunState.running);
  });

  test('tasks do not run before start', () async {
    final harness = QueueHarness();
    var calls = 0;
    harness.register(handler: (task, context) async => calls++);

    await harness.queue.enqueue(ValueTask('a'));
    await harness.until(() => harness.of<TaskEnqueued>().isNotEmpty);

    expect(calls, 0);
    expect((await harness.queue.getTasks()).single.status, TaskStatus.pending);
  });

  test('tasks run in creation order', () async {
    final harness = QueueHarness(idGenerator: _sequence());
    final seen = <String>[];
    harness.register(
      handler: (task, context) async {
        seen.add(task.value);
      },
    );

    await harness.queue.start();
    await harness.queue.enqueue(ValueTask('a'));
    await harness.queue.enqueue(ValueTask('b'));
    await harness.queue.enqueue(ValueTask('c'));
    await harness.until(() => harness.of<TaskCompleted>().length == 3);

    expect(seen, ['a', 'b', 'c']);
    expect(harness.of<TaskStarted>().map((event) => event.taskId), [
      'task-1',
      'task-2',
      'task-3',
    ]);
  });

  test('one handler failure does not stop later tasks', () async {
    final harness = QueueHarness();
    final seen = <String>[];
    harness.register(
      handler: (task, context) async {
        seen.add(task.value);
        if (task.value == 'bad') throw TemporaryFailure();
      },
    );

    await harness.queue.start();
    await harness.queue.enqueue(ValueTask('ok'));
    await harness.queue.enqueue(ValueTask('bad'));
    await harness.queue.enqueue(ValueTask('after'));
    await harness.until(() => harness.of<TaskFailed>().isNotEmpty);
    await harness.until(() => harness.of<TaskCompleted>().length == 2);

    expect(seen, ['ok', 'bad', 'after']);
    final failed = await harness.queue.getTasks(status: TaskStatus.failed);
    expect(failed.single.payload['value'], 'bad');
    expect(failed.single.lastFailure?.error, contains('TemporaryFailure'));
    expect(failed.single.lastFailure?.attempt, 1);
    expect(failed.single.lastFailure?.stackTrace, isNotEmpty);
  });

  test('queries filter by status', () async {
    final harness = QueueHarness();
    harness.register(
      handler: (task, context) async {
        if (task.value == 'bad') throw TemporaryFailure();
      },
    );
    await harness.queue.start();
    await harness.queue.enqueue(ValueTask('good'));
    await harness.queue.enqueue(ValueTask('bad'));
    await harness.until(
      () =>
          harness.of<TaskCompleted>().isNotEmpty &&
          harness.of<TaskFailed>().isNotEmpty,
    );

    expect(await harness.queue.getTasks(status: TaskStatus.pending), isEmpty);
    expect(
      (await harness.queue.getTasks(status: TaskStatus.completed))
          .single
          .payload['value'],
      'good',
    );
    expect(await harness.queue.getTasks(), hasLength(2));
    expect(await harness.queue.getTask('missing'), isNull);
  });

  test(
    'unknown types, duplicate registration, and bad payloads are rejected',
    () async {
      final harness = QueueHarness();
      harness.register();

      expect(
        () => harness.queue.register<ValueTask>(
          type: 'value',
          decoder: ValueTask.fromJson,
          handler: (task, context) async {},
        ),
        throwsStateError,
      );
      expect(harness.queue.enqueue(ValueTask('a')), completes);

      final unregistered = QueueHarness();
      expect(
        unregistered.queue.enqueue(ValueTask('a')),
        throwsA(isA<UnknownTaskTypeException>()),
      );
      expect(
        () => unregistered.queue.register<ValueTask>(
          type: '',
          decoder: ValueTask.fromJson,
          handler: (task, context) async {},
        ),
        throwsArgumentError,
      );
      expect(
        () =>
            DurableQueue(storage: MemoryQueueStorage(), maxConcurrentTasks: 0),
        throwsArgumentError,
      );
    },
  );

  test('non-json payloads are rejected at enqueue', () async {
    final harness = QueueHarness();
    harness.queue.register<BadPayloadTask>(
      type: 'bad',
      decoder: BadPayloadTask.fromJson,
      handler: (task, context) async {},
    );

    expect(harness.queue.enqueue(BadPayloadTask()), throwsArgumentError);
  });

  test('a decode error fails the task without a retry', () async {
    final harness = QueueHarness();
    var calls = 0;
    harness.register(
      decoder: (json) => throw const FormatException('bad payload'),
      handler: (task, context) async => calls++,
    );
    harness.queue; // registered with a retry budget below
    await harness.queue.start();
    await harness.queue.enqueue(
      ValueTask('a'),
      retryPolicy: RetryPolicy.fixed(
        maxAttempts: 4,
        delay: const Duration(seconds: 1),
      ),
    );

    await harness.until(() => harness.of<TaskFailed>().isNotEmpty);
    expect(calls, 0);
    expect(harness.of<TaskRetryScheduled>(), isEmpty);
    expect(
      (await harness.queue.getTasks()).single.lastFailure?.error,
      contains('bad payload'),
    );
  });

  test('a decoded type mismatch fails permanently', () async {
    final harness = QueueHarness();
    harness.register(decoder: (json) => RenamedValueTask());
    await harness.queue.start();
    await harness.queue.enqueue(ValueTask('a'));

    await harness.until(() => harness.of<TaskFailed>().isNotEmpty);
    expect(
      (await harness.queue.getTasks()).single.lastFailure?.error,
      contains('renamed'),
    );
  });

  test(
    'an unknown persisted type fails instead of blocking the queue',
    () async {
      final storage = MemoryQueueStorage();
      await storage.save(storedTask(id: 'old', type: 'removed'));
      await storage.save(storedTask(id: 'new'));
      final harness = QueueHarness(storage: storage);
      final seen = <String>[];
      harness.register(
        handler: (task, context) async {
          seen.add(task.value);
        },
      );

      await harness.queue.start();
      await harness.until(() => harness.of<TaskCompleted>().isNotEmpty);
      await harness.until(() => harness.of<TaskFailed>().isNotEmpty);

      expect(seen, ['new']);
      final failed = await harness.queue.getTask('old');
      expect(failed?.status, TaskStatus.failed);
      expect(failed?.lastFailure?.error, contains('removed'));
    },
  );

  test('pause holds new work and resume continues it', () async {
    final harness = QueueHarness();
    final release = <String, Completer<void>>{};
    harness.register(
      handler: (task, context) async {
        await (release[task.value] = Completer<void>()).future;
      },
    );

    await harness.queue.start();
    await harness.queue.enqueue(ValueTask('first'));
    await harness.until(() => harness.of<TaskStarted>().length == 1);
    await harness.queue.pause();
    expect(harness.queue.runState, QueueRunState.paused);

    await harness.queue.enqueue(ValueTask('second'));
    await harness.until(() => harness.of<TaskEnqueued>().length == 2);
    expect(harness.of<TaskStarted>(), hasLength(1));

    release['first']!.complete();
    await harness.until(() => harness.of<TaskCompleted>().length == 1);
    expect(harness.of<TaskStarted>(), hasLength(1));

    await harness.queue.resume();
    await harness.until(() => harness.of<TaskStarted>().length == 2);
    release['second']!.complete();
    await harness.until(() => harness.of<TaskCompleted>().length == 2);
  });

  test('due retries do not start while paused', () async {
    final harness = QueueHarness();
    var calls = 0;
    harness.register(
      handler: (task, context) async {
        calls++;
        if (calls == 1) throw TemporaryFailure();
      },
    );

    await harness.queue.start();
    await harness.queue.enqueue(
      ValueTask('a'),
      retryPolicy: RetryPolicy.fixed(
        maxAttempts: 3,
        delay: const Duration(seconds: 5),
      ),
    );
    await harness.until(() => harness.of<TaskRetryScheduled>().isNotEmpty);
    await harness.queue.pause();
    harness.clock.advance(const Duration(seconds: 5));
    await Future<void>.delayed(Duration.zero);
    expect(calls, 1);

    await harness.queue.resume();
    await harness.until(() => harness.of<TaskCompleted>().isNotEmpty);
    expect(calls, 2);
  });

  test('stop waits for the running handler and can start again', () async {
    final harness = QueueHarness();
    final release = Completer<void>();
    var calls = 0;
    harness.register(
      handler: (task, context) async {
        calls++;
        if (calls == 1) await release.future;
      },
    );

    await harness.queue.start();
    await harness.queue.enqueue(ValueTask('first'));
    await harness.queue.enqueue(ValueTask('second'));
    await harness.until(() => harness.of<TaskStarted>().length == 1);

    var stopped = false;
    final stopping = harness.queue.stop().whenComplete(() => stopped = true);
    await Future<void>.delayed(Duration.zero);
    expect(stopped, isFalse);
    expect(harness.queue.runState, QueueRunState.stopping);

    release.complete();
    await stopping;
    expect(harness.queue.runState, QueueRunState.idle);
    expect(calls, 1);
    expect(
      (await harness.queue.getTask(
        (await harness.queue.getTasks())
            .firstWhere((task) => task.payload['value'] == 'second')
            .id,
      ))?.status,
      TaskStatus.pending,
    );

    await harness.queue.start();
    await harness.until(() => harness.of<TaskCompleted>().length == 2);
    expect(calls, 2);
    await harness.queue.stop();
    expect(harness.queue.runState, QueueRunState.idle);
  });

  test('illegal lifecycle transitions throw', () async {
    final harness = QueueHarness();
    expect(harness.queue.pause(), throwsStateError);
    expect(harness.queue.resume(), throwsStateError);

    await harness.queue.start();
    expect(harness.queue.start(), throwsStateError);
    expect(harness.queue.resume(), throwsStateError);
    await harness.queue.pause();
    expect(harness.queue.pause(), throwsStateError);
    expect(harness.queue.start(), throwsStateError);
  });

  test('a retry wait does not block other pending tasks', () async {
    final harness = QueueHarness();
    final seen = <String>[];
    harness.register(
      handler: (task, context) async {
        seen.add(task.value);
        if (task.value == 'slow') throw TemporaryFailure();
      },
    );

    await harness.queue.start();
    await harness.queue.enqueue(
      ValueTask('slow'),
      retryPolicy: RetryPolicy.fixed(
        maxAttempts: 3,
        delay: const Duration(minutes: 1),
      ),
    );
    await harness.queue.enqueue(ValueTask('next'));
    await harness.until(() => seen.contains('next'));

    expect(seen, ['slow', 'next']);
    expect(harness.of<TaskRetryScheduled>(), hasLength(1));
    expect(harness.of<TaskCompleted>(), hasLength(1));
  });
}

String Function() _sequence() {
  var next = 0;
  return () => 'task-${++next}';
}

final class BadPayloadTask extends DurableTask {
  @override
  String get type => 'bad';

  @override
  Map<String, dynamic> toJson() => {'when': DateTime.utc(2026)};

  static BadPayloadTask fromJson(Map<String, dynamic> json) => BadPayloadTask();
}
