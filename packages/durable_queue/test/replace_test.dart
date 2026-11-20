import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  const replace = DuplicatePolicy.replace;

  test('a task that has not started gets the new payload and keeps its '
      'settings', () async {
    final h = QueueHarness();
    final seen = <String>[];
    h.register(handler: (task, context) async => seen.add(task.value));

    final id = await h.queue.enqueue(
      ValueTask('first edit'),
      deduplicationKey: 'profile',
      priority: 5,
      group: 'profile',
      retryPolicy: RetryPolicy.fixed(
        maxAttempts: 3,
        delay: const Duration(seconds: 1),
      ),
    );
    final replaced = await h.queue.enqueue(
      ValueTask('latest edit'),
      deduplicationKey: 'profile',
      onDuplicate: replace,
    );

    expect(replaced, id);
    final task = (await h.queue.getTasks()).single;
    expect(task.id, id);
    expect(task.payload, {'value': 'latest edit'});
    expect(task.priority, 5);
    expect(task.group, 'profile');
    expect(task.retryPolicy.maxAttempts, 3);
    expect(task.status, TaskStatus.pending);

    final enqueued = h.of<TaskEnqueued>();
    expect(enqueued.map((e) => e.replaced), [false, true]);
    expect(enqueued.last.taskId, id);
    expect(enqueued.last.deduplicationKey, 'profile');

    await h.queue.drain();
    expect(seen, ['latest edit']);
  });

  test('keep, the default, still leaves the existing task unchanged', () async {
    final h = QueueHarness();
    h.register();
    final id = await h.queue.enqueue(ValueTask('a'), deduplicationKey: 'k');
    expect(
      await h.queue.enqueue(
        ValueTask('b'),
        deduplicationKey: 'k',
        onDuplicate: DuplicatePolicy.keep,
      ),
      id,
    );
    expect((await h.queue.getTask(id))!.payload, {'value': 'a'});
  });

  test(
    'a running task is followed by one task with the latest payload',
    () async {
      final h = QueueHarness();
      final seen = <String>[];
      final release = Completer<void>();
      h.register(
        handler: (task, context) async {
          seen.add(task.value);
          if (task.value == 'v1') await release.future;
        },
      );
      await h.queue.start();

      final running = await h.queue.enqueue(
        ValueTask('v1'),
        deduplicationKey: 'profile',
      );
      await h.until(() => h.of<TaskStarted>().length == 1);

      final followUp = await h.queue.enqueue(
        ValueTask('v2'),
        deduplicationKey: 'profile',
        onDuplicate: replace,
      );
      expect(followUp, isNot(running));
      final again = await h.queue.enqueue(
        ValueTask('v3'),
        deduplicationKey: 'profile',
        onDuplicate: replace,
      );
      expect(again, followUp);

      final waiting = (await h.queue.getTask(followUp))!;
      expect(waiting.status, TaskStatus.waiting);
      expect(waiting.dependsOn, [running]);
      expect(waiting.onDependencyFailure, DependencyFailurePolicy.run);
      expect(waiting.payload, {'value': 'v3'});
      expect((await h.queue.getTask(running))!.payload, {'value': 'v1'});
      expect(await h.queue.getTasks(), hasLength(2));

      release.complete();
      await h.until(() => h.of<TaskCompleted>().length == 2);
      expect(seen, ['v1', 'v3']);
      await h.queue.stop();
    },
  );

  test('the follow-up runs even when the running task fails', () async {
    final h = QueueHarness();
    final seen = <String>[];
    final release = Completer<void>();
    h.register(
      handler: (task, context) async {
        seen.add(task.value);
        if (task.value == 'old') {
          await release.future;
          throw TemporaryFailure();
        }
      },
    );
    await h.queue.start();
    final running = await h.queue.enqueue(
      ValueTask('old'),
      deduplicationKey: 'k',
    );
    await h.until(() => h.of<TaskStarted>().length == 1);
    final followUp = await h.queue.enqueue(
      ValueTask('new'),
      deduplicationKey: 'k',
      onDuplicate: replace,
    );

    release.complete();
    await h.until(() => h.of<TaskCompleted>().length == 1);
    expect(seen, ['old', 'new']);
    expect((await h.queue.getTask(running))!.status, TaskStatus.failed);
    expect((await h.queue.getTask(followUp))!.status, TaskStatus.completed);
    await h.queue.stop();
  });

  test('after the running task is scheduled to retry, the follow-up still '
      'gets the payload, so the latest version runs last', () async {
    final h = QueueHarness();
    final seen = <String>[];
    final release = Completer<void>();
    var attempts = 0;
    h.register(
      handler: (task, context) async {
        seen.add(task.value);
        if (task.value == 'v1' && ++attempts == 1) {
          await release.future;
          throw TemporaryFailure();
        }
      },
    );
    await h.queue.start();
    final first = await h.queue.enqueue(
      ValueTask('v1'),
      deduplicationKey: 'k',
      retryPolicy: RetryPolicy.fixed(
        maxAttempts: 2,
        delay: const Duration(minutes: 1),
      ),
    );
    await h.until(() => h.of<TaskStarted>().length == 1);
    final followUp = await h.queue.enqueue(
      ValueTask('v2'),
      deduplicationKey: 'k',
      onDuplicate: replace,
    );
    release.complete();
    await h.until(() => h.of<TaskRetryScheduled>().length == 1);

    // The first task waits to retry; the follow-up is the one replaced.
    expect(
      await h.queue.enqueue(
        ValueTask('v3'),
        deduplicationKey: 'k',
        onDuplicate: replace,
      ),
      followUp,
    );
    expect((await h.queue.getTask(first))!.payload, {'value': 'v1'});

    h.clock.advance(const Duration(minutes: 1));
    await h.until(() => h.of<TaskCompleted>().length == 2);
    expect(seen, ['v1', 'v1', 'v3']);
    await h.queue.stop();
  });

  test(
    'a task waiting to retry takes the idempotency key of the new call',
    () async {
      final h = QueueHarness();
      final sent = <(String, String?)>[];
      h.register(
        handler: (task, context) async {
          sent.add((task.value, context.idempotencyKey));
          // The first attempt reached the server, then timed out.
          if (sent.length == 1) throw TemporaryFailure();
        },
      );
      final id = await h.queue.enqueue(
        ValueTask('rev 1'),
        deduplicationKey: 'profile',
        idempotencyKey: 'profile:1',
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 3,
          delay: const Duration(minutes: 1),
        ),
      );
      await h.queue.drain();
      expect((await h.queue.getTask(id))!.status, TaskStatus.retryScheduled);

      await h.queue.enqueue(
        ValueTask('rev 2'),
        deduplicationKey: 'profile',
        idempotencyKey: 'profile:2',
        onDuplicate: replace,
      );
      final replaced = (await h.queue.getTask(id))!;
      expect(replaced.idempotencyKey, 'profile:2');
      expect(replaced.attempts, 1);
      expect(h.of<TaskEnqueued>().last.idempotencyKey, 'profile:2');

      h.clock.advance(const Duration(minutes: 1));
      await h.queue.drain();
      expect(sent, [('rev 1', 'profile:1'), ('rev 2', 'profile:2')]);
    },
  );

  test('a replace without an idempotency key clears the old one', () async {
    final h = QueueHarness();
    h.register();
    final id = await h.queue.enqueue(
      ValueTask('a'),
      deduplicationKey: 'k',
      idempotencyKey: 'old',
    );
    await h.queue.enqueue(
      ValueTask('b'),
      deduplicationKey: 'k',
      onDuplicate: replace,
    );
    expect((await h.queue.getTask(id))!.idempotencyKey, isNull);
  });

  test('a held task keeps its hold', () async {
    final h = QueueHarness();
    h.register();
    final id = await h.queue.enqueue(ValueTask('a'), deduplicationKey: 'k');
    await h.queue.hold(id);
    await h.queue.enqueue(
      ValueTask('b'),
      deduplicationKey: 'k',
      onDuplicate: replace,
    );
    final task = (await h.queue.getTask(id))!;
    expect(task.held, isTrue);
    expect(task.payload, {'value': 'b'});
  });

  test('the replaced payload is stored at the registered version', () async {
    final storage = MemoryQueueStorage();
    final old = QueueHarness(storage: storage)..register();
    final id = await old.queue.enqueue(ValueTask('a'), deduplicationKey: 'k');
    await old.queue.close();

    final h = QueueHarness(storage: storage)
      ..register(version: 2, migrate: (from, json) => json);
    await h.queue.enqueue(
      ValueTask('b'),
      deduplicationKey: 'k',
      onDuplicate: replace,
    );
    expect((await h.queue.getTask(id))!.payloadVersion, 2);
  });

  test('metrics do not count a replaced payload as a new task', () async {
    final h = QueueHarness();
    h.register();
    final metrics = QueueMetrics(h.queue);
    addTearDown(metrics.dispose);
    await h.queue.enqueue(ValueTask('a'), deduplicationKey: 'k');
    await h.queue.enqueue(
      ValueTask('b'),
      deduplicationKey: 'k',
      onDuplicate: replace,
    );
    await Future<void>.delayed(Duration.zero);
    expect(metrics.snapshot().total.enqueued, 1);
  });

  test(
    'replace needs a deduplication key held by a task of the same type',
    () async {
      final h = QueueHarness();
      h.register();
      h.register(type: 'renamed');
      expect(
        () => h.queue.enqueue(ValueTask('a'), onDuplicate: replace),
        throwsArgumentError,
      );

      await h.queue.enqueue(ValueTask('a'), deduplicationKey: 'k');
      await expectLater(
        h.queue.enqueue(
          RenamedValueTask(),
          deduplicationKey: 'k',
          onDuplicate: replace,
        ),
        throwsArgumentError,
      );
      expect((await h.queue.getTasks()).single.payload, {'value': 'a'});
    },
  );
}
