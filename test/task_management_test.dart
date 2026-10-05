import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  group('TaskStatus', () {
    test('terminal and active statuses partition the enum', () {
      expect(
        TaskStatus.values.where((status) => status.isTerminal),
        unorderedEquals([
          TaskStatus.completed,
          TaskStatus.failed,
          TaskStatus.cancelled,
        ]),
      );
      for (final status in TaskStatus.values) {
        expect(status.isActive, !status.isTerminal);
      }
    });
  });

  group('retry', () {
    test('a failed task runs again with a fresh attempt budget', () async {
      final harness = QueueHarness();
      var calls = 0;
      final attempts = <int>[];
      harness.register(
        handler: (task, context) async {
          calls++;
          attempts.add(context.attempt);
          if (calls == 1) throw TemporaryFailure();
        },
      );

      await harness.queue.start();
      final id = await harness.queue.enqueue(ValueTask('a'));
      await harness.until(() => harness.of<TaskFailed>().isNotEmpty);
      expect((await harness.queue.getTask(id))?.status, TaskStatus.failed);

      final retried = await harness.queue.retry(id);
      expect(retried, id);
      await harness.until(() => harness.of<TaskCompleted>().isNotEmpty);

      final stored = await harness.queue.getTask(id);
      expect(stored?.status, TaskStatus.completed);
      expect(stored?.attempts, 1);
      expect(attempts, [1, 1]);
      expect(harness.of<TaskEnqueued>(), hasLength(2));
    });

    test('a cancelled task can be retried while the queue is idle', () async {
      final harness = QueueHarness();
      harness.register();
      final id = await harness.queue.enqueue(ValueTask('a'));
      await harness.queue.cancel(id);

      await harness.queue.retry(
        id,
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 3,
          delay: const Duration(seconds: 1),
        ),
      );

      final stored = await harness.queue.getTask(id);
      expect(stored?.status, TaskStatus.pending);
      expect(stored?.attempts, 0);
      expect(stored?.cancelRequested, isFalse);
      expect(stored?.retryPolicy.maxAttempts, 3);

      await harness.queue.start();
      await harness.until(() => harness.of<TaskCompleted>().isNotEmpty);
    });

    test('pending and completed tasks cannot be retried', () async {
      final harness = QueueHarness();
      harness.register();
      final pending = await harness.queue.enqueue(ValueTask('a'));
      await expectLater(harness.queue.retry(pending), throwsStateError);

      await harness.queue.start();
      await harness.until(() => harness.of<TaskCompleted>().isNotEmpty);
      await expectLater(harness.queue.retry(pending), throwsStateError);
    });

    test('unknown ids throw TaskNotFoundException', () async {
      final harness = QueueHarness();
      await expectLater(
        harness.queue.retry('missing'),
        throwsA(isA<TaskNotFoundException>()),
      );
    });

    test('an active duplicate blocks the retry', () async {
      final harness = QueueHarness();
      harness.register();
      final first = await harness.queue.enqueue(
        ValueTask('a'),
        deduplicationKey: 'key',
      );
      await harness.queue.cancel(first);
      final second = await harness.queue.enqueue(
        ValueTask('b'),
        deduplicationKey: 'key',
      );
      expect(second, isNot(first));

      await expectLater(harness.queue.retry(first), throwsStateError);
      expect(
        (await harness.queue.getTask(first))?.status,
        TaskStatus.cancelled,
      );
    });

    test('an unregistered type cannot be retried', () async {
      final storage = MemoryQueueStorage();
      await storage.save(
        storedTask(id: 'orphan', type: 'gone', status: TaskStatus.failed),
      );
      final harness = QueueHarness(storage: storage);
      await expectLater(
        harness.queue.retry('orphan'),
        throwsA(isA<UnknownTaskTypeException>()),
      );
    });
  });

  group('delete', () {
    test('removes a finished task', () async {
      final harness = QueueHarness();
      harness.register();
      final id = await harness.queue.enqueue(ValueTask('a'));
      await harness.queue.cancel(id);

      await harness.queue.delete(id);
      expect(await harness.queue.getTask(id), isNull);
    });

    test('refuses active tasks and unknown ids', () async {
      final harness = QueueHarness();
      harness.register();
      final id = await harness.queue.enqueue(ValueTask('a'));

      await expectLater(harness.queue.delete(id), throwsStateError);
      expect((await harness.queue.getTask(id))?.status, TaskStatus.pending);
      await expectLater(
        harness.queue.delete('missing'),
        throwsA(isA<TaskNotFoundException>()),
      );
    });
  });

  group('purge', () {
    test('removes completed tasks by default', () async {
      final storage = MemoryQueueStorage();
      await storage.save(
        storedTask(id: 'done', status: TaskStatus.completed, sequence: 1),
      );
      await storage.save(
        storedTask(id: 'failed', status: TaskStatus.failed, sequence: 2),
      );
      await storage.save(storedTask(id: 'waiting', sequence: 3));
      final harness = QueueHarness(storage: storage);

      expect(await harness.queue.purge(), 1);
      expect(await storage.get('done'), isNull);
      expect(await storage.get('failed'), isNotNull);
      expect(await storage.get('waiting'), isNotNull);
    });

    test('honors statuses and olderThan', () async {
      final clock = FakeQueueClock(DateTime.utc(2026, 1, 10));
      final storage = MemoryQueueStorage();
      await storage.save(
        storedTask(
          id: 'old',
          status: TaskStatus.failed,
          updatedAt: DateTime.utc(2026, 1, 1),
          sequence: 1,
        ),
      );
      await storage.save(
        storedTask(
          id: 'recent',
          status: TaskStatus.cancelled,
          updatedAt: DateTime.utc(2026, 1, 9),
          sequence: 2,
        ),
      );
      final harness = QueueHarness(storage: storage, clock: clock);

      final removed = await harness.queue.purge(
        statuses: {TaskStatus.failed, TaskStatus.cancelled},
        olderThan: const Duration(days: 7),
      );

      expect(removed, 1);
      expect(await storage.get('old'), isNull);
      expect(await storage.get('recent'), isNotNull);
    });

    test('rejects active statuses and negative ages', () async {
      final harness = QueueHarness();
      expect(
        () => harness.queue.purge(statuses: {TaskStatus.pending}),
        throwsArgumentError,
      );
      expect(
        () => harness.queue.purge(olderThan: const Duration(seconds: -1)),
        throwsArgumentError,
      );
    });
  });

  group('close', () {
    test('stops the worker and closes the streams', () async {
      final harness = QueueHarness();
      harness.register();
      var eventsDone = false;
      var errorsDone = false;
      harness.queue.events.listen((_) {}, onDone: () => eventsDone = true);
      harness.queue.storageErrors.listen(
        (_) {},
        onDone: () => errorsDone = true,
      );
      await harness.queue.start();

      final first = harness.queue.close();
      final second = harness.queue.close();
      expect(identical(first, second), isTrue);
      await first;
      await pumpEventQueue();

      expect(harness.queue.isClosed, isTrue);
      expect(harness.queue.runState, QueueRunState.idle);
      expect(eventsDone, isTrue);
      expect(errorsDone, isTrue);
    });

    test('rejects mutations but still answers queries', () async {
      final harness = QueueHarness();
      harness.register();
      final id = await harness.queue.enqueue(ValueTask('a'));
      await harness.queue.close();

      expect(() => harness.queue.enqueue(ValueTask('b')), throwsStateError);
      expect(() => harness.queue.start(), throwsStateError);
      expect(() => harness.queue.cancel(id), throwsStateError);
      expect(() => harness.queue.retry(id), throwsStateError);
      expect(() => harness.queue.delete(id), throwsStateError);
      expect(() => harness.queue.purge(), throwsStateError);
      expect(() => harness.register(type: 'other'), throwsStateError);
      expect((await harness.queue.getTask(id))?.status, TaskStatus.pending);
      expect(await harness.queue.getTasks(), hasLength(1));
    });

    test('waits for an in-flight handler', () async {
      final harness = QueueHarness();
      var finished = false;
      harness.register(
        handler: (task, context) async {
          await Future<void>.delayed(Duration.zero);
          finished = true;
        },
      );
      await harness.queue.start();
      await harness.queue.enqueue(ValueTask('a'));
      await harness.until(() => harness.of<TaskStarted>().isNotEmpty);

      await harness.queue.close();
      expect(finished, isTrue);
    });
  });
}
