import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  late QueueHarness h;
  late List<String> ran;
  late List<TaskHoldChanged> changes;

  setUp(() {
    h = QueueHarness();
    ran = [];
    changes = [];
    h.queue.holdChanges.listen(changes.add);
  });

  tearDown(() => h.queue.close());

  Future<StoredTask> stored(String id) async => (await h.queue.getTask(id))!;

  group('a task that has not started', () {
    setUp(
      () => h.register(handler: (task, context) async => ran.add(task.value)),
    );

    test('does not start while held, and starts once released', () async {
      final a = await h.queue.enqueue(ValueTask('a'));
      await h.queue.enqueue(ValueTask('b'));
      expect(await h.queue.hold(a), isTrue);
      await pumpEventQueue();

      var task = await stored(a);
      expect(task.held, isTrue);
      expect(task.status, TaskStatus.pending);
      expect(task.nextAttemptAt, StoredTask.heldNextAttemptAt);
      expect(task.releaseAt, isNull);
      expect(changes.single.held, isTrue);
      expect(changes.single.status, TaskStatus.pending);

      await h.queue.start();
      await h.until(() => ran.length == 1);
      await pumpEventQueue();
      expect(ran, ['b']);

      expect(await h.queue.release(a), isTrue);
      await h.until(() => ran.length == 2);
      task = await stored(a);
      expect(task.held, isFalse);
      expect(task.status, TaskStatus.completed);
      expect(changes.last.held, isFalse);
    });

    test('keeps its start time for release', () async {
      final start = h.clock.now().add(const Duration(minutes: 10));
      final id = await h.queue.enqueue(ValueTask('a'), runAt: start);
      await h.queue.hold(id);
      expect((await stored(id)).releaseAt, start);
      await h.queue.start();

      h.clock.advance(const Duration(minutes: 5));
      await pumpEventQueue();
      expect(ran, isEmpty);

      await h.queue.release(id);
      final task = await stored(id);
      expect(task.nextAttemptAt, start);
      expect(task.releaseAt, isNull);
      await pumpEventQueue();
      expect(ran, isEmpty);
      h.clock.advance(const Duration(minutes: 5));
      await h.until(() => ran.length == 1);
    });

    test(
      'a held dependent stays held after its dependency completes',
      () async {
        final parent = await h.queue.enqueue(ValueTask('parent'));
        final child = await h.queue.enqueue(
          ValueTask('child'),
          dependsOn: [parent],
        );
        await h.queue.hold(child);
        await h.queue.start();
        await h.until(() => ran.length == 1);
        await pumpEventQueue();
        final task = await stored(child);
        expect(task.status, TaskStatus.pending);
        expect(task.held, isTrue);
        await h.queue.release(child);
        await h.until(() => ran.length == 2);
      },
    );

    test('cancel still cancels it, and clears the hold', () async {
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.hold(id);
      await h.queue.cancel(id);
      final task = await stored(id);
      expect(task.status, TaskStatus.cancelled);
      expect(task.held, isFalse);
      expect(task.nextAttemptAt, isNull);
      expect(await h.queue.release(id), isFalse);
    });

    test('dependents cancelled because of it are not held', () async {
      final parent = await h.queue.enqueue(ValueTask('parent'));
      final child = await h.queue.enqueue(
        ValueTask('child'),
        dependsOn: [parent],
      );
      await h.queue.hold(child);
      await h.queue.cancel(parent);
      final task = await stored(child);
      expect(task.status, TaskStatus.cancelled);
      expect(task.held, isFalse);
      expect(task.releaseAt, isNull);
    });

    test('hold and release report whether anything changed', () async {
      final id = await h.queue.enqueue(ValueTask('a'));
      expect(await h.queue.release(id), isFalse);
      expect(await h.queue.hold(id), isTrue);
      expect(await h.queue.hold(id), isFalse);
      expect(await h.queue.release(id), isTrue);
      await pumpEventQueue();
      expect(changes, hasLength(2));
      expect(h.queue.hold('missing'), throwsA(isA<TaskNotFoundException>()));
      expect(h.queue.release('missing'), throwsA(isA<TaskNotFoundException>()));
    });

    test('finished tasks cannot be held', () async {
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await h.until(() => ran.length == 1);
      await pumpEventQueue();
      expect(await h.queue.hold(id), isFalse);
      expect((await stored(id)).held, isFalse);
    });

    test('drain ignores held tasks and reports no wake time', () async {
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.hold(id);
      final result = await h.queue.drain(waitForScheduled: true);
      expect(result.reason, DrainStopReason.idle);
      expect(result.nextWakeAt, isNull);
      expect(ran, isEmpty);
    });

    test('the hold survives a restart', () async {
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.hold(id);
      final copy = await reloadStorage(h.storage);
      final loaded = (await copy.get(id))!;
      expect(loaded.held, isTrue);
      expect(loaded.nextAttemptAt, StoredTask.heldNextAttemptAt);
      expect(await copy.getNextReady(DateTime.utc(3000)), isNull);
    });

    test('a task released after its expiry is cancelled', () async {
      final id = await h.queue.enqueue(
        ValueTask('a'),
        expiresAfter: const Duration(minutes: 1),
      );
      await h.queue.hold(id);
      await h.queue.start();
      h.clock.advance(const Duration(minutes: 2));
      await pumpEventQueue();
      expect(h.of<TaskCancelled>(), isEmpty);
      await h.queue.release(id);
      await h.until(() => h.of<TaskCancelled>().length == 1);
      expect(ran, isEmpty);
    });
  });

  group('a running task', () {
    test(
      'is asked to stop and goes back held; the attempt does not count',
      () async {
        var sawRequest = false;
        var calls = 0;
        h.register(
          handler: (task, context) async {
            calls++;
            if (calls == 1) {
              await context.whenCancellationRequested;
              sawRequest = context.isCancellationRequested;
              throw const TaskCancelledException();
            }
            ran.add(task.value);
          },
        );
        final id = await h.queue.enqueue(
          ValueTask('upload'),
          retryPolicy: RetryPolicy.fixed(
            maxAttempts: 3,
            delay: const Duration(seconds: 1),
          ),
        );
        await h.queue.start();
        await h.until(() => calls == 1);
        expect(await h.queue.hold(id), isTrue);
        expect((await stored(id)).held, isTrue);

        await h.until(() => changes.length == 2);
        var task = await stored(id);
        expect(task.status, TaskStatus.pending);
        expect(task.attempts, 0);
        expect(task.held, isTrue);
        expect(task.nextAttemptAt, StoredTask.heldNextAttemptAt);
        expect(h.of<TaskCancelled>(), isEmpty);
        expect(sawRequest, isTrue);
        expect(changes.last.status, TaskStatus.pending);

        await h.queue.release(id);
        await h.until(() => ran.length == 1);
        task = await stored(id);
        expect(task.status, TaskStatus.completed);
        expect(task.attempts, 1);
        expect(h.of<TaskStarted>().map((e) => e.attempt), [1, 1]);
      },
    );

    test('a later attempt goes back as retryScheduled', () async {
      var calls = 0;
      h.register(
        handler: (task, context) async {
          calls++;
          if (calls == 1) throw TemporaryFailure();
          if (calls == 2) {
            await context.whenCancellationRequested;
            throw StateError('stopped');
          }
        },
      );
      final id = await h.queue.enqueue(
        ValueTask('a'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 3,
          delay: const Duration(seconds: 1),
        ),
      );
      await h.queue.start();
      await h.until(() => h.of<TaskRetryScheduled>().length == 1);
      h.clock.advance(const Duration(seconds: 1));
      await h.until(() => calls == 2);
      await h.queue.hold(id);
      await h.until(() => changes.length == 2);
      final task = await stored(id);
      expect(task.status, TaskStatus.retryScheduled);
      expect(task.attempts, 1);
      expect(task.lastFailure!.error, 'TemporaryFailure');
      expect(h.of<TaskRetryScheduled>(), hasLength(1));
    });

    test('a handler that finishes anyway completes the task', () async {
      final release = Completer<void>();
      h.register(handler: (task, context) => release.future);
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await pumpEventQueue();
      await h.queue.hold(id);
      release.complete();
      await h.until(() => h.of<TaskCompleted>().length == 1);
      final task = await stored(id);
      expect(task.status, TaskStatus.completed);
      expect(task.held, isFalse);
    });

    test('released before the handler stops: runs again at once', () async {
      var calls = 0;
      h.register(
        handler: (task, context) async {
          calls++;
          if (calls == 1) {
            await context.whenCancellationRequested;
            await Future<void>.delayed(Duration.zero);
            throw const TaskCancelledException();
          }
        },
      );
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await h.until(() => calls == 1);
      await h.queue.hold(id);
      await h.queue.release(id);
      await h.until(() => h.of<TaskCompleted>().length == 1);
      expect(calls, 2);
      expect(h.of<TaskCancelled>(), isEmpty);
      expect((await stored(id)).attempts, 1);
    });

    test('cancel wins over hold', () async {
      h.register(
        handler: (task, context) async {
          await context.whenCancellationRequested;
          throw const TaskCancelledException();
        },
      );
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await pumpEventQueue();
      await h.queue.cancel(id);
      expect(await h.queue.hold(id), isFalse);
      await h.until(() => h.of<TaskCancelled>().length == 1);
      expect((await stored(id)).held, isFalse);
    });

    test('a held task interrupted by a crash comes back held', () async {
      final storage = MemoryQueueStorage();
      await storage.save(
        storedTask(
          id: 'a',
          status: TaskStatus.running,
          attempts: 1,
        ).copyWith(held: true),
      );
      final local = QueueHarness(storage: storage);
      addTearDown(local.queue.close);
      local.register(handler: (task, context) async => ran.add(task.value));
      await local.queue.start();
      await pumpEventQueue();
      final task = (await storage.get('a'))!;
      expect(task.status, TaskStatus.pending);
      expect(task.attempts, 0);
      expect(task.held, isTrue);
      expect(ran, isEmpty);
      await local.queue.release('a');
      await local.until(() => ran.length == 1);
    });
  });

  group('a handed-off task', () {
    setUp(() {
      h.register(
        handler: (task, context) async =>
            context.handOff('ref-${context.attempt}'),
      );
    });

    test('stays running and is reported for the outside work', () async {
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await pumpEventQueue();
      expect(await h.queue.hold(id), isTrue);
      await pumpEventQueue();
      var task = await stored(id);
      expect(task.status, TaskStatus.running);
      expect(task.isHandedOff, isTrue);
      expect(task.held, isTrue);
      expect(changes.single.isHandedOff, isTrue);
      expect(changes.single.held, isTrue);

      expect(await h.queue.release(id), isTrue);
      await pumpEventQueue();
      expect(changes.last.isHandedOff, isTrue);
      expect(changes.last.held, isFalse);
      expect(await h.queue.completeHandOff(id, reference: 'ref-1'), isTrue);
      task = await stored(id);
      expect(task.status, TaskStatus.completed);
    });

    test('stopping the outside work puts it back held', () async {
      final id = await h.queue.enqueue(
        ValueTask('a'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 2,
          delay: const Duration(seconds: 1),
        ),
      );
      await h.queue.start();
      await pumpEventQueue();
      await h.queue.hold(id);
      expect(
        await h.queue.failHandOff(
          id,
          const TaskCancelledException(),
          reference: 'ref-1',
        ),
        isTrue,
      );
      final task = await stored(id);
      expect(task.status, TaskStatus.pending);
      expect(task.attempts, 0);
      expect(task.held, isTrue);
      expect(task.handOffReference, isNull);
      expect(h.of<TaskCancelled>(), isEmpty);

      await h.queue.release(id);
      await h.until(() => h.of<TaskStarted>().length == 2);
      expect((await stored(id)).handOffReference, 'ref-1');
    });

    test('an outcome while held still applies', () async {
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await pumpEventQueue();
      await h.queue.hold(id);
      expect(await h.queue.completeHandOff(id, reference: 'ref-1'), isTrue);
      final task = await stored(id);
      expect(task.status, TaskStatus.completed);
      expect(task.held, isFalse);
    });

    test('a hold asked for before the hand-off is reported after it', () async {
      final local = QueueHarness();
      addTearDown(local.queue.close);
      final seen = <TaskHoldChanged>[];
      local.queue.holdChanges.listen(seen.add);
      final go = Completer<void>();
      local.register(
        handler: (task, context) async {
          await go.future;
          context.handOff('ref');
        },
      );
      final id = await local.queue.enqueue(ValueTask('a'));
      await local.queue.start();
      await pumpEventQueue();
      await local.queue.hold(id);
      go.complete();
      await local.until(() => seen.length == 2);
      expect(seen.last.isHandedOff, isTrue);
      expect(seen.last.held, isTrue);
    });
  });
}
