import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';
import 'support/probe_storage.dart';

void main() {
  group('priority', () {
    test('higher priority starts first; ties keep enqueue order', () async {
      final h = QueueHarness();
      final order = <String>[];
      h.register(handler: (task, _) async => order.add(task.value));

      await h.queue.enqueue(ValueTask('low'), priority: -1);
      await h.queue.enqueue(ValueTask('normal-1'));
      await h.queue.enqueue(ValueTask('high'), priority: 5);
      await h.queue.enqueue(ValueTask('normal-2'));
      await h.queue.start();
      await h.until(() => order.length == 4);

      expect(order, ['high', 'normal-1', 'normal-2', 'low']);
    });

    test('a due retry competes by priority with pending work', () async {
      final h = QueueHarness();
      final order = <String>[];
      var failedOnce = false;
      h.register(
        handler: (task, _) async {
          if (task.value == 'urgent' && !failedOnce) {
            failedOnce = true;
            throw TemporaryFailure();
          }
          order.add(task.value);
        },
      );
      await h.queue.enqueue(
        ValueTask('urgent'),
        priority: 10,
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 2,
          delay: const Duration(seconds: 5),
        ),
      );
      await h.queue.start();
      await h.until(() => h.of<TaskRetryScheduled>().isNotEmpty);
      await h.queue.pause();
      await h.queue.enqueue(ValueTask('a'));
      await h.queue.enqueue(ValueTask('b'));
      h.clock.advance(const Duration(seconds: 5));
      await h.queue.resume();
      await h.until(() => order.length == 3);

      expect(order, ['urgent', 'a', 'b']);
    });
  });

  group('dependencies', () {
    test('a dependent waits until every dependency completes', () async {
      final h = QueueHarness();
      final order = <String>[];
      final gates = {'a': Completer<void>(), 'b': Completer<void>()};
      h.register(
        handler: (task, _) async {
          await gates[task.value]?.future;
          order.add(task.value);
        },
      );
      final a = await h.queue.enqueue(ValueTask('a'));
      final b = await h.queue.enqueue(ValueTask('b'));
      final c = await h.queue.enqueue(ValueTask('c'), dependsOn: [a, b]);
      expect((await h.queue.getTask(c))?.status, TaskStatus.waiting);
      expect((await h.queue.getTask(c))?.dependsOn, [a, b]);

      await h.queue.start();
      gates['a']!.complete();
      await h.until(() => order.contains('a'));
      expect((await h.queue.getTask(c))?.status, TaskStatus.waiting);

      gates['b']!.complete();
      await h.until(() => order.length == 3);
      expect(order, ['a', 'b', 'c']);
      expect((await h.queue.getTask(c))?.status, TaskStatus.completed);
    });

    test(
      'a failed dependency cancels dependents by default, transitively',
      () async {
        final h = QueueHarness();
        final ran = <String>[];
        h.register(
          handler: (task, _) async {
            ran.add(task.value);
            if (task.value == 'root') throw TemporaryFailure();
          },
        );
        final root = await h.queue.enqueue(ValueTask('root'));
        final child = await h.queue.enqueue(
          ValueTask('child'),
          dependsOn: [root],
        );
        final grandchild = await h.queue.enqueue(
          ValueTask('grandchild'),
          dependsOn: [child],
        );

        await h.queue.start();
        await h.until(() => h.of<TaskCancelled>().length == 2);

        expect(ran, ['root']);
        final childRecord = await h.queue.getTask(child);
        expect(childRecord?.status, TaskStatus.cancelled);
        expect(childRecord?.lastFailure?.error, contains(root));
        expect(childRecord?.lastFailure?.error, contains('failed'));
        expect(
          (await h.queue.getTask(grandchild))?.status,
          TaskStatus.cancelled,
        );
        expect(h.of<TaskCancelled>().map((event) => event.taskId), [
          child,
          grandchild,
        ]);
        expect(
          h.of<TaskCancelled>().every((event) => !event.wasRunning),
          isTrue,
        );
      },
    );

    test('DependencyFailurePolicy.fail fails the dependent', () async {
      final h = QueueHarness();
      h.register(
        handler: (task, _) async {
          if (task.value == 'root') throw TemporaryFailure();
        },
      );
      final root = await h.queue.enqueue(ValueTask('root'));
      final child = await h.queue.enqueue(
        ValueTask('child'),
        dependsOn: [root],
        onDependencyFailure: DependencyFailurePolicy.fail,
      );

      await h.queue.start();
      await h.until(() => h.of<TaskFailed>().length == 2);

      final record = await h.queue.getTask(child);
      expect(record?.status, TaskStatus.failed);
      expect(record?.attempts, 0);
      expect(
        h.of<TaskFailed>().last.failure.error,
        startsWithDependencyFailure,
      );
    });

    test(
      'DependencyFailurePolicy.run waits for every dependency to finish',
      () async {
        final h = QueueHarness();
        final ran = <String>[];
        final slow = Completer<void>();
        h.register(
          handler: (task, _) async {
            if (task.value == 'slow') await slow.future;
            ran.add(task.value);
            if (task.value == 'bad') throw TemporaryFailure();
          },
        );
        final bad = await h.queue.enqueue(ValueTask('bad'));
        final slowId = await h.queue.enqueue(ValueTask('slow'));
        final cleanup = await h.queue.enqueue(
          ValueTask('cleanup'),
          dependsOn: [bad, slowId],
          onDependencyFailure: DependencyFailurePolicy.run,
        );

        await h.queue.start();
        await h.until(() => h.of<TaskFailed>().isNotEmpty);
        await pumpEventQueue();
        expect((await h.queue.getTask(cleanup))?.status, TaskStatus.waiting);

        slow.complete();
        await h.until(() => ran.contains('cleanup'));
        expect(ran, ['bad', 'slow', 'cleanup']);
      },
    );

    test('dependencies are checked at enqueue time', () async {
      final h = QueueHarness();
      h.register(
        handler: (task, _) async {
          if (task.value == 'bad') throw TemporaryFailure();
        },
      );
      final ok = await h.queue.enqueue(ValueTask('ok'));
      final bad = await h.queue.enqueue(ValueTask('bad'));
      await h.queue.start();
      await h.until(() => h.of<TaskFailed>().isNotEmpty);
      await h.until(() => h.of<TaskCompleted>().isNotEmpty);
      await h.queue.pause();

      final released = await h.queue.enqueue(
        ValueTask('after-ok'),
        dependsOn: [ok],
      );
      expect((await h.queue.getTask(released))?.status, TaskStatus.pending);

      final blocked = await h.queue.enqueue(
        ValueTask('after-bad'),
        dependsOn: [bad],
      );
      expect((await h.queue.getTask(blocked))?.status, TaskStatus.cancelled);
      await h.until(() => h.of<TaskCancelled>().isNotEmpty);
      expect(h.of<TaskEnqueued>().last.taskId, blocked);
      expect(h.of<TaskCancelled>().single.taskId, blocked);

      await expectLater(
        h.queue.enqueue(ValueTask('x'), dependsOn: ['missing']),
        throwsA(isA<TaskNotFoundException>()),
      );
      expect(
        () => h.queue.enqueue(ValueTask('x'), dependsOn: ['']),
        throwsArgumentError,
      );
      expect(await h.queue.getTasks(), hasLength(4));
    });

    test('duplicate dependency ids are stored once', () async {
      final h = QueueHarness();
      h.register();
      final a = await h.queue.enqueue(ValueTask('a'));
      final b = await h.queue.enqueue(ValueTask('b'), dependsOn: [a, a]);
      expect((await h.queue.getTask(b))?.dependsOn, [a]);
    });

    test(
      'cancelling a waiting task or its dependency settles dependents',
      () async {
        final h = QueueHarness();
        h.register();
        final a = await h.queue.enqueue(ValueTask('a'));
        final b = await h.queue.enqueue(ValueTask('b'), dependsOn: [a]);
        final c = await h.queue.enqueue(ValueTask('c'), dependsOn: [b]);
        final d = await h.queue.enqueue(
          ValueTask('d'),
          dependsOn: [b],
          onDependencyFailure: DependencyFailurePolicy.run,
        );

        await h.queue.cancel(b);

        expect((await h.queue.getTask(a))?.status, TaskStatus.pending);
        expect((await h.queue.getTask(b))?.status, TaskStatus.cancelled);
        expect((await h.queue.getTask(c))?.status, TaskStatus.cancelled);
        expect((await h.queue.getTask(d))?.status, TaskStatus.pending);
      },
    );

    test('a waiting task counts as active for deduplication', () async {
      final h = QueueHarness();
      h.register();
      final a = await h.queue.enqueue(ValueTask('a'));
      final first = await h.queue.enqueue(
        ValueTask('b'),
        dependsOn: [a],
        deduplicationKey: 'key',
      );
      final second = await h.queue.enqueue(
        ValueTask('b'),
        deduplicationKey: 'key',
      );
      expect(second, first);
    });

    test(
      'retry of a dependent requires its dependencies to be healthy',
      () async {
        final h = QueueHarness();
        var failRoot = true;
        h.register(
          handler: (task, _) async {
            if (task.value == 'root' && failRoot) throw TemporaryFailure();
          },
        );
        final root = await h.queue.enqueue(ValueTask('root'));
        final child = await h.queue.enqueue(
          ValueTask('child'),
          dependsOn: [root],
        );
        await h.queue.start();
        await h.until(() => h.of<TaskCancelled>().isNotEmpty);

        await expectLater(h.queue.retry(child), throwsStateError);

        failRoot = false;
        await h.queue.pause();
        await h.queue.retry(root);
        await h.queue.retry(child);
        expect((await h.queue.getTask(child))?.status, TaskStatus.waiting);

        await h.queue.resume();
        await h.until(() => h.of<TaskCompleted>().length == 2);
        expect((await h.queue.getTask(child))?.status, TaskStatus.completed);
      },
    );

    test('a dependency that a waiting task needs cannot be deleted', () async {
      final h = QueueHarness();
      h.register();
      final done = await h.queue.enqueue(ValueTask('done'));
      await h.queue.start();
      await h.until(() => h.of<TaskCompleted>().isNotEmpty);
      await h.queue.pause();
      final other = await h.queue.enqueue(ValueTask('other'));
      final waiting = await h.queue.enqueue(
        ValueTask('waiting'),
        dependsOn: [done, other],
      );
      expect((await h.queue.getTask(waiting))?.status, TaskStatus.waiting);

      await expectLater(h.queue.delete(done), throwsStateError);
      expect(await h.queue.purge(), 0);
      expect(await h.queue.getTask(done), isNotNull);
    });

    test(
      'recovery that fails an interrupted task settles its dependents',
      () async {
        final storage = MemoryQueueStorage();
        await storage.save(
          storedTask(id: 'root', status: TaskStatus.running, attempts: 1),
        );
        await storage.save(
          storedTask(
            id: 'child',
            status: TaskStatus.waiting,
            dependsOn: ['root'],
            sequence: 1,
          ),
        );
        final h = QueueHarness(storage: storage);
        h.register();

        await h.queue.start();

        expect((await storage.get('root'))?.status, TaskStatus.failed);
        expect((await storage.get('child'))?.status, TaskStatus.cancelled);
        expect(h.of<TaskFailed>().single.taskId, 'root');
        expect(h.of<TaskCancelled>().single.taskId, 'child');
      },
    );

    test('dependents are written before the task that settles them', () async {
      final storage = ProbeStorage();
      final h = QueueHarness(storage: storage);
      h.register();
      final a = await h.queue.enqueue(ValueTask('a'));
      final b = await h.queue.enqueue(ValueTask('b'), dependsOn: [a]);
      final c = await h.queue.enqueue(ValueTask('c'), dependsOn: [b]);
      final writes = <String>[];
      storage.before = (method, argument) async {
        if (method == 'update') writes.add((argument! as StoredTask).id);
      };

      await h.queue.cancel(a);

      expect(writes, [c, b, a]);
    });

    test('a crash mid-cascade never strands a waiting task', () async {
      final storage = ProbeStorage();
      final h = QueueHarness(storage: storage);
      h.register();
      final a = await h.queue.enqueue(ValueTask('a'));
      final b = await h.queue.enqueue(ValueTask('b'), dependsOn: [a]);
      storage.before = (method, argument) async {
        if (method == 'update' && (argument! as StoredTask).id == a) {
          throw StateError('disk full');
        }
      };

      await expectLater(h.queue.cancel(a), throwsStateError);

      // The dependency write failed, so it is still pending; its dependent
      // was already settled rather than left waiting on a cancelled task.
      expect((await storage.inner.get(a))?.status, TaskStatus.pending);
      expect((await storage.inner.get(b))?.status, TaskStatus.cancelled);
      expect(await storage.inner.getByStatus(TaskStatus.waiting), isEmpty);
    });
  });

  group('chains', () {
    test('steps run one at a time, in order', () async {
      final h = QueueHarness(maxConcurrentTasks: 3);
      final order = <String>[];
      var running = 0;
      var maxRunning = 0;
      h.register(
        handler: (task, _) async {
          running++;
          maxRunning = running > maxRunning ? running : maxRunning;
          await Future<void>.delayed(Duration.zero);
          order.add(task.value);
          running--;
        },
      );

      final ids = await h.queue.enqueueChain(
        [ValueTask('one'), ValueTask('two'), ValueTask('three')],
        group: 'checkout',
        priority: 2,
      );
      await h.queue.start();
      await h.until(() => order.length == 3);

      expect(order, ['one', 'two', 'three']);
      expect(maxRunning, 1);
      final records = [for (final id in ids) (await h.queue.getTask(id))!];
      expect(records.map((task) => task.group).toSet(), {'checkout'});
      expect(records.map((task) => task.priority).toSet(), {2});
      expect(records[1].dependsOn, [ids[0]]);
      expect(records[2].dependsOn, [ids[1]]);
    });

    test('a failing step cancels the rest of the chain', () async {
      final h = QueueHarness();
      final ran = <String>[];
      h.register(
        handler: (task, _) async {
          ran.add(task.value);
          if (task.value == 'two') throw TemporaryFailure();
        },
      );
      final ids = await h.queue.enqueueChain([
        ValueTask('one'),
        ValueTask('two'),
        ValueTask('three'),
      ]);
      await h.queue.start();
      await h.until(() => h.of<TaskCancelled>().isNotEmpty);

      expect(ran, ['one', 'two']);
      expect((await h.queue.getTask(ids[2]))?.status, TaskStatus.cancelled);
    });

    test('the first step can depend on existing tasks', () async {
      final h = QueueHarness();
      h.register();
      final before = await h.queue.enqueue(ValueTask('before'));
      final ids = await h.queue.enqueueChain(
        [ValueTask('one')],
        dependsOn: [before],
      );
      expect((await h.queue.getTask(ids.single))?.dependsOn, [before]);
    });

    test('an invalid chain stores nothing', () async {
      final h = QueueHarness();
      h.register();
      await expectLater(h.queue.enqueueChain([]), throwsArgumentError);
      await expectLater(
        h.queue.enqueueChain([ValueTask('a'), RenamedValueTask()]),
        throwsA(isA<UnknownTaskTypeException>()),
      );
      expect(await h.queue.getTasks(), isEmpty);
    });
  });

  group('groups', () {
    test('tasks can be queried by group and status', () async {
      final h = QueueHarness();
      h.register();
      final a = await h.queue.enqueue(ValueTask('a'), group: 'sync');
      await h.queue.enqueue(ValueTask('b'), group: 'upload');
      final c = await h.queue.enqueue(ValueTask('c'), group: 'sync');
      await h.queue.cancel(c);

      expect((await h.queue.getTasks(group: 'sync')).map((t) => t.id), [a, c]);
      expect(
        (await h.queue.getTasks(
          group: 'sync',
          status: TaskStatus.cancelled,
        )).map((t) => t.id),
        [c],
      );
      expect(await h.queue.getTasks(group: 'none'), isEmpty);
      expect(
        () => h.queue.enqueue(ValueTask('x'), group: ''),
        throwsArgumentError,
      );
    });

    test('cancelGroup cancels queued work and flags running work', () async {
      final h = QueueHarness();
      final gate = Completer<void>();
      h.register(
        handler: (task, context) async {
          if (task.value == 'running') {
            await gate.future;
            if (context.isCancellationRequested) {
              throw const TaskCancelledException();
            }
          }
        },
      );
      final running = await h.queue.enqueue(
        ValueTask('running'),
        group: 'sync',
      );
      await h.queue.start();
      await h.until(() => h.of<TaskStarted>().isNotEmpty);
      final queued = await h.queue.enqueue(ValueTask('queued'), group: 'sync');
      final waiting = await h.queue.enqueue(
        ValueTask('waiting'),
        group: 'sync',
        dependsOn: [running],
      );
      final outsider = await h.queue.enqueue(
        ValueTask('outsider'),
        dependsOn: [queued],
      );
      await h.queue.pause();

      expect(await h.queue.cancelGroup('sync'), 3);
      expect((await h.queue.getTask(queued))?.status, TaskStatus.cancelled);
      expect((await h.queue.getTask(waiting))?.status, TaskStatus.cancelled);
      expect((await h.queue.getTask(outsider))?.status, TaskStatus.cancelled);
      expect((await h.queue.getTask(running))?.cancelRequested, isTrue);

      gate.complete();
      await h.until(
        () => h.of<TaskCancelled>().any((event) => event.taskId == running),
      );
      expect(await h.queue.cancelGroup('sync'), 0);
    });
  });

  test('a closed queue rejects chains and group cancellation', () async {
    final h = QueueHarness();
    h.register();
    await h.queue.close();
    expect(() => h.queue.enqueueChain([ValueTask('a')]), throwsStateError);
    expect(() => h.queue.cancelGroup('sync'), throwsStateError);
  });
}

final Matcher startsWithDependencyFailure = predicate<String>(
  (error) => error.startsWith('DependencyFailedException'),
  'starts with DependencyFailedException',
);
