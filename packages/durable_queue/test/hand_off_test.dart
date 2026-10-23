import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';
import 'support/probe_storage.dart';

void main() {
  late QueueHarness h;
  late List<String> ran;
  late Map<String, TaskContext> contexts;

  setUp(() {
    h = QueueHarness();
    ran = [];
    contexts = {};
  });

  tearDown(() => h.queue.close());

  /// Registers a handler that hands off tasks whose value starts with 'up'.
  void registerHandingOff({RetryPredicate? retryIf}) {
    h.register(
      retryIf: retryIf,
      handler: (task, context) async {
        ran.add('${task.value}#${context.attempt}');
        contexts[task.value] = context;
        if (task.value.startsWith('up')) {
          context.handOff('transfer-${task.value}-${context.attempt}');
        }
      },
    );
  }

  Future<StoredTask> stored(String id) async => (await h.queue.getTask(id))!;

  test('a handed-off task stays running until completeHandOff', () async {
    registerHandingOff();
    final id = await h.queue.enqueue(ValueTask('up'));
    await h.queue.start();
    await h.until(() => ran.isNotEmpty);
    await pumpEventQueue();

    var task = await stored(id);
    expect(task.status, TaskStatus.running);
    expect(task.handOffReference, 'transfer-up-1');
    expect(task.isHandedOff, isTrue);
    expect(h.of<TaskCompleted>(), isEmpty);

    expect(
      await h.queue.completeHandOff(id, reference: 'transfer-up-1'),
      isTrue,
    );
    task = await stored(id);
    expect(task.status, TaskStatus.completed);
    expect(task.isHandedOff, isFalse);
    expect(task.handOffReference, 'transfer-up-1');
    expect(h.of<TaskCompleted>().single.taskId, id);

    expect(await h.queue.completeHandOff(id), isFalse, reason: 'repeat');
    expect(h.of<TaskCompleted>(), hasLength(1));
  });

  test('a handed-off task does not hold a concurrency slot', () async {
    registerHandingOff();
    await h.queue.enqueue(ValueTask('up'));
    await h.queue.enqueue(ValueTask('next'));
    await h.queue.start();
    await h.until(() => ran.length == 2);
    expect(ran, ['up#1', 'next#1']);
  });

  test('dependents wait for the outcome', () async {
    registerHandingOff();
    final ids = await h.queue.enqueueChain([
      ValueTask('up'),
      ValueTask('notify'),
    ]);
    await h.queue.start();
    await h.until(() => ran.isNotEmpty);
    await pumpEventQueue();
    expect(ran, ['up#1']);
    await h.queue.completeHandOff(ids.first);
    await h.until(() => ran.length == 2);
    expect(ran, ['up#1', 'notify#1']);
  });

  test('failHandOff retries by running the handler again', () async {
    registerHandingOff();
    final id = await h.queue.enqueue(
      ValueTask('up'),
      retryPolicy: RetryPolicy.fixed(maxAttempts: 3, delay: Duration.zero),
    );
    await h.queue.start();
    await h.until(() => ran.isNotEmpty);
    await pumpEventQueue();

    expect(
      await h.queue.failHandOff(
        id,
        StateError('upload failed'),
        reference: 'transfer-up-1',
      ),
      isTrue,
    );
    await h.until(() => ran.length == 2);
    expect(ran, ['up#1', 'up#2']);
    await pumpEventQueue();
    final task = await stored(id);
    expect(task.handOffReference, 'transfer-up-2');
    expect(h.of<TaskRetryScheduled>().single.error, contains('upload failed'));
    expect(
      await h.queue.completeHandOff(id, reference: 'transfer-up-1'),
      isFalse,
      reason: 'an outcome for the old attempt',
    );
    expect(
      await h.queue.completeHandOff(id, reference: 'transfer-up-2'),
      isTrue,
    );
  });

  test('failHandOff follows retryIf', () async {
    registerHandingOff(retryIf: (error, _) => error is! ArgumentError);
    final id = await h.queue.enqueue(ValueTask('up'));
    await h.queue.start();
    await h.until(() => ran.isNotEmpty);
    await pumpEventQueue();
    await h.queue.failHandOff(id, ArgumentError('rejected'));
    final task = await stored(id);
    expect(task.status, TaskStatus.failed);
    expect(task.lastFailure!.error, contains('rejected'));
    expect(h.of<TaskFailed>(), hasLength(1));
  });

  test('a TaskCancelledException outcome cancels', () async {
    registerHandingOff();
    final id = await h.queue.enqueue(ValueTask('up'));
    await h.queue.start();
    await h.until(() => ran.isNotEmpty);
    await pumpEventQueue();
    await h.queue.failHandOff(id, const TaskCancelledException());
    expect((await stored(id)).status, TaskStatus.cancelled);
  });

  test('outcomes for unknown or not handed-off tasks are ignored', () async {
    final release = Completer<void>();
    h.register(
      handler: (task, context) async {
        ran.add(task.value);
        await release.future;
      },
    );
    final pending = await h.queue.enqueue(
      ValueTask('later'),
      delay: const Duration(hours: 1),
    );
    final running = await h.queue.enqueue(ValueTask('running'));
    await h.queue.start();
    await h.until(() => ran.isNotEmpty);
    expect(await h.queue.completeHandOff('nope'), isFalse);
    expect(await h.queue.completeHandOff(pending), isFalse);
    expect(await h.queue.completeHandOff(running), isFalse);
    expect(await h.queue.failHandOff(running, StateError('x')), isFalse);
    release.complete();
    await h.until(() => h.of<TaskCompleted>().isNotEmpty);
    expect(await h.queue.completeHandOff(running), isFalse);
  });

  test('a wrong reference is ignored', () async {
    registerHandingOff();
    final id = await h.queue.enqueue(ValueTask('up'));
    await h.queue.start();
    await h.until(() => ran.isNotEmpty);
    await pumpEventQueue();
    expect(await h.queue.completeHandOff(id, reference: 'other'), isFalse);
    expect(
      await h.queue.failHandOff(id, StateError('x'), reference: 'other'),
      isFalse,
    );
    expect((await stored(id)).isHandedOff, isTrue);
  });

  test('a restart leaves handed-off tasks alone', () async {
    final storage = MemoryQueueStorage();
    final first = QueueHarness(storage: storage, clock: h.clock);
    first.register(
      handler: (task, context) async => context.handOff('ref-${task.value}'),
    );
    final id = await first.queue.enqueue(ValueTask('a'));
    await first.queue.start();
    await first.until(() => first.of<TaskStarted>().isNotEmpty);
    await pumpEventQueue();
    await first.queue.close();

    final second = QueueHarness(
      storage: await reloadStorage(storage),
      clock: h.clock,
    );
    second.register();
    await second.queue.start();
    await pumpEventQueue();
    final task = (await second.queue.getTask(id))!;
    expect(task.status, TaskStatus.running);
    expect(task.isHandedOff, isTrue);
    expect(second.of<TaskRetryScheduled>(), isEmpty);
    expect(await second.queue.completeHandOff(id, reference: 'ref-a'), isTrue);
    expect((await second.queue.getTask(id))!.status, TaskStatus.completed);
    await second.queue.close();
  });

  test('the reference is saved before the handler returns', () async {
    final storage = MemoryQueueStorage();
    final first = QueueHarness(storage: storage, clock: h.clock);
    final never = Completer<void>();
    first.register(
      handler: (task, context) async {
        context.handOff('ref');
        await never.future;
      },
    );
    final id = await first.queue.enqueue(ValueTask('a'));
    await first.queue.start();
    await first.until(() => first.of<TaskStarted>().isNotEmpty);
    await pumpEventQueue();
    expect((await storage.get(id))!.handOffReference, 'ref');

    // The process dies here. The next one must not start the work again.
    final second = QueueHarness(
      storage: await reloadStorage(storage),
      clock: h.clock,
    );
    second.register();
    await second.queue.start();
    await pumpEventQueue();
    expect((await second.queue.getTask(id))!.isHandedOff, isTrue);
    expect(second.of<TaskStarted>(), isEmpty);
    await second.queue.close();
  });

  test(
    'an outcome reported while the handler still runs waits for it',
    () async {
      final release = Completer<void>();
      h.register(
        handler: (task, context) async {
          ran.add(task.value);
          context.handOff('ref');
          await release.future;
        },
      );
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await h.until(() => ran.isNotEmpty);
      expect(await h.queue.completeHandOff(id, reference: 'other'), isFalse);
      expect(await h.queue.completeHandOff(id, reference: 'ref'), isTrue);
      expect(await h.queue.completeHandOff(id), isFalse, reason: 'one outcome');
      await pumpEventQueue();
      expect((await stored(id)).status, TaskStatus.running);
      release.complete();
      await h.until(() => h.of<TaskCompleted>().isNotEmpty);
      expect((await stored(id)).status, TaskStatus.completed);
    },
  );

  test('an early failure is applied after the handler returns', () async {
    final release = Completer<void>();
    h.register(
      retryIf: (error, _) => false,
      handler: (task, context) async {
        ran.add(task.value);
        context.handOff('ref');
        await release.future;
      },
    );
    final id = await h.queue.enqueue(ValueTask('a'));
    await h.queue.start();
    await h.until(() => ran.isNotEmpty);
    expect(await h.queue.failHandOff(id, StateError('lost')), isTrue);
    release.complete();
    await h.until(() => h.of<TaskFailed>().isNotEmpty);
    expect((await stored(id)).lastFailure!.error, contains('lost'));
  });

  test('a handler that throws after handing off is a normal failure', () async {
    final release = Completer<void>();
    h.register(
      retryIf: (error, _) => false,
      handler: (task, context) async {
        ran.add(task.value);
        context.handOff('ref');
        await release.future;
        throw StateError('handler broke');
      },
    );
    final id = await h.queue.enqueue(ValueTask('a'));
    await h.queue.start();
    await h.until(() => ran.isNotEmpty);
    await h.queue.completeHandOff(id);
    release.complete();
    await h.until(() => h.of<TaskFailed>().isNotEmpty);
    final task = await stored(id);
    expect(task.status, TaskStatus.failed);
    expect(task.lastFailure!.error, contains('handler broke'));
    expect(h.of<TaskCompleted>(), isEmpty);
    expect(await h.queue.completeHandOff(id), isFalse);
  });

  test('a handler that cancels after handing off is cancelled', () async {
    h.register(
      handler: (task, context) async {
        context.handOff('ref');
        throw const TaskCancelledException();
      },
    );
    final id = await h.queue.enqueue(ValueTask('a'));
    await h.queue.start();
    await h.until(() => h.of<TaskCancelled>().isNotEmpty);
    expect((await stored(id)).status, TaskStatus.cancelled);
    expect(await h.queue.completeHandOff(id), isFalse);
  });

  test('cancel stops waiting for a handed-off task at once', () async {
    registerHandingOff();
    final ids = await h.queue.enqueueChain([ValueTask('up'), ValueTask('b')]);
    await h.queue.start();
    await h.until(() => ran.isNotEmpty);
    await pumpEventQueue();
    await h.queue.cancel(ids.first);
    await pumpEventQueue();
    final cancelled = h.of<TaskCancelled>();
    expect(cancelled.first.taskId, ids.first);
    expect(cancelled.first.wasRunning, isTrue);
    expect((await stored(ids.first)).status, TaskStatus.cancelled);
    expect((await stored(ids.first)).handOffReference, 'transfer-up-1');
    expect((await stored(ids.last)).status, TaskStatus.cancelled);
    expect(await h.queue.completeHandOff(ids.first), isFalse);
  });

  test(
    'cancel requested while the handler runs cancels the hand-off',
    () async {
      final release = Completer<void>();
      h.register(
        handler: (task, context) async {
          ran.add(task.value);
          context.handOff('ref');
          await release.future;
        },
      );
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await h.until(() => ran.isNotEmpty);
      await h.queue.cancel(id);
      release.complete();
      await h.until(() => h.of<TaskCancelled>().isNotEmpty);
      final task = await stored(id);
      expect(task.status, TaskStatus.cancelled);
      expect(task.handOffReference, 'ref');
      expect(task.cancelRequested, isFalse);
      expect(h.of<TaskCompleted>(), isEmpty);
    },
  );

  test('handOff is validated', () async {
    final errors = <Object>[];
    h.register(
      handler: (task, context) async {
        contexts[task.value] = context;
        try {
          context.handOff(' ');
        } on Object catch (error) {
          errors.add(error);
        }
        context.handOff('ref');
        try {
          context.handOff('again');
        } on Object catch (error) {
          errors.add(error);
        }
      },
    );
    await h.queue.enqueue(ValueTask('a'));
    await h.queue.start();
    await h.until(() => errors.length == 2);
    expect(errors.first, isA<ArgumentError>());
    expect(errors.last, isA<StateError>());
    await pumpEventQueue();
    expect(() => contexts['a']!.handOff('late'), throwsStateError);

    final manual = TaskContext(
      taskId: 'x',
      attempt: 1,
      maxAttempts: 1,
      idempotencyKey: null,
      deduplicationKey: null,
      isCancellationRequested: () => false,
    );
    expect(() => manual.handOff('ref'), throwsUnsupportedError);
  });

  test('drain does not wait for handed-off work', () async {
    registerHandingOff();
    await h.queue.enqueue(ValueTask('up'));
    await h.queue.enqueue(ValueTask('b'));
    final result = await h.queue.drain();
    expect(result.reason, DrainStopReason.idle);
    expect(result.started, 2);
    expect(result.completed, 1);
    final handedOff = await h.queue.getTasks(status: TaskStatus.running);
    expect(handedOff.single.isHandedOff, isTrue);
  });

  test(
    'outcomes work while the queue is idle and after close throws',
    () async {
      registerHandingOff();
      final id = await h.queue.enqueue(ValueTask('up'));
      await h.queue.drain();
      expect(h.queue.runState, QueueRunState.idle);
      expect(await h.queue.completeHandOff(id), isTrue);
      await h.queue.close();
      expect(() => h.queue.completeHandOff(id), throwsStateError);
    },
  );

  test('recovery pages past many handed-off tasks', () async {
    final storage = MemoryQueueStorage();
    final now = h.clock.now();
    for (var i = 0; i < 250; i++) {
      await storage.save(
        StoredTask(
          id: 'task-$i',
          type: 'value',
          payload: {'value': '$i'},
          status: TaskStatus.running,
          attempts: 1,
          retryPolicy: RetryPolicy.none(),
          createdAt: now,
          updatedAt: now,
          sequence: i + 1,
          // Handed-off tasks first, so whole batches hold nothing to recover.
          handOffReference: i < 220 ? 'ref-$i' : null,
        ),
      );
    }
    final queue = QueueHarness(storage: storage, clock: h.clock);
    queue.register();
    await queue.queue.start();
    await pumpEventQueue();
    final running = await queue.queue.getTasks(status: TaskStatus.running);
    expect(running, hasLength(220));
    expect(running.every((task) => task.isHandedOff), isTrue);
    expect(queue.of<TaskFailed>(), hasLength(30));
    await queue.queue.close();
  });

  test('a new attempt starts without the old reference', () async {
    final release = Completer<void>();
    h.register(
      handler: (task, context) async {
        ran.add('${context.attempt}');
        if (context.attempt == 1) {
          context.handOff('ref');
        } else {
          await release.future;
        }
      },
    );
    final id = await h.queue.enqueue(
      ValueTask('a'),
      retryPolicy: RetryPolicy.fixed(maxAttempts: 2, delay: Duration.zero),
    );
    await h.queue.start();
    await h.until(() => ran.isNotEmpty);
    await pumpEventQueue();
    await h.queue.failHandOff(id, StateError('x'));
    await h.until(() => ran.length == 2);
    final task = await stored(id);
    expect(task.status, TaskStatus.running);
    expect(task.handOffReference, isNull);
    expect(task.isHandedOff, isFalse);
    release.complete();
  });

  test('an outcome for a type that is no longer registered fails it', () async {
    final storage = MemoryQueueStorage();
    final first = QueueHarness(storage: storage, clock: h.clock);
    first.register(handler: (task, context) async => context.handOff('ref'));
    final id = await first.queue.enqueue(ValueTask('a'));
    await first.queue.drain();
    await first.queue.close();
    final second = QueueHarness(storage: storage, clock: h.clock);
    second.register(type: 'other');
    expect(await second.queue.failHandOff(id, StateError('x')), isTrue);
    expect((await second.queue.getTask(id))!.status, TaskStatus.failed);
    await second.queue.close();
  });

  test('a task another queue is running is not completed', () async {
    final storage = MemoryQueueStorage();
    final release = Completer<void>();
    final first = QueueHarness(storage: storage, clock: h.clock);
    first.register(handler: (task, context) => release.future);
    final id = await first.queue.enqueue(ValueTask('a'));
    await first.queue.start();
    await first.until(() => first.of<TaskStarted>().isNotEmpty);
    final second = QueueHarness(storage: storage, clock: h.clock);
    second.register();
    expect(await second.queue.completeHandOff(id), isFalse);
    expect((await storage.get(id))!.status, TaskStatus.running);
    release.complete();
    await first.until(() => first.of<TaskCompleted>().isNotEmpty);
    await first.queue.close();
    await second.queue.close();
  });

  test('the reference is stored even if saving it early fails', () async {
    final storage = ProbeStorage();
    final probe = QueueHarness(storage: storage, clock: h.clock);
    final release = Completer<void>();
    var failNext = false;
    storage.before = (method, argument) async {
      if (failNext && method == 'update') {
        failNext = false;
        throw StateError('disk busy');
      }
    };
    probe.register(
      handler: (task, context) async {
        failNext = true;
        context.handOff('ref');
        await release.future;
      },
    );
    final id = await probe.queue.enqueue(ValueTask('a'));
    await probe.queue.start();
    await probe.until(() => probe.of<TaskStarted>().isNotEmpty);
    await pumpEventQueue();
    expect((await storage.inner.get(id))!.handOffReference, isNull);
    release.complete();
    await pumpEventQueue();
    expect((await storage.inner.get(id))!.handOffReference, 'ref');
    await probe.queue.close();
  });
}
