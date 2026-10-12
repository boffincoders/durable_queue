import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';
import 'support/probe_storage.dart';

void main() {
  for (final resume in [false, true]) {
    test(
      'retry crossing its deadline during ${resume ? 'resume' : 'start'} wakes',
      () async {
        final storage = ProbeStorage();
        final h = QueueHarness(storage: storage);
        h.register();
        await storage.save(
          storedTask(
            id: 'delayed',
            status: TaskStatus.retryScheduled,
            attempts: 1,
            retryPolicy: RetryPolicy.fixed(
              maxAttempts: 3,
              delay: const Duration(seconds: 1),
            ),
            nextAttemptAt: h.clock.now().add(const Duration(seconds: 1)),
          ),
        );
        if (resume) {
          await h.queue.start();
          await h.queue.pause();
        }
        storage.after = (method, result) async {
          if (method == 'getNextReady') {
            storage.after = null;
            h.clock.advance(const Duration(seconds: 2));
          }
        };
        await (resume ? h.queue.resume() : h.queue.start());
        await h.until(() => h.of<TaskCompleted>().isNotEmpty);
        expect((await h.queue.getTask('delayed'))!.attempts, 2);
        await h.queue.stop();
      },
    );
  }

  test('nested payload snapshots and attempts are isolated', () async {
    final h = QueueHarness();
    final seen = <List<dynamic>>[];
    h.queue.register<NestedTask>(
      type: 'nested',
      decoder: NestedTask.fromJson,
      handler: (task, context) async {
        final items = task.data['items'] as List;
        seen.add(List.of(items));
        items.clear();
        if (context.attempt == 1) throw TemporaryFailure();
      },
    );
    final original = {
      'items': <dynamic>['original'],
    };
    final id = await h.queue.enqueue(
      NestedTask(original),
      retryPolicy: RetryPolicy.fixed(
        maxAttempts: 2,
        delay: const Duration(seconds: 1),
      ),
    );
    (original['items'] as List).clear();
    final record = (await h.queue.getTask(id))!;
    final data = record.payload['data'] as Map;
    expect(() => (data['items'] as List).clear(), throwsUnsupportedError);
    expect(() => data['new'] = true, throwsUnsupportedError);
    expect(
      () => ((record.toJson()['payload'] as Map)['data'] as Map).clear(),
      throwsUnsupportedError,
    );
    await h.queue.start();
    await h.until(() => h.of<TaskRetryScheduled>().isNotEmpty);
    h.clock.advance(const Duration(seconds: 1));
    await h.until(() => h.of<TaskCompleted>().isNotEmpty);
    expect(seen, [
      ['original'],
      ['original'],
    ]);
    expect((await h.queue.getTask(id))!.payload['data'], {
      'items': ['original'],
    });
    await h.queue.stop();
  });

  test(
    'failed recovery stays idle and can be retried after partial progress',
    () async {
      final storage = ProbeStorage();
      final h = QueueHarness(storage: storage);
      h.register();
      for (final id in ['a', 'b']) {
        await storage.save(
          storedTask(id: id, status: TaskStatus.running, attempts: 1),
        );
      }
      storage.before = (method, argument) async {
        if (method == 'update' && (argument as StoredTask).id == 'b') {
          throw StateError('offline');
        }
      };
      await expectLater(h.queue.start(), throwsStateError);
      expect(h.queue.runState, QueueRunState.idle);
      await h.until(() => h.of<TaskFailed>().length == 1);
      storage.before = null;
      await h.queue.start();
      await h.until(() => h.of<TaskFailed>().length == 2);
      expect(h.of<TaskFailed>().map((e) => e.taskId), ['a', 'b']);
      await h.queue.stop();
    },
  );

  test('failed initial recovery read permits another start', () async {
    final storage = ProbeStorage();
    final h = QueueHarness(storage: storage);
    h.register();
    storage.before = (method, argument) async {
      if (method == 'getByStatus') throw StateError('offline');
    };
    await expectLater(h.queue.start(), throwsStateError);
    expect(h.queue.runState, QueueRunState.idle);
    storage.before = null;
    await h.queue.enqueue(ValueTask('work'));
    await h.queue.start();
    await h.until(() => h.of<TaskCompleted>().isNotEmpty);
    await h.queue.stop();
  });

  for (final committed in [false, true]) {
    test(
      'completion write retries ${committed ? 'after' : 'before'} commit without rerunning handler',
      () async {
        final storage = ProbeStorage();
        final h = QueueHarness(storage: storage);
        final errors = <QueueStorageFailure>[];
        h.queue.storageErrors.listen(errors.add);
        var calls = 0;
        h.register(
          handler: (_, _) async {
            calls++;
          },
        );
        var faults = 2;
        Future<void> fail() async {
          if (faults-- > 0) throw StateError('offline');
        }

        if (committed) {
          storage.after = (method, _) async {
            if (method == 'update' &&
                (await storage.inner.get('task'))?.status ==
                    TaskStatus.completed) {
              await fail();
            }
          };
        } else {
          storage.before = (method, argument) async {
            if (method == 'update' &&
                (argument as StoredTask).status == TaskStatus.completed) {
              await fail();
            }
          };
        }
        await storage.save(storedTask(id: 'task'));
        await h.queue.start();
        await h.until(() => errors.length == 1);
        expect(h.of<TaskCompleted>(), isEmpty);
        expect(calls, 1);
        var stopped = false;
        final stopping = h.queue.stop().then((_) => stopped = true);
        await Future<void>.delayed(Duration.zero);
        expect(stopped, isFalse);
        expect(errors, hasLength(1)); // No polling before the retry delay.
        h.clock.advance(const Duration(seconds: 1));
        await h.until(() => errors.length == 2);
        h.clock.advance(const Duration(seconds: 1));
        await stopping;
        await h.until(() => h.of<TaskCompleted>().length == 1);
        expect(calls, 1);
        expect(errors.map((e) => e.attempt), [1, 2]);
        expect((await h.queue.getTask('task'))!.status, TaskStatus.completed);
        expect((await h.queue.getTask('task'))!.attempts, 1);
      },
    );
  }

  test('claim write with uncertain commit retries the same attempt', () async {
    final storage = ProbeStorage();
    final h = QueueHarness(storage: storage);
    final errors = <QueueStorageFailure>[];
    h.queue.storageErrors.listen(errors.add);
    h.register();
    await h.queue.enqueue(ValueTask('work'));
    storage.after = (method, _) async {
      if (method == 'update') {
        storage.after = null;
        throw StateError('commit acknowledgement lost');
      }
    };
    final starting = h.queue.start();
    await h.until(() => errors.isNotEmpty);
    expect(h.of<TaskStarted>(), isEmpty);
    h.clock.advance(const Duration(seconds: 1));
    await starting;
    await h.until(() => h.of<TaskCompleted>().isNotEmpty);
    expect(h.of<TaskStarted>().single.attempt, 1);
    await h.queue.stop();
  });

  test(
    'failed retry write retains outcome and eventually executes retry',
    () async {
      final storage = ProbeStorage();
      final h = QueueHarness(storage: storage);
      final errors = <QueueStorageFailure>[];
      h.queue.storageErrors.listen(errors.add);
      h.register(
        handler: (_, context) async {
          if (context.attempt == 1) throw TemporaryFailure();
        },
      );
      storage.before = (method, argument) async {
        if (method == 'update' &&
            (argument as StoredTask).status == TaskStatus.retryScheduled) {
          storage.before = null;
          throw StateError('offline');
        }
      };
      await h.queue.enqueue(
        ValueTask('work'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 2,
          delay: const Duration(seconds: 1),
        ),
      );
      await h.queue.start();
      await h.until(() => errors.isNotEmpty);
      h.clock.advance(const Duration(seconds: 1));
      await h.until(() => h.of<TaskCompleted>().isNotEmpty);
      expect(h.of<TaskRetryScheduled>(), hasLength(1));
      expect(h.of<TaskStarted>().map((e) => e.attempt), [1, 2]);
      await h.queue.stop();
    },
  );

  for (final method in ['getNextReady', 'get', 'getNextWakeAt']) {
    test('worker $method failures retry without uncaught errors', () async {
      final storage = ProbeStorage();
      final h = QueueHarness(storage: storage);
      final errors = <QueueStorageFailure>[];
      h.queue.storageErrors.listen(errors.add);
      h.register();
      if (method != 'getNextWakeAt') await h.queue.enqueue(ValueTask('work'));
      storage.before = (name, argument) async {
        if (name == method) {
          storage.before = null;
          throw StateError('offline');
        }
      };
      final starting = h.queue.start();
      await h.until(() => errors.isNotEmpty);
      expect(errors.single.operation, method);
      h.clock.advance(const Duration(seconds: 1));
      await starting;
      if (method == 'getNextWakeAt') await h.queue.enqueue(ValueTask('work'));
      await h.until(() => h.of<TaskCompleted>().isNotEmpty);
      expect(h.of<TaskStarted>(), hasLength(1));
      await h.queue.stop();
    });
  }

  for (final cancelled in [false, true]) {
    test(
      '${cancelled ? 'cancelled' : 'failed'} outcomes survive storage errors',
      () async {
        final storage = ProbeStorage();
        final h = QueueHarness(storage: storage);
        final errors = <QueueStorageFailure>[];
        h.queue.storageErrors.listen(errors.add);
        h.register(
          handler: (_, _) async {
            if (cancelled) throw const TaskCancelledException();
            throw TemporaryFailure();
          },
        );
        final status = cancelled ? TaskStatus.cancelled : TaskStatus.failed;
        storage.before = (name, argument) async {
          if (name == 'update' && (argument as StoredTask).status == status) {
            storage.before = null;
            throw StateError('offline');
          }
        };
        final id = await h.queue.enqueue(ValueTask('work'));
        await h.queue.start();
        await h.until(() => errors.isNotEmpty);
        h.clock.advance(const Duration(seconds: 1));
        await h.until(
          () => cancelled
              ? h.of<TaskCancelled>().isNotEmpty
              : h.of<TaskFailed>().isNotEmpty,
        );
        expect((await h.queue.getTask(id))!.status, status);
        expect(h.of<TaskStarted>(), hasLength(1));
        await h.queue.stop();
      },
    );
  }

  test('storage retry delay must be positive', () {
    expect(
      () => DurableQueue(
        storage: MemoryQueueStorage(),
        storageRetryDelay: Duration.zero,
      ),
      throwsArgumentError,
    );
  });
}

final class NestedTask extends DurableTask {
  NestedTask(this.data);
  final Map<String, dynamic> data;
  @override
  String get type => 'nested';
  @override
  Map<String, dynamic> toJson() => {'data': data};
  static NestedTask fromJson(Map<String, dynamic> json) =>
      NestedTask(json['data'] as Map<String, dynamic>);
}
