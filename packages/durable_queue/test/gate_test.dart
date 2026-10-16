import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

/// A connection-style failure that `deferOn` attributes to the network.
final class ConnectionLost implements Exception {
  @override
  String toString() => 'ConnectionLost';
}

/// A gate that never reports changes and counts rechecks.
final class SilentGate extends QueueGate {
  SilentGate({this.open = false});

  bool open;
  var rechecks = 0;
  final controller = StreamController<bool>.broadcast();

  @override
  bool get isOpen => open;

  @override
  Stream<bool> get changes => controller.stream;

  @override
  void recheck() => rechecks++;
}

/// A second task type for tests that mix gated and ungated work.
final class LocalTask extends DurableTask {
  LocalTask(this.value);

  final String value;

  @override
  String get type => 'local';

  @override
  Map<String, dynamic> toJson() => {'value': value};

  static LocalTask fromJson(Map<String, dynamic> json) =>
      LocalTask(json['value'] as String);
}

String? networkOnConnectionLost(Object error, StackTrace stackTrace) =>
    error is ConnectionLost ? 'network' : null;

void main() {
  group('setup', () {
    test('register checks gates and deferOn', () {
      final h = QueueHarness();
      expect(() => h.register(requires: {'network'}), throwsArgumentError);
      expect(
        () => h.register(deferOn: networkOnConnectionLost),
        throwsArgumentError,
      );
      h.queue.addGate('network', ManualQueueGate());
      expect(
        () => h.queue.addGate('network', ManualQueueGate()),
        throwsStateError,
      );
      expect(() => h.queue.addGate('', ManualQueueGate()), throwsArgumentError);
      h.register(requires: {'network'}, deferOn: networkOnConnectionLost);
    });

    test('ManualQueueGate reports only real changes', () async {
      final gate = ManualQueueGate(open: false);
      final seen = <bool>[];
      gate.changes.listen(seen.add);
      gate.close();
      gate.open();
      gate.open();
      gate.set(false);
      expect(seen, [true, false]);
      expect(gate.isOpen, isFalse);
      await gate.dispose();
      gate.open();
      expect(gate.isOpen, isTrue);
    });
  });

  group('closed gate', () {
    late QueueHarness h;
    late ManualQueueGate network;
    late List<String> started;

    setUp(() {
      h = QueueHarness(maxConcurrentTasks: 2);
      network = ManualQueueGate(open: false);
      started = [];
      h.queue.addGate('network', network);
      h.register(
        requires: {'network'},
        handler: (task, context) async => started.add(task.value),
      );
      h.queue.register<LocalTask>(
        type: 'local',
        decoder: LocalTask.fromJson,
        handler: (task, context) async => started.add(task.value),
      );
    });

    tearDown(() => h.queue.close());

    test('defers gated work, runs other work, releases on open', () async {
      final start = h.clock.now();
      final id = await h.queue.enqueue(ValueTask('upload'), priority: 5);
      await h.queue.enqueue(LocalTask('log'));
      await h.queue.start();
      await h.until(() => started.contains('log'));
      expect(started, ['log']);

      final deferred = h.of<TaskDeferred>().single;
      expect(deferred.taskId, id);
      expect(deferred.reason, DeferralReason.gateClosed);
      expect(deferred.name, 'network');
      expect(deferred.until, start.add(const Duration(minutes: 5)));
      final stored = (await h.queue.getTask(id))!;
      expect(stored.status, TaskStatus.pending);
      expect(stored.attempts, 0);

      network.open();
      await h.until(() => started.length == 2);
      expect(started.last, 'upload');
      expect(h.of<TaskStarted>().last.attempt, 1);
      // Released without the clock moving.
      expect(h.clock.now(), start);
    });

    test('a due retry waiting on the gate keeps its status', () async {
      var calls = 0;
      final retrying = QueueHarness();
      final gate = ManualQueueGate();
      retrying.queue.addGate('network', gate);
      retrying.register(
        requires: {'network'},
        handler: (task, context) async {
          if (++calls == 1) {
            gate.close();
            throw TemporaryFailure();
          }
        },
      );
      final id = await retrying.queue.enqueue(
        ValueTask('a'),
        retryPolicy: RetryPolicy.fixed(maxAttempts: 2, delay: Duration.zero),
      );
      await retrying.queue.start();
      await retrying.until(() => retrying.of<TaskDeferred>().isNotEmpty);
      final stored = (await retrying.queue.getTask(id))!;
      expect(stored.status, TaskStatus.retryScheduled);
      expect(stored.attempts, 1);

      gate.open();
      await retrying.until(() => retrying.of<TaskCompleted>().isNotEmpty);
      expect(retrying.of<TaskCompleted>().single.attempt, 2);
      await retrying.queue.close();
    });

    test('a gate that never reports opening is checked again later', () async {
      final silent = SilentGate();
      final q = QueueHarness();
      q.queue.addGate('vpn', silent);
      q.register(requires: {'vpn'});
      await q.queue.enqueue(ValueTask('a'));
      await q.queue.start();
      await q.until(() => q.of<TaskDeferred>().length == 1);

      // Still closed at the recheck: deferred again.
      q.clock.advance(const Duration(minutes: 5));
      await q.until(() => q.of<TaskDeferred>().length == 2);

      silent.open = true;
      q.clock.advance(const Duration(minutes: 5));
      await q.until(() => q.of<TaskCompleted>().isNotEmpty);
      await q.queue.close();
    });

    test('closing a gate does not cancel a running task', () async {
      final release = Completer<void>();
      final q = QueueHarness();
      final gate = ManualQueueGate();
      q.queue.addGate('network', gate);
      var cancelled = false;
      q.register(
        requires: {'network'},
        handler: (task, context) async {
          await release.future;
          cancelled = context.isCancellationRequested;
        },
      );
      await q.queue.enqueue(ValueTask('a'));
      await q.queue.start();
      await q.until(() => q.of<TaskStarted>().isNotEmpty);
      gate.close();
      release.complete();
      await q.until(() => q.of<TaskCompleted>().isNotEmpty);
      expect(cancelled, isFalse);
      await q.queue.close();
    });

    test(
      'opening while paused releases tasks but starts them on resume',
      () async {
        await h.queue.enqueue(ValueTask('upload'));
        await h.queue.start();
        await h.until(() => h.of<TaskDeferred>().isNotEmpty);
        await h.queue.pause();
        network.open();
        await pumpEventQueue();
        expect(started, isEmpty);
        final stored = (await h.queue.getTasks()).single;
        expect(stored.nextAttemptAt, isNull);
        await h.queue.resume();
        await h.until(() => started.isNotEmpty);
      },
    );
  });

  group('deferOn', () {
    test('a connection error defers without using an attempt', () async {
      final h = QueueHarness();
      final gate = SilentGate(open: true);
      h.queue.addGate('network', gate);
      var calls = 0;
      final attempts = <int>[];
      h.register(
        requires: {'network'},
        deferOn: networkOnConnectionLost,
        handler: (task, context) async {
          attempts.add(context.attempt);
          if (++calls == 1) throw ConnectionLost();
        },
      );
      // With no retries at all, the task still survives the connection error.
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await h.until(() => h.of<TaskDeferred>().isNotEmpty);

      final deferred = h.of<TaskDeferred>().single;
      expect(deferred.reason, DeferralReason.gateError);
      expect(deferred.name, 'network');
      expect(deferred.error, 'ConnectionLost');
      expect(deferred.until, h.clock.now().add(const Duration(seconds: 30)));
      expect(gate.rechecks, 1);
      final stored = (await h.queue.getTask(id))!;
      expect(stored.status, TaskStatus.pending);
      expect(stored.attempts, 0);
      expect(stored.lastFailure?.error, 'ConnectionLost');
      expect(h.of<TaskFailed>(), isEmpty);
      expect(h.of<TaskRetryScheduled>(), isEmpty);

      h.clock.advance(const Duration(seconds: 30));
      await h.until(() => h.of<TaskCompleted>().isNotEmpty);
      expect(attempts, [1, 1]);
      await h.queue.close();
    });

    test('after earlier attempts the task stays retry-scheduled', () async {
      final h = QueueHarness();
      h.queue.addGate('network', ManualQueueGate());
      var calls = 0;
      h.register(
        requires: {'network'},
        deferOn: networkOnConnectionLost,
        handler: (task, context) async {
          calls++;
          if (calls == 1) throw TemporaryFailure();
          if (calls == 2) throw ConnectionLost();
        },
      );
      final id = await h.queue.enqueue(
        ValueTask('a'),
        retryPolicy: RetryPolicy.fixed(maxAttempts: 2, delay: Duration.zero),
      );
      await h.queue.start();
      await h.until(() => h.of<TaskDeferred>().isNotEmpty);
      final stored = (await h.queue.getTask(id))!;
      expect(stored.status, TaskStatus.retryScheduled);
      expect(stored.attempts, 1);

      h.clock.advance(const Duration(seconds: 30));
      await h.until(() => h.of<TaskCompleted>().isNotEmpty);
      expect(h.of<TaskCompleted>().single.attempt, 2);
      await h.queue.close();
    });

    test('closing and reopening the gate releases the task early', () async {
      final h = QueueHarness();
      final gate = ManualQueueGate();
      h.queue.addGate('network', gate);
      var calls = 0;
      h.register(
        requires: {'network'},
        deferOn: networkOnConnectionLost,
        handler: (task, context) async {
          if (++calls == 1) throw ConnectionLost();
        },
      );
      await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await h.until(() => h.of<TaskDeferred>().isNotEmpty);
      gate.close();
      gate.open();
      await h.until(() => h.of<TaskCompleted>().isNotEmpty);
      await h.queue.close();
    });

    test(
      'other errors, unknown gates, and throwing rules fail normally',
      () async {
        final h = QueueHarness();
        h.queue.addGate('network', ManualQueueGate());
        h.queue.addGate('battery', ManualQueueGate());
        final errors = <Object>[
          TemporaryFailure(),
          StateError('wrong gate'),
          ArgumentError('rule throws'),
        ];
        h.register(
          requires: {'network'},
          deferOn: (error, stackTrace) {
            if (error is StateError) return 'battery';
            if (error is ArgumentError) throw error;
            return null;
          },
          handler: (task, context) async => throw errors[int.parse(task.value)],
        );
        for (var i = 0; i < errors.length; i++) {
          await h.queue.enqueue(ValueTask('$i'));
        }
        await h.queue.start();
        await h.until(() => h.of<TaskFailed>().length == 3);
        expect(h.of<TaskDeferred>(), isEmpty);
        await h.queue.close();
      },
    );
  });

  test('close stops listening to gates', () async {
    final h = QueueHarness();
    var listening = false;
    final controller = StreamController<bool>.broadcast(
      onListen: () => listening = true,
      onCancel: () => listening = false,
    );
    final gate = _StreamGate(controller.stream);
    h.queue.addGate('network', gate);
    expect(listening, isTrue);
    await h.queue.close();
    expect(listening, isFalse);
    expect(() => h.queue.addGate('other', gate), throwsStateError);
  });
}

final class _StreamGate extends QueueGate {
  _StreamGate(this.changes);

  @override
  bool get isOpen => true;

  @override
  final Stream<bool> changes;
}
