import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';
import 'support/probe_storage.dart';

void main() {
  late QueueHarness h;

  setUp(() => h = QueueHarness());
  tearDown(() => h.queue.close());

  group('QueueMetrics', () {
    test('counts outcomes and times runs and waits per type', () async {
      final gate = Completer<void>();
      var calls = 0;
      h.register(
        handler: (task, context) async {
          calls++;
          if (task.value == 'slow') {
            await gate.future;
          } else if (task.value == 'flaky' && context.attempt == 1) {
            throw TemporaryFailure();
          } else if (task.value == 'bad') {
            throw TemporaryFailure();
          }
        },
      );
      final metrics = QueueMetrics(h.queue);
      addTearDown(metrics.dispose);

      await h.queue.enqueue(ValueTask('slow'));
      await h.queue.enqueue(
        ValueTask('flaky'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 2,
          delay: const Duration(seconds: 1),
        ),
      );
      await h.queue.enqueue(ValueTask('bad'));
      final cancelled = await h.queue.enqueue(
        ValueTask('later'),
        delay: const Duration(hours: 1),
      );

      h.clock.advance(const Duration(seconds: 3)); // waited 3 s before start
      await h.queue.start();
      await h.until(() => calls == 1);
      h.clock.advance(const Duration(seconds: 10)); // slow runs 10 s
      gate.complete();
      await h.until(() => h.of<TaskRetryScheduled>().length == 1);
      await h.until(() => h.of<TaskFailed>().length == 1);
      h.clock.advance(const Duration(seconds: 1));
      await h.until(() => h.of<TaskCompleted>().length == 2);
      await h.queue.cancel(cancelled);
      await pumpEventQueue();

      final snap = metrics.snapshot();
      final value = snap.byType['value']!;
      expect(snap.total.toJson(), value.toJson());
      expect(value.enqueued, 4);
      expect(value.started, 4);
      expect(value.completed, 2);
      expect(value.failed, 1);
      expect(value.retried, 1);
      expect(value.cancelled, 1);
      expect((value.successRate! * 1000).round(), 667);
      expect(value.timedRuns, 4);
      expect(value.maxRunTime, const Duration(seconds: 10));
      expect(value.totalRunTime, const Duration(seconds: 10));
      expect(value.averageRunTime, const Duration(milliseconds: 2500));
      // slow waited 3 s, flaky and bad 13 s each.
      expect(value.timedWaits, 3);
      expect(value.maxWaitTime, const Duration(seconds: 13));
      expect(value.totalWaitTime, const Duration(seconds: 29));
      expect(value.toJson()['averageWaitMs'], 9666);
      expect(snap.toJson()['byType'], {'value': value.toJson()});
    });

    test('a scheduled task waits from its start time', () async {
      h.register(handler: (task, context) async {});
      final metrics = QueueMetrics(h.queue);
      addTearDown(metrics.dispose);
      await h.queue.enqueue(ValueTask('a'), delay: const Duration(hours: 1));
      await h.queue.start();
      h.clock.advance(const Duration(hours: 1));
      await h.until(() => h.of<TaskCompleted>().length == 1);
      expect(metrics.snapshot().total.maxWaitTime, Duration.zero);
    });

    test('snapshots do not change, and reset starts again', () async {
      h.register(handler: (task, context) async {});
      final metrics = QueueMetrics(h.queue);
      addTearDown(metrics.dispose);
      await h.queue.enqueue(ValueTask('a'));
      await pumpEventQueue();
      final before = metrics.snapshot();
      await h.queue.enqueue(ValueTask('b'));
      await pumpEventQueue();
      expect(before.total.enqueued, 1);
      expect(metrics.snapshot().total.enqueued, 2);

      h.clock.advance(const Duration(minutes: 1));
      metrics.reset();
      final after = metrics.snapshot();
      expect(after.total.enqueued, 0);
      expect(after.byType, isEmpty);
      expect(after.since, h.clock.now());
      expect(after.total.averageRunTime, isNull);
      expect(after.total.successRate, isNull);
    });

    test('a gate error deferral ends the attempt', () async {
      final gate = ManualQueueGate();
      h.queue.addGate('network', gate);
      h.register(
        requires: {'network'},
        deferOn: (error, stackTrace) => 'network',
        handler: (task, context) async {
          if (context.attempt == 1 && !gate.isOpen) return;
          gate.close();
          throw TemporaryFailure();
        },
      );
      final metrics = QueueMetrics(h.queue);
      addTearDown(metrics.dispose);
      await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await h.until(() => h.of<TaskDeferred>().isNotEmpty);
      final snap = metrics.snapshot().total;
      expect(snap.deferred, 1);
      expect(snap.timedRuns, 1);
    });
  });

  group('QueueLogger', () {
    test('logs each event at its level', () async {
      h.register(
        handler: (task, context) async {
          if (task.value == 'bad') throw TemporaryFailure();
        },
      );
      final records = <QueueLogRecord>[];
      final logger = QueueLogger(h.queue, onRecord: records.add);
      addTearDown(logger.dispose);

      final ok = await h.queue.enqueue(ValueTask('ok'));
      await h.queue.enqueue(ValueTask('bad'));
      final held = await h.queue.enqueue(
        ValueTask('held'),
        delay: const Duration(hours: 1),
      );
      await h.queue.hold(held);
      await h.queue.start();
      await h.until(() => h.of<TaskFailed>().length == 1);
      await h.queue.cancel(held);
      await pumpEventQueue();

      String? messageFor(QueueLogLevel level, String start) => records
          .where((r) => r.level == level && r.message.startsWith(start))
          .map((r) => r.message)
          .firstOrNull;

      expect(messageFor(QueueLogLevel.debug, 'enqueued'), 'enqueued value $ok');
      expect(
        messageFor(QueueLogLevel.debug, 'enqueued value $held'),
        startsWith('enqueued value $held to run at 2026-01-01T01:00'),
      );
      expect(
        messageFor(QueueLogLevel.debug, 'started'),
        'started value $ok (attempt 1)',
      );
      expect(
        messageFor(QueueLogLevel.info, 'completed'),
        'completed value $ok (attempt 1)',
      );
      expect(
        messageFor(QueueLogLevel.error, 'failed'),
        contains('after attempt 1: TemporaryFailure'),
      );
      expect(
        messageFor(QueueLogLevel.info, 'held'),
        'held value $held (pending)',
      );
      expect(
        messageFor(QueueLogLevel.info, 'cancelled'),
        'cancelled value $held',
      );
      final completed = records.firstWhere(
        (r) => r.message.startsWith('completed'),
      );
      expect(completed.taskId, ok);
      expect(completed.taskType, 'value');
      expect(completed.event, isA<TaskCompleted>());
      expect(completed.toString(), startsWith('[durable_queue] info: '));
    });

    test('minLevel drops routine records', () async {
      h.register(handler: (task, context) async => throw TemporaryFailure());
      final records = <QueueLogRecord>[];
      final logger = QueueLogger(
        h.queue,
        onRecord: records.add,
        minLevel: QueueLogLevel.warning,
      );
      addTearDown(logger.dispose);
      await h.queue.enqueue(
        ValueTask('a'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 2,
          delay: const Duration(seconds: 1),
        ),
      );
      await h.queue.start();
      await h.until(() => h.of<TaskRetryScheduled>().length == 1);
      h.clock.advance(const Duration(seconds: 1));
      await h.until(() => h.of<TaskFailed>().length == 1);
      await pumpEventQueue();
      expect(records.map((r) => r.level), [
        QueueLogLevel.warning,
        QueueLogLevel.error,
      ]);
      expect(
        records.first.message,
        contains('attempt 1 of 2 failed for value'),
      );
    });

    test('logs storage errors as warnings with the error', () async {
      final storage = ProbeStorage();
      var failNext = false;
      storage.before = (method, _) async {
        if (method == 'getNextReady' && failNext) {
          failNext = false;
          throw StateError('disk busy');
        }
      };
      final local = QueueHarness(storage: storage);
      addTearDown(local.queue.close);
      local.register(handler: (task, context) async {});
      final records = <QueueLogRecord>[];
      final logger = QueueLogger(local.queue, onRecord: records.add);
      addTearDown(logger.dispose);
      await local.queue.enqueue(ValueTask('a'));
      failNext = true;
      // start waits for the first pump, which waits out the storage retry.
      final started = local.queue.start();
      await local.until(
        () => records.any((r) => r.message.startsWith('storage')),
      );
      final record = records.firstWhere((r) => r.message.startsWith('storage'));
      expect(record.level, QueueLogLevel.warning);
      expect(record.error, isA<StateError>());
      expect(record.stackTrace, isNotNull);
      local.clock.advance(const Duration(seconds: 1));
      await started;
      await local.until(() => local.of<TaskCompleted>().length == 1);
    });
  });
}
