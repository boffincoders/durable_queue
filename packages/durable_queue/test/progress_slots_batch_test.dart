import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

/// A second task type, to show other types run while one is limited.
final class OtherTask extends DurableTask {
  OtherTask(this.value);

  final String value;

  @override
  String get type => 'other';

  @override
  Map<String, dynamic> toJson() => {'value': value};

  static OtherTask fromJson(Map<String, dynamic> json) =>
      OtherTask(json['value'] as String);
}

void main() {
  group('progress', () {
    late QueueHarness h;
    late List<TaskProgress> reports;
    late Completer<void> finish;
    late TaskContext context;

    setUp(() {
      h = QueueHarness();
      reports = [];
      finish = Completer<void>();
      h.queue.progress.listen(reports.add);
      h.register(
        handler: (task, c) async {
          context = c;
          await finish.future;
          if (task.value == 'fail' && c.attempt == 1) throw TemporaryFailure();
        },
      );
    });

    tearDown(() async {
      if (!finish.isCompleted) finish.complete();
      await h.queue.close();
    });

    test('a handler reports progress; it is cleared when it ends', () async {
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await h.until(() => h.of<TaskStarted>().isNotEmpty);
      expect(h.queue.progressOf(id), isNull);

      context.reportProgress(0.25);
      context.reportProgress(0.25); // unchanged: not repeated
      context.reportProgress(double.nan); // ignored
      context.reportProgress(0.5);
      context.reportProgress(1.7); // clamped
      await pumpEventQueue();
      expect(reports.map((r) => r.fraction), [0.25, 0.5, 1.0]);
      expect(reports.first.taskId, id);
      expect(reports.first.reportedAt, h.clock.now());
      expect(h.queue.progressOf(id)!.fraction, 1.0);

      finish.complete();
      await h.until(() => h.of<TaskCompleted>().length == 1);
      expect(h.queue.progressOf(id), isNull);
    });

    test('a new attempt starts without the old progress', () async {
      final id = await h.queue.enqueue(
        ValueTask('fail'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 2,
          delay: const Duration(seconds: 1),
        ),
      );
      await h.queue.start();
      await h.until(() => h.of<TaskStarted>().isNotEmpty);
      context.reportProgress(0.5);
      finish.complete();
      await h.until(() => h.of<TaskRetryScheduled>().length == 1);
      expect(h.queue.progressOf(id), isNull);

      // A late report from the old attempt is dropped when the next starts.
      h.queue.reportProgress(id, 0.9);
      finish = Completer<void>(); // keep attempt 2 running
      h.clock.advance(const Duration(seconds: 1));
      await h.until(() => h.of<TaskStarted>().length == 2);
      expect(h.queue.progressOf(id), isNull);
    });

    test('outside work reports progress for a handed-off task', () async {
      final local = QueueHarness();
      addTearDown(local.queue.close);
      local.register(handler: (task, c) async => c.handOff('ref'));
      final id = await local.queue.enqueue(ValueTask('upload'));
      await local.queue.start();
      await pumpEventQueue();
      local.queue.reportProgress(id, 0.4);
      expect(local.queue.progressOf(id)!.fraction, 0.4);
      await local.queue.completeHandOff(id, reference: 'ref');
      expect(local.queue.progressOf(id), isNull);
    });

    test('a context not created by a queue ignores progress', () {
      TaskContext(
        taskId: 't',
        attempt: 1,
        maxAttempts: 1,
        idempotencyKey: null,
        deduplicationKey: null,
        isCancellationRequested: () => false,
      ).reportProgress(0.5);
    });
  });

  group('register(maxConcurrent:)', () {
    test('limits one type while others run', () async {
      final h = QueueHarness(maxConcurrentTasks: 4);
      addTearDown(h.queue.close);
      final gates = <String, Completer<void>>{};
      var running = 0;
      var peak = 0;
      Future<void> slow(ValueTask task, TaskContext context) async {
        running++;
        if (running > peak) peak = running;
        await (gates[task.value] = Completer<void>()).future;
        running--;
      }

      h.register(maxConcurrent: 2, handler: slow);
      final others = <String>[];
      h.queue.register<OtherTask>(
        type: 'other',
        decoder: OtherTask.fromJson,
        handler: (task, context) async => others.add(task.value),
      );

      final ids = [
        for (var i = 0; i < 5; i++) await h.queue.enqueue(ValueTask('u$i')),
      ];
      await h.queue.enqueue(OtherTask('o1'));
      await h.queue.start();
      await h.until(() => gates.length == 2 && others.length == 1);
      await pumpEventQueue();
      expect(gates.keys, ['u0', 'u1']);
      expect(peak, 2);
      final waiting = (await h.queue.getTask(ids[2]))!;
      expect(waiting.status, TaskStatus.pending);
      expect(waiting.attempts, 0);

      // Each finished one lets the next start, in order.
      gates['u0']!.complete();
      await h.until(() => gates.length == 3);
      expect(gates.keys.last, 'u2');
      gates['u1']!.complete();
      gates['u2']!.complete();
      await h.until(() => gates.length == 5);
      gates['u3']!.complete();
      gates['u4']!.complete();
      await h.until(() => h.of<TaskCompleted>().length == 6);
      expect(peak, 2);
      expect(h.of<TaskDeferred>(), isEmpty);
    });

    test('a task put aside still starts if the queue forgot it', () async {
      final h = QueueHarness(maxConcurrentTasks: 2);
      addTearDown(h.queue.close);
      final first = Completer<void>();
      h.register(
        maxConcurrent: 1,
        handler: (task, context) async {
          if (task.value == 'a') await first.future;
        },
      );
      await h.queue.enqueue(ValueTask('a'));
      final b = await h.queue.enqueue(ValueTask('b'));
      await h.queue.start();
      await pumpEventQueue();
      final aside = (await h.queue.getTask(b))!;
      expect(
        aside.nextAttemptAt,
        h.clock.now().add(h.queue.gateRecheckInterval),
      );
      first.complete();
      await h.until(() => h.of<TaskCompleted>().length == 2);
    });

    test('handed-off tasks do not take a slot', () async {
      final h = QueueHarness(maxConcurrentTasks: 4);
      addTearDown(h.queue.close);
      h.register(
        maxConcurrent: 1,
        handler: (task, context) async => context.handOff('r-${task.value}'),
      );
      for (var i = 0; i < 3; i++) {
        await h.queue.enqueue(ValueTask('$i'));
      }
      await h.queue.start();
      await h.until(() => h.of<TaskStarted>().length == 3);
    });

    test('must be at least 1', () {
      final h = QueueHarness();
      addTearDown(h.queue.close);
      expect(() => h.register(maxConcurrent: 0), throwsA(isA<ArgumentError>()));
    });
  });

  group('enqueueAll', () {
    test('stores every task with the shared options, in order', () async {
      final h = QueueHarness(maxConcurrentTasks: 3);
      addTearDown(h.queue.close);
      final ran = <String>[];
      h.register(handler: (task, context) async => ran.add(task.value));
      final ids = await h.queue.enqueueAll(
        [ValueTask('a'), ValueTask('b'), ValueTask('c')],
        group: 'sync',
        priority: 3,
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 2,
          delay: const Duration(seconds: 1),
        ),
      );
      expect(ids.toSet(), hasLength(3));
      final stored = await h.queue.getTasks(group: 'sync');
      expect(stored.map((t) => t.id), ids);
      expect(stored.every((t) => t.priority == 3), isTrue);
      expect(stored.every((t) => t.dependsOn.isEmpty), isTrue);
      expect(stored.first.retryPolicy.maxAttempts, 2);
      await pumpEventQueue();
      expect(h.of<TaskEnqueued>(), hasLength(3));
      await h.queue.start();
      await h.until(() => ran.length == 3);
      expect(ran, ['a', 'b', 'c']);
    });

    test('validates everything before storing anything', () async {
      final h = QueueHarness();
      addTearDown(h.queue.close);
      h.register();
      await expectLater(
        h.queue.enqueueAll([ValueTask('ok'), RenamedValueTask()]),
        throwsA(isA<UnknownTaskTypeException>()),
      );
      expect(await h.queue.getTasks(), isEmpty);
      expect(() => h.queue.enqueueAll([]), throwsA(isA<ArgumentError>()));
    });

    test('shares dependencies and a start time', () async {
      final h = QueueHarness();
      addTearDown(h.queue.close);
      h.register();
      final parent = await h.queue.enqueue(ValueTask('p'));
      final ids = await h.queue.enqueueAll(
        [ValueTask('a'), ValueTask('b')],
        dependsOn: [parent],
        delay: const Duration(minutes: 1),
      );
      for (final id in ids) {
        final task = (await h.queue.getTask(id))!;
        expect(task.status, TaskStatus.waiting);
        expect(task.dependsOn, [parent]);
        expect(
          task.nextAttemptAt,
          h.clock.now().add(const Duration(minutes: 1)),
        );
      }
    });
  });
}
