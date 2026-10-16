import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

/// A task whose type is chosen per instance, so one queue can hold several
/// registered types.
final class TypedTask extends DurableTask {
  TypedTask(this.type, this.value, {this.host});

  @override
  final String type;
  final String value;
  final String? host;

  @override
  Map<String, dynamic> toJson() => {
    'type': type,
    'value': value,
    'host': ?host,
  };

  static TaskDecoder<TypedTask> decoderFor(String type) {
    return (json) =>
        TypedTask(type, json['value'] as String, host: json['host'] as String?);
  }
}

void main() {
  group('RateLimit', () {
    test('rejects invalid values', () {
      expect(
        () => RateLimit(0, per: const Duration(seconds: 1)),
        throwsArgumentError,
      );
      expect(
        () => RateLimit(5, per: const Duration(microseconds: 4)),
        throwsArgumentError,
      );
      expect(
        RateLimit(4, per: const Duration(minutes: 1)).interval,
        const Duration(seconds: 15),
      );
    });
  });

  group('register(rateLimit:)', () {
    late QueueHarness h;
    late List<String> started;

    setUp(() {
      h = QueueHarness(maxConcurrentTasks: 5);
      started = [];
      h.register(
        rateLimit: RateLimit(2, per: const Duration(minutes: 1)),
        handler: (task, context) async => started.add(task.value),
      );
    });

    test(
      'allows a burst, then spaces starts and defers each task once',
      () async {
        final start = h.clock.now();
        for (final value in ['a', 'b', 'c', 'd', 'e']) {
          await h.queue.enqueue(ValueTask(value));
        }
        await h.queue.start();
        await h.until(() => h.of<TaskDeferred>().length == 3);
        expect(started, ['a', 'b']);

        final deferred = h.of<TaskDeferred>();
        expect(deferred.map((e) => e.reason).toSet(), {
          DeferralReason.rateLimit,
        });
        expect(deferred.map((e) => e.name).toSet(), {'type:value'});
        expect(deferred.map((e) => e.until), [
          start.add(const Duration(seconds: 30)),
          start.add(const Duration(seconds: 60)),
          start.add(const Duration(seconds: 90)),
        ]);
        // Deferral keeps status and attempts.
        final c = (await h.queue.getTasks(status: TaskStatus.pending))
            .firstWhere((task) => task.payload['value'] == 'c');
        expect(c.attempts, 0);
        expect(c.nextAttemptAt, start.add(const Duration(seconds: 30)));

        h.clock.advance(const Duration(seconds: 29));
        await pumpEventQueue();
        expect(started, ['a', 'b']);
        h.clock.advance(const Duration(seconds: 1));
        await h.until(() => started.length == 3);
        h.clock.advance(const Duration(seconds: 60));
        await h.until(() => started.length == 5);
        expect(started, ['a', 'b', 'c', 'd', 'e']);
        // Every task was deferred at most once.
        expect(h.of<TaskDeferred>(), hasLength(3));
        await h.queue.close();
      },
    );

    test('a retry counts as a start', () async {
      final limited = QueueHarness();
      var calls = 0;
      limited.register(
        rateLimit: RateLimit(1, per: const Duration(seconds: 10)),
        handler: (task, context) async {
          if (++calls == 1) throw TemporaryFailure();
        },
      );
      await limited.queue.enqueue(
        ValueTask('a'),
        retryPolicy: RetryPolicy.fixed(maxAttempts: 2, delay: Duration.zero),
      );
      await limited.queue.start();
      await limited.until(() => limited.of<TaskDeferred>().isNotEmpty);
      expect(calls, 1);
      expect(
        limited.of<TaskDeferred>().single.until,
        limited.clock.now().add(const Duration(seconds: 10)),
      );
      final stored = (await limited.queue.getTasks()).single;
      expect(stored.status, TaskStatus.retryScheduled);
      expect(stored.attempts, 1);

      limited.clock.advance(const Duration(seconds: 10));
      await limited.until(() => limited.of<TaskCompleted>().isNotEmpty);
      expect(limited.of<TaskCompleted>().single.attempt, 2);
      await limited.queue.close();
    });
  });

  group('limits do not block other work', () {
    test('an unlimited type runs while a limited type waits', () async {
      final h = QueueHarness();
      final started = <String>[];
      h.queue.register<TypedTask>(
        type: 'upload',
        decoder: TypedTask.decoderFor('upload'),
        handler: (task, context) async => started.add(task.value),
        rateLimit: RateLimit(1, per: const Duration(minutes: 1)),
      );
      h.queue.register<TypedTask>(
        type: 'log',
        decoder: TypedTask.decoderFor('log'),
        handler: (task, context) async => started.add(task.value),
      );
      await h.queue.enqueue(TypedTask('upload', 'u1'), priority: 1);
      await h.queue.enqueue(TypedTask('upload', 'u2'), priority: 1);
      await h.queue.enqueue(TypedTask('upload', 'u3'), priority: 1);
      await h.queue.enqueue(TypedTask('log', 'l1'));
      await h.queue.start();
      await h.until(() => started.length == 2);
      expect(started, ['u1', 'l1']);
      h.clock.advance(const Duration(minutes: 2));
      await h.until(() => started.length == 4);
      expect(started, ['u1', 'l1', 'u2', 'u3']);
      await h.queue.close();
    });
  });

  group('addRateLimit', () {
    late QueueHarness h;
    late List<String> started;

    setUp(() {
      h = QueueHarness(maxConcurrentTasks: 10);
      started = [];
      for (final type in ['get', 'post', 'local']) {
        h.queue.register<TypedTask>(
          type: type,
          decoder: TypedTask.decoderFor(type),
          handler: (task, context) async => started.add(task.value),
        );
      }
    });

    test(
      'keyOf gives each key its own bucket; null keys are unlimited',
      () async {
        h.queue.addRateLimit(
          'api',
          RateLimit(1, per: const Duration(seconds: 10)),
          keyOf: (task) => task.payload['host'] as String?,
        );
        await h.queue.enqueue(TypedTask('get', 'a1', host: 'a.example'));
        await h.queue.enqueue(TypedTask('post', 'a2', host: 'a.example'));
        await h.queue.enqueue(TypedTask('get', 'b1', host: 'b.example'));
        await h.queue.enqueue(TypedTask('get', 'n1'));
        await h.queue.enqueue(TypedTask('get', 'n2'));
        await h.queue.start();
        await h.until(() => started.length == 4);
        expect(started, ['a1', 'b1', 'n1', 'n2']);
        final deferred = h.of<TaskDeferred>().single;
        expect(deferred.name, 'api');
        h.clock.advance(const Duration(seconds: 10));
        await h.until(() => started.length == 5);
        expect(started.last, 'a2');
        await h.queue.close();
      },
    );

    test('types restricts the limit; the strictest limit wins', () async {
      h.queue.addRateLimit(
        'writes',
        RateLimit(1, per: const Duration(minutes: 1)),
        types: {'post'},
      );
      h.queue.addRateLimit(
        'all',
        RateLimit(3, per: const Duration(seconds: 3)),
      );
      for (var i = 0; i < 2; i++) {
        await h.queue.enqueue(TypedTask('post', 'p$i'));
      }
      for (var i = 0; i < 2; i++) {
        await h.queue.enqueue(TypedTask('local', 'l$i'));
      }
      await h.queue.start();
      await h.until(() => started.length == 3);
      expect(started, ['p0', 'l0', 'l1']);
      final deferred = h.of<TaskDeferred>().single;
      expect(deferred.name, 'writes');
      expect(deferred.until, h.clock.now().add(const Duration(minutes: 1)));
      h.clock.advance(const Duration(minutes: 1));
      await h.until(() => started.length == 4);
      await h.queue.close();
    });

    test('a keyOf that throws leaves the task unlimited', () async {
      h.queue.addRateLimit(
        'broken',
        RateLimit(1, per: const Duration(hours: 1)),
        keyOf: (task) => throw StateError('bad key'),
      );
      for (var i = 0; i < 3; i++) {
        await h.queue.enqueue(TypedTask('get', 'g$i'));
      }
      await h.queue.start();
      await h.until(() => started.length == 3);
      expect(h.of<TaskDeferred>(), isEmpty);
      await h.queue.close();
    });

    test('validates its arguments', () {
      final limit = RateLimit(1, per: const Duration(seconds: 1));
      h.queue.addRateLimit('a', limit);
      expect(() => h.queue.addRateLimit('a', limit), throwsStateError);
      expect(() => h.queue.addRateLimit('', limit), throwsArgumentError);
      expect(
        () => h.queue.addRateLimit('b', limit, types: {}),
        throwsArgumentError,
      );
    });
  });

  test('deferred tasks keep their time across a restart', () async {
    final storage = MemoryQueueStorage();
    final first = QueueHarness(storage: storage);
    first.register(rateLimit: RateLimit(1, per: const Duration(minutes: 10)));
    await first.queue.enqueue(ValueTask('a'));
    await first.queue.enqueue(ValueTask('b'));
    await first.queue.start();
    await first.until(() => first.of<TaskDeferred>().isNotEmpty);
    final until = first.of<TaskDeferred>().single.until;
    await first.queue.close();

    final second = QueueHarness(storage: storage, clock: first.clock);
    second.register(rateLimit: RateLimit(1, per: const Duration(minutes: 10)));
    await second.queue.start();
    await pumpEventQueue();
    expect(second.of<TaskStarted>(), isEmpty);
    expect((await storage.getNextWakeAt()), until);
    // Limits restart empty, so the task starts as soon as it is due.
    second.clock.advance(const Duration(minutes: 10));
    await second.until(() => second.of<TaskCompleted>().isNotEmpty);
    expect(second.of<TaskDeferred>(), isEmpty);
    await second.queue.close();
  });
}
