import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue/testing.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

/// Version 2 of the value task: the payload key was renamed.
final class RenamedField extends DurableTask {
  RenamedField(this.text);

  final String text;

  @override
  String get type => 'value';

  @override
  Map<String, dynamic> toJson() => {'text': text};

  static RenamedField fromJson(Map<String, dynamic> json) =>
      RenamedField(json['text'] as String);
}

void main() {
  late MemoryQueueStorage storage;
  late FakeQueueClock clock;

  setUp(() {
    storage = MemoryQueueStorage();
    clock = FakeQueueClock(DateTime.utc(2026, 1, 1));
  });

  /// App version 1 stores a task and closes.
  Future<String> storeWithVersion1() async {
    final v1 = QueueHarness(storage: storage, clock: clock)..register();
    final id = await v1.queue.enqueue(ValueTask('hello'));
    expect((await storage.get(id))!.payloadVersion, 1);
    await v1.queue.close();
    return id;
  }

  /// App version 2: renamed `value` to `text`.
  (DurableQueue, List<String>, List<QueueEvent>) openVersion2({
    TaskMigration? migrate,
    int version = 2,
  }) {
    final ran = <String>[];
    final events = <QueueEvent>[];
    final queue = DurableQueue(storage: storage, clock: clock)
      ..register<RenamedField>(
        type: 'value',
        version: version,
        migrate: migrate,
        decoder: RenamedField.fromJson,
        handler: (task, context) async => ran.add(task.text),
      );
    queue.events.listen(events.add);
    addTearDown(queue.close);
    return (queue, ran, events);
  }

  test('an older task is migrated before it runs', () async {
    final id = await storeWithVersion1();
    final seen = <(int, Map<String, dynamic>)>[];
    final (queue, ran, _) = openVersion2(
      migrate: (from, json) {
        seen.add((from, json));
        return switch (from) {
          1 => {'text': json['value']},
          _ => json,
        };
      },
    );
    await queue.start();
    await queue.waitFor(id);
    expect(ran, ['hello']);
    expect(seen.single.$1, 1);
    expect(seen.single.$2, {'value': 'hello'});
    // The stored payload keeps its original shape and version.
    final stored = (await queue.getTask(id))!;
    expect(stored.payload, {'value': 'hello'});
    expect(stored.payloadVersion, 1);
  });

  test('new tasks are stored with the registered version', () async {
    final (queue, _, _) = openVersion2(migrate: (from, json) => json);
    final id = await queue.enqueue(RenamedField('x'));
    expect((await queue.getTask(id))!.payloadVersion, 2);
    final chain = await queue.enqueueChain([RenamedField('a')]);
    expect((await queue.getTask(chain.single))!.payloadVersion, 2);
  });

  test('current tasks are not migrated', () async {
    var calls = 0;
    final (queue, ran, _) = openVersion2(
      migrate: (from, json) {
        calls++;
        return json;
      },
    );
    final id = await queue.enqueue(RenamedField('now'));
    await queue.start();
    await queue.waitFor(id);
    expect(ran, ['now']);
    expect(calls, 0);
  });

  test('a migration that throws fails the task at once', () async {
    final id = await storeWithVersion1();
    final (queue, ran, _) = openVersion2(
      migrate: (from, json) => throw StateError('no rule for $from'),
    );
    await queue.start();
    final done = await queue.waitFor(id);
    expect(ran, isEmpty);
    expect(done.status, TaskStatus.failed);
    expect(done.lastFailure!.error, contains('TaskDecodeException'));
    expect(done.lastFailure!.error, contains('from version 1 to 2'));
    expect(done.lastFailure!.error, contains('no rule for 1'));
  });

  test('without migrate, an older payload goes to the decoder as is', () async {
    final id = await storeWithVersion1();
    final (queue, ran, _) = openVersion2();
    await queue.start();
    final done = await queue.waitFor(id);
    // The version 2 decoder cannot read the old key.
    expect(done.status, TaskStatus.failed);
    expect(ran, isEmpty);
  });

  test('a task from a newer app waits instead of running', () async {
    final newer = DurableQueue(storage: storage, clock: clock)
      ..register<RenamedField>(
        type: 'value',
        version: 3,
        decoder: RenamedField.fromJson,
        handler: (task, context) async {},
      );
    final id = await newer.enqueue(RenamedField('from the future'));
    await newer.close();

    // The user went back to an app that knows version 2.
    final (queue, ran, events) = openVersion2(migrate: (from, json) => json);
    await queue.start();
    await pumpEventQueue();
    var stored = (await queue.getTask(id))!;
    expect(ran, isEmpty);
    expect(events.whereType<TaskStarted>(), isEmpty);
    expect(stored.status, TaskStatus.pending);
    expect(stored.attempts, 0);
    expect(stored.nextAttemptAt, clock.now().add(const Duration(hours: 1)));
    expect(stored.lastFailure!.error, contains('TaskVersionException'));
    expect(stored.lastFailure!.error, contains('payload version 3'));

    // Still too old an hour later.
    clock.advance(const Duration(hours: 1));
    await pumpEventQueue();
    stored = (await queue.getTask(id))!;
    expect(ran, isEmpty);
    expect(stored.attempts, 0);
    await queue.close();

    // An updated app runs it.
    final updated = DurableQueue(storage: storage, clock: clock)
      ..register<RenamedField>(
        type: 'value',
        version: 3,
        decoder: RenamedField.fromJson,
        handler: (task, context) async => ran.add(task.text),
      );
    addTearDown(updated.close);
    clock.advance(const Duration(hours: 1));
    await updated.start();
    await updated.waitFor(id);
    expect(ran, ['from the future']);
  });

  test('version must be at least 1', () {
    final queue = DurableQueue(storage: storage, clock: clock);
    addTearDown(queue.close);
    expect(
      () => queue.register<RenamedField>(
        type: 'value',
        version: 0,
        decoder: RenamedField.fromJson,
        handler: (task, context) async {},
      ),
      throwsA(isA<ArgumentError>()),
    );
  });

  test('records written before 0.5.11 load as version 1', () {
    final json = storedTask(id: 'a').toJson()
      ..remove('payloadVersion')
      ..remove('failures');
    final loaded = StoredTask.fromJson(json);
    expect(loaded.payloadVersion, 1);
    expect(loaded.failures, isEmpty);
  });

  group('failure history', () {
    test('keeps the last failures, oldest first', () async {
      final h = QueueHarness(failureHistory: 2);
      addTearDown(h.queue.close);
      h.register(
        handler: (task, context) async =>
            throw StateError('attempt ${context.attempt}'),
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
      var stored = (await h.queue.getTask(id))!;
      expect(stored.failures.map((f) => f.attempt), [1]);
      h.clock.advance(const Duration(seconds: 1));
      await h.until(() => h.of<TaskRetryScheduled>().length == 2);
      h.clock.advance(const Duration(seconds: 1));
      await h.until(() => h.of<TaskFailed>().length == 1);
      stored = (await h.queue.getTask(id))!;
      expect(stored.failures.map((f) => f.attempt), [2, 3]);
      expect(stored.failures.last.error, contains('attempt 3'));
      expect(stored.failures.last.failedAt, stored.lastFailure!.failedAt);

      // Survives a round trip and a retry.
      final copy = await reloadStorage(h.storage);
      expect((await copy.get(id))!.failures, hasLength(2));
      await h.queue.retry(id);
      expect((await h.queue.getTask(id))!.failures, hasLength(2));
    });

    test('defaults to 5', () async {
      final queue = DurableQueue(storage: MemoryQueueStorage());
      addTearDown(queue.close);
      expect(queue.failureHistory, 5);
    });

    test('is off with failureHistory: 0', () async {
      final h = QueueHarness(failureHistory: 0);
      addTearDown(h.queue.close);
      h.register(handler: (task, context) async => throw TemporaryFailure());
      final id = await h.queue.enqueue(ValueTask('a'));
      await h.queue.start();
      await h.until(() => h.of<TaskFailed>().length == 1);
      final stored = (await h.queue.getTask(id))!;
      expect(stored.lastFailure, isNotNull);
      expect(stored.failures, isEmpty);
    });

    test('records dependency cancellations too', () async {
      final h = QueueHarness(failureHistory: 5);
      addTearDown(h.queue.close);
      h.register();
      final parent = await h.queue.enqueue(ValueTask('p'));
      final child = await h.queue.enqueue(ValueTask('c'), dependsOn: [parent]);
      await h.queue.cancel(parent);
      final stored = (await h.queue.getTask(child))!;
      expect(stored.status, TaskStatus.cancelled);
      expect(stored.failures.single.error, contains(parent));
    });

    test('an unchanged failure is not recorded twice', () async {
      final h = QueueHarness(failureHistory: 5);
      addTearDown(h.queue.close);
      h.register(handler: (task, context) async => throw TemporaryFailure());
      final id = await h.queue.enqueue(
        ValueTask('a'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 3,
          delay: const Duration(hours: 1),
        ),
      );
      await h.queue.start();
      await h.until(() => h.of<TaskRetryScheduled>().length == 1);
      // Cancelling while the retry waits keeps the same last failure.
      await h.queue.cancel(id);
      expect((await h.queue.getTask(id))!.failures, hasLength(1));
    });

    test('a task cancelled when enqueued records why', () async {
      final h = QueueHarness(failureHistory: 5);
      addTearDown(h.queue.close);
      h.register();
      final parent = await h.queue.enqueue(ValueTask('p'));
      await h.queue.cancel(parent);
      final child = await h.queue.enqueue(ValueTask('c'), dependsOn: [parent]);
      final stored = (await h.queue.getTask(child))!;
      expect(stored.status, TaskStatus.cancelled);
      expect(stored.failures, hasLength(1));
    });

    test('must be from 0 to 20', () {
      expect(
        () => DurableQueue(storage: storage, failureHistory: 21),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => DurableQueue(storage: storage, failureHistory: -1),
        throwsA(isA<ArgumentError>()),
      );
    });
  });
}
