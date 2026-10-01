import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

/// Behaviors every [QueueStorage] adapter must satisfy.
///
/// Adapter packages can mirror this suite. The scenarios are also described
/// in `doc/storage.md`.
void queueStorageContractTests(QueueStorage Function() create) {
  late QueueStorage storage;

  setUp(() {
    storage = create();
  });

  test('save then get returns the same record', () async {
    final task = storedTask(id: 'a', idempotencyKey: 'order-1');
    await storage.save(task);

    final loaded = await storage.get('a');
    expect(loaded?.id, 'a');
    expect(loaded?.idempotencyKey, 'order-1');
    expect(loaded?.payload, {'value': 'a'});
  });

  test('get returns null for an unknown id', () async {
    expect(await storage.get('missing'), isNull);
  });

  test('save rejects a duplicate id', () async {
    await storage.save(storedTask(id: 'a'));
    expect(storage.save(storedTask(id: 'a')), throwsStateError);
  });

  test('update replaces an existing record', () async {
    await storage.save(storedTask(id: 'a'));
    await storage.update(
      storedTask(id: 'a', status: TaskStatus.running, attempts: 1),
    );

    final loaded = await storage.get('a');
    expect(loaded?.status, TaskStatus.running);
    expect(loaded?.attempts, 1);
  });

  test('update rejects an unknown id', () async {
    expect(storage.update(storedTask(id: 'missing')), throwsStateError);
  });

  test('delete removes a task and ignores an unknown id', () async {
    await storage.save(storedTask(id: 'a'));
    await storage.delete('a');
    await storage.delete('missing');
    expect(await storage.get('a'), isNull);
  });

  test('queries are ordered by createdAt, sequence, then id', () async {
    final earlier = DateTime.utc(2026, 1, 1);
    final later = DateTime.utc(2026, 1, 2);
    await storage.save(storedTask(id: 'b', createdAt: later));
    await storage.save(
      storedTask(id: 'c', createdAt: earlier, status: TaskStatus.failed),
    );
    await storage.save(storedTask(id: 'a', createdAt: earlier));

    expect((await storage.getAll()).map((task) => task.id), ['a', 'c', 'b']);
    expect((await storage.getPending()).map((task) => task.id), ['a', 'b']);
    expect(
      (await storage.getByStatus(TaskStatus.failed)).map((task) => task.id),
      ['c'],
    );
  });

  test('deduplication lookup returns the oldest active task', () async {
    final earlier = DateTime.utc(2026, 1, 1);
    final later = DateTime.utc(2026, 1, 2);
    await storage.save(
      storedTask(
        id: 'done',
        createdAt: earlier,
        status: TaskStatus.completed,
        deduplicationKey: 'user',
      ),
    );
    await storage.save(
      storedTask(
        id: 'second',
        createdAt: later,
        status: TaskStatus.running,
        deduplicationKey: 'user',
      ),
    );
    await storage.save(
      storedTask(
        id: 'first',
        createdAt: earlier,
        status: TaskStatus.retryScheduled,
        deduplicationKey: 'user',
      ),
    );
    await storage.save(
      storedTask(
        id: 'other',
        status: TaskStatus.pending,
        deduplicationKey: 'other',
      ),
    );

    expect((await storage.findActiveByDeduplicationKey('user'))?.id, 'first');
    expect(await storage.findActiveByDeduplicationKey('absent'), isNull);
  });

  test('deduplication lookup ignores terminal tasks', () async {
    for (final status in [
      TaskStatus.completed,
      TaskStatus.failed,
      TaskStatus.cancelled,
    ]) {
      await storage.save(
        storedTask(id: status.name, status: status, deduplicationKey: 'key'),
      );
    }

    expect(await storage.findActiveByDeduplicationKey('key'), isNull);
  });

  test('parallel saves all persist', () async {
    await Future.wait([
      for (var i = 0; i < 20; i++) storage.save(storedTask(id: 't$i')),
    ]);

    expect(await storage.getAll(), hasLength(20));
  });

  test(
    'bounded queries merge due retries and pending work in enqueue order',
    () async {
      final now = DateTime.utc(2026);
      await storage.save(
        storedTask(
          id: 'future',
          sequence: 0,
          status: TaskStatus.retryScheduled,
          nextAttemptAt: now.add(const Duration(seconds: 1)),
        ),
      );
      await storage.save(
        storedTask(
          id: 'retry',
          sequence: 1,
          status: TaskStatus.retryScheduled,
          nextAttemptAt: now,
        ),
      );
      await storage.save(storedTask(id: 'pending', sequence: 2));
      expect((await storage.getNextReady(now))?.id, 'retry');
      expect(await storage.getNextWakeAt(), now);
      expect(await storage.getMaxSequence(), 2);
      expect(
        (await storage.getByStatus(
          TaskStatus.retryScheduled,
          limit: 1,
        )).single.id,
        'future',
      );
      expect(
        storage.getByStatus(TaskStatus.pending, limit: 0),
        throwsArgumentError,
      );
      await storage.update(
        (await storage.get('retry'))!.copyWith(status: TaskStatus.completed),
      );
      expect((await storage.getNextReady(now))?.id, 'pending');
      expect(
        await storage.getNextWakeAt(),
        now.add(const Duration(seconds: 1)),
      );
      await storage.delete('pending');
      expect(await storage.getNextReady(now), isNull);
      expect(await storage.getMaxSequence(), 1);
      expect(
        (await storage.getNextReady(now.add(const Duration(seconds: 1))))?.id,
        'future',
      );
    },
  );

  test('empty bounded queries return null or zero', () async {
    expect(await storage.getNextReady(DateTime.utc(2026)), isNull);
    expect(await storage.getNextWakeAt(), isNull);
    expect(await storage.getMaxSequence(), 0);
    expect(await storage.getByStatus(TaskStatus.running, limit: 10), isEmpty);
  });
}
