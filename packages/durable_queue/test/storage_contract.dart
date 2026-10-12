import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

/// Behaviors every [QueueStorage] adapter must satisfy.
///
/// Adapter packages can mirror this suite. The scenarios are also described
/// in `STORAGE.md`.
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

  test('getNextReady orders by priority before enqueue order', () async {
    final now = DateTime.utc(2026);
    await storage.save(storedTask(id: 'old-low', sequence: 1, priority: -1));
    await storage.save(storedTask(id: 'normal', sequence: 2));
    await storage.save(
      storedTask(
        id: 'due-high',
        sequence: 3,
        priority: 5,
        status: TaskStatus.retryScheduled,
        nextAttemptAt: now,
      ),
    );
    await storage.save(
      storedTask(
        id: 'future-top',
        sequence: 4,
        priority: 9,
        status: TaskStatus.retryScheduled,
        nextAttemptAt: now.add(const Duration(minutes: 1)),
      ),
    );

    expect((await storage.getNextReady(now))?.id, 'due-high');
    await storage.delete('due-high');
    expect((await storage.getNextReady(now))?.id, 'normal');
    await storage.delete('normal');
    expect((await storage.getNextReady(now))?.id, 'old-low');
    expect(
      (await storage.getNextReady(now.add(const Duration(minutes: 1))))?.id,
      'future-top',
    );
  });

  test('waiting tasks are never ready but count as active', () async {
    await storage.save(
      storedTask(
        id: 'waiting',
        status: TaskStatus.waiting,
        dependsOn: ['parent'],
        deduplicationKey: 'key',
      ),
    );

    expect(await storage.getNextReady(DateTime.utc(2030)), isNull);
    expect((await storage.findActiveByDeduplicationKey('key'))?.id, 'waiting');
  });

  test('getWaitingDependents follows status and dependency edges', () async {
    await storage.save(
      storedTask(
        id: 'b',
        status: TaskStatus.waiting,
        dependsOn: ['root', 'other'],
        sequence: 2,
      ),
    );
    await storage.save(
      storedTask(
        id: 'a',
        status: TaskStatus.waiting,
        dependsOn: ['root'],
        sequence: 1,
      ),
    );
    await storage.save(
      storedTask(id: 'released', dependsOn: ['root'], sequence: 3),
    );

    expect(
      (await storage.getWaitingDependents('root')).map((task) => task.id),
      ['a', 'b'],
    );
    expect(
      (await storage.getWaitingDependents('other')).map((task) => task.id),
      ['b'],
    );
    expect(await storage.getWaitingDependents('nobody'), isEmpty);

    await storage.update(
      (await storage.get('a'))!.copyWith(status: TaskStatus.pending),
    );
    await storage.delete('b');
    expect(await storage.getWaitingDependents('root'), isEmpty);
  });

  test('getByGroup filters by group and optional status', () async {
    await storage.save(storedTask(id: 'a', group: 'sync', sequence: 1));
    await storage.save(
      storedTask(
        id: 'b',
        group: 'sync',
        sequence: 2,
        status: TaskStatus.completed,
      ),
    );
    await storage.save(storedTask(id: 'c', group: 'upload', sequence: 3));
    await storage.save(storedTask(id: 'd', sequence: 4));

    expect((await storage.getByGroup('sync')).map((t) => t.id), ['a', 'b']);
    expect(
      (await storage.getByGroup(
        'sync',
        status: TaskStatus.completed,
      )).map((t) => t.id),
      ['b'],
    );
    expect(await storage.getByGroup('missing'), isEmpty);

    await storage.update((await storage.get('a'))!.copyWith(group: 'upload'));
    expect((await storage.getByGroup('sync')).map((t) => t.id), ['b']);
    expect((await storage.getByGroup('upload')).map((t) => t.id), ['a', 'c']);
  });

  test('orchestration fields persist', () async {
    await storage.save(
      storedTask(
        id: 'a',
        status: TaskStatus.waiting,
        priority: 3,
        dependsOn: ['x', 'y'],
        onDependencyFailure: DependencyFailurePolicy.run,
        group: 'g',
      ),
    );
    final loaded = await storage.get('a');
    expect(loaded?.priority, 3);
    expect(loaded?.dependsOn, ['x', 'y']);
    expect(loaded?.onDependencyFailure, DependencyFailurePolicy.run);
    expect(loaded?.group, 'g');
  });
}
