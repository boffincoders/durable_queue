import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  test('stored tasks round-trip through json', () {
    final original = storedTask(
      id: 'task-1',
      payload: {
        'path': '/storage/avatar.jpg',
        'sizes': [1, 2],
      },
      status: TaskStatus.retryScheduled,
      attempts: 2,
      retryPolicy: RetryPolicy.exponential(
        maxAttempts: 5,
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(minutes: 1),
        jitter: true,
      ),
      createdAt: DateTime.utc(2026, 1, 1, 12),
      updatedAt: DateTime.utc(2026, 1, 1, 12, 0, 5),
      nextAttemptAt: DateTime.utc(2026, 1, 1, 12, 0, 9),
      deduplicationKey: 'sync-user:123',
      idempotencyKey: 'order-9',
      lastFailure: TaskFailure(
        error: 'TemporaryFailure',
        stackTrace: '#0 main',
        failedAt: DateTime.utc(2026, 1, 1, 12, 0, 5),
        attempt: 2,
      ),
      cancelRequested: true,
      priority: -2,
      dependsOn: ['task-0'],
      onDependencyFailure: DependencyFailurePolicy.fail,
      group: 'uploads',
    );

    final restored = StoredTask.fromJson(original.toJson());

    expect(restored.toJson(), original.toJson());
    expect(restored.retryPolicy, original.retryPolicy);
    expect(restored.lastFailure?.attempt, 2);
    expect(restored.cancelRequested, isTrue);
    expect(restored.priority, -2);
    expect(restored.dependsOn, ['task-0']);
    expect(restored.onDependencyFailure, DependencyFailurePolicy.fail);
    expect(restored.group, 'uploads');
  });

  test('records written by 0.2.0 load with orchestration defaults', () {
    final legacy = storedTask(id: 'old').toJson()
      ..remove('priority')
      ..remove('dependsOn')
      ..remove('onDependencyFailure')
      ..remove('group');

    final restored = StoredTask.fromJson(legacy);

    expect(restored.priority, 0);
    expect(restored.dependsOn, isEmpty);
    expect(restored.onDependencyFailure, DependencyFailurePolicy.cancel);
    expect(restored.group, isNull);
  });

  test('invalid orchestration fields are rejected', () {
    final json = storedTask(id: 'a').toJson();
    expect(
      () => StoredTask.fromJson({...json, 'priority': '1'}),
      throwsFormatException,
    );
    expect(
      () => StoredTask.fromJson({
        ...json,
        'dependsOn': [1],
      }),
      throwsFormatException,
    );
    expect(
      () => StoredTask.fromJson({...json, 'onDependencyFailure': 'skip'}),
      throwsFormatException,
    );
    expect(() => storedTask(id: 'a', dependsOn: ['a']), throwsArgumentError);
    expect(() => storedTask(id: 'a', group: ''), throwsArgumentError);
  });

  test('dependsOn is unmodifiable and de-duplicated', () {
    final task = storedTask(id: 'a', dependsOn: ['x', 'y', 'x']);
    expect(task.dependsOn, ['x', 'y']);
    expect(() => task.dependsOn.add('z'), throwsUnsupportedError);
  });

  test('malformed stored task json is rejected', () {
    expect(() => StoredTask.fromJson({'id': 'x'}), throwsFormatException);
  });

  test('task payloads survive a storage reload', () async {
    final storage = MemoryQueueStorage();
    await storage.save(
      storedTask(id: 'photo', payload: {'path': '/storage/avatar.jpg'}),
    );

    final reloaded = await reloadStorage(storage);
    expect((await reloaded.get('photo'))?.payload, {
      'path': '/storage/avatar.jpg',
    });
  });
}
