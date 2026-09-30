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
    );

    final restored = StoredTask.fromJson(original.toJson());

    expect(restored.toJson(), original.toJson());
    expect(restored.retryPolicy, original.retryPolicy);
    expect(restored.lastFailure?.attempt, 2);
    expect(restored.cancelRequested, isTrue);
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
