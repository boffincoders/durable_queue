import 'package:durable_queue/durable_queue.dart';

/// Adapter wrapper for deterministic I/O faults and query-bound checks.
final class ProbeStorage implements QueueStorage {
  final inner = MemoryQueueStorage();
  Future<void> Function(String method, Object? argument)? before;
  Future<void> Function(String method, Object? result)? after;
  final calls = <String, int>{};

  Future<T> _call<T>(
    String method,
    Object? argument,
    Future<T> Function() action,
  ) async {
    calls.update(method, (n) => n + 1, ifAbsent: () => 1);
    await before?.call(method, argument);
    final result = await action();
    await after?.call(method, result);
    return result;
  }

  @override
  Future<void> save(StoredTask task) =>
      _call('save', task, () => inner.save(task));
  @override
  Future<void> update(StoredTask task) =>
      _call('update', task, () => inner.update(task));
  @override
  Future<void> delete(String id) => _call('delete', id, () => inner.delete(id));
  @override
  Future<StoredTask?> get(String id) => _call('get', id, () => inner.get(id));
  @override
  Future<List<StoredTask>> getAll() => _call('getAll', null, inner.getAll);
  @override
  Future<List<StoredTask>> getPending() =>
      _call('getPending', null, inner.getPending);
  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit}) =>
      _call(
        'getByStatus',
        limit,
        () => inner.getByStatus(status, limit: limit),
      );
  @override
  Future<StoredTask?> getNextReady(DateTime now) =>
      _call('getNextReady', now, () => inner.getNextReady(now));
  @override
  Future<DateTime?> getNextWakeAt() =>
      _call('getNextWakeAt', null, inner.getNextWakeAt);
  @override
  Future<int> getMaxSequence() =>
      _call('getMaxSequence', null, inner.getMaxSequence);
  @override
  Future<StoredTask?> findActiveByDeduplicationKey(String key) => _call(
    'findActiveByDeduplicationKey',
    key,
    () => inner.findActiveByDeduplicationKey(key),
  );
}
