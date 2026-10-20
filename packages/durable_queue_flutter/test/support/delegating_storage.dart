import 'package:durable_queue/durable_queue.dart';

/// Delegates to a [MemoryQueueStorage]; tests override single methods.
class DelegatingStorage implements QueueStorage {
  final MemoryQueueStorage _inner = MemoryQueueStorage();

  @override
  Future<void> save(StoredTask task) => _inner.save(task);
  @override
  Future<StoredTask?> get(String id) => _inner.get(id);
  @override
  Future<List<StoredTask>> getPending() => _inner.getPending();
  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit}) =>
      _inner.getByStatus(status, limit: limit);
  @override
  Future<StoredTask?> getNextReady(DateTime now) => _inner.getNextReady(now);
  @override
  Future<DateTime?> getNextWakeAt() => _inner.getNextWakeAt();
  @override
  Future<int> getMaxSequence() => _inner.getMaxSequence();
  @override
  Future<List<StoredTask>> getAll() => _inner.getAll();
  @override
  Future<StoredTask?> findActiveByDeduplicationKey(String key) =>
      _inner.findActiveByDeduplicationKey(key);
  @override
  Future<List<StoredTask>> getWaitingDependents(String id) =>
      _inner.getWaitingDependents(id);
  @override
  Future<List<StoredTask>> getByGroup(String group, {TaskStatus? status}) =>
      _inner.getByGroup(group, status: status);
  @override
  Future<void> update(StoredTask task) => _inner.update(task);
  @override
  Future<void> delete(String id) => _inner.delete(id);
}
