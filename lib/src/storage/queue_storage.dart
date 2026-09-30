import '../task/stored_task.dart';
import '../task/task_status.dart';

/// Persistence boundary for the queue.
///
/// The engine depends on this interface, not on a database. [MemoryQueueStorage]
/// is the in-memory implementation shipped with the core package. A process
/// restart only survives when the application supplies an adapter that
/// actually stores [StoredTask] outside memory.
///
/// Contract:
///
/// * [save] inserts a new id and throws [StateError] if the id exists.
/// * [update] replaces an existing id and throws [StateError] if it does not.
/// * [delete] removes an id and does nothing if the id is absent.
/// * [get] returns the current record or null.
/// * [getPending] returns `status == pending`, oldest [StoredTask.createdAt]
///   first, then by [StoredTask.sequence], then by [StoredTask.id].
/// * [getByStatus] uses that same order.
/// * [getAll] returns every record in that same order.
/// * [findActiveByDeduplicationKey] returns the oldest task whose key matches
///   and whose status is `pending`, `running`, or `retryScheduled`.
/// * Each method is atomic. Callers must not observe a partially written task.
/// * Deduplication check-and-insert is performed by the queue, which
///   serializes enqueue calls. Adapters do not need a multi-method transaction.
abstract interface class QueueStorage {
  /// Inserts [task]. Throws [StateError] if [StoredTask.id] already exists.
  Future<void> save(StoredTask task);

  /// Returns the task with [id], or null.
  Future<StoredTask?> get(String id);

  /// Returns tasks in [TaskStatus.pending], oldest first.
  Future<List<StoredTask>> getPending();

  /// Returns tasks whose status is [status], oldest first.
  Future<List<StoredTask>> getByStatus(TaskStatus status);

  /// Returns every stored task, oldest first.
  Future<List<StoredTask>> getAll();

  /// Returns the oldest non-terminal task stored under [key], or null.
  ///
  /// Non-terminal means [TaskStatus.pending], [TaskStatus.running], or
  /// [TaskStatus.retryScheduled].
  Future<StoredTask?> findActiveByDeduplicationKey(String key);

  /// Replaces the stored record for `task.id`.
  ///
  /// Throws [StateError] if the id is not already stored.
  Future<void> update(StoredTask task);

  /// Removes [id]. Does nothing when [id] is not stored.
  Future<void> delete(String id);
}
