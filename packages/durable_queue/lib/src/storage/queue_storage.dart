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
///   and whose status is `pending`, `waiting`, `running`, or
///   `retryScheduled`.
/// * [getNextReady] orders by highest [StoredTask.priority] first, then the
///   order above. See [compareReadyTasks].
/// * [getWaitingDependents] and [getByGroup] use the createdAt, sequence, id
///   order.
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
  ///
  /// [limit], when supplied, must be positive and bounds the returned records.
  /// Recovery repeatedly requests bounded batches of running tasks.
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit});

  /// Returns the next eligible pending or retry-scheduled task, or null.
  ///
  /// Eligible means `nextAttemptAt` is null or at/before [now]. Order by
  /// highest [StoredTask.priority] first, then createdAt, sequence, and id,
  /// across both statuses ([compareReadyTasks]). [TaskStatus.waiting] tasks
  /// are never eligible. This is a bounded query: adapters should use
  /// indexes, not materialize the entire backlog.
  Future<StoredTask?> getNextReady(DateTime now);

  /// Earliest non-null nextAttemptAt among pending and retry-scheduled tasks.
  ///
  /// Return overdue timestamps too. Return null if no timed work exists.
  Future<DateTime?> getNextWakeAt();

  /// Largest sequence currently stored, or zero for empty storage.
  ///
  /// Implement with an aggregate/index rather than loading all records.
  Future<int> getMaxSequence();

  /// Returns every stored task, oldest first.
  Future<List<StoredTask>> getAll();

  /// Returns the oldest non-terminal task stored under [key], or null.
  ///
  /// Non-terminal means [TaskStatus.pending], [TaskStatus.waiting],
  /// [TaskStatus.running], or [TaskStatus.retryScheduled].
  Future<StoredTask?> findActiveByDeduplicationKey(String key);

  /// Returns [TaskStatus.waiting] tasks whose [StoredTask.dependsOn] contains
  /// [id], oldest first.
  ///
  /// The queue calls this when [id] reaches a terminal state, and before
  /// deleting [id]. Adapters should index dependency edges of waiting tasks
  /// rather than scan every record.
  Future<List<StoredTask>> getWaitingDependents(String id);

  /// Returns tasks whose [StoredTask.group] equals [group], oldest first.
  ///
  /// When [status] is supplied, only tasks in that status are returned.
  Future<List<StoredTask>> getByGroup(String group, {TaskStatus? status});

  /// Replaces the stored record for `task.id`.
  ///
  /// Throws [StateError] if the id is not already stored.
  Future<void> update(StoredTask task);

  /// Removes [id]. Does nothing when [id] is not stored.
  Future<void> delete(String id);
}
