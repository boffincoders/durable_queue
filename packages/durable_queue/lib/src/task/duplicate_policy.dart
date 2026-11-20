/// @docImport '../events/queue_event.dart';
/// @docImport '../queue/durable_queue.dart';
/// @docImport 'task_status.dart';
library;

/// What [DurableQueue.enqueue] does when another task with the same
/// deduplication key is still active.
enum DuplicatePolicy {
  /// Keep the existing task unchanged and return its id. The new task is
  /// not stored. This is the default.
  keep,

  /// Run the new payload instead, so the latest version wins, for example
  /// the newest edit of a profile.
  ///
  /// * If the existing task has not started, its payload is replaced. It
  ///   keeps its place, attempts, schedule, and other settings, and a
  ///   [TaskEnqueued] event with `replaced: true` is emitted. Its
  ///   idempotency key becomes the one passed to this `enqueue` call, or
  ///   none: an earlier attempt may have reached the server under the old
  ///   key, and a server that honours it would not apply the new payload.
  ///   Give each version its own key, for example the record id and a
  ///   revision number.
  /// * If it is [TaskStatus.running], that attempt goes on with the old
  ///   payload, and the new task is stored to run after it, whether it
  ///   completes or not. Further calls replace the payload of that waiting
  ///   task.
  replace,
}
