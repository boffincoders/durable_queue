/// @docImport '../queue/durable_queue.dart';
library;

import '../task/task_failure.dart';

/// A lifecycle notification.
///
/// The stream is broadcast and does not replay past events. Subscribe before
/// calling [DurableQueue.start] or [DurableQueue.enqueue] if those events
/// matter.
sealed class QueueEvent {
  /// Creates an event that occurred at [occurredAt].
  const QueueEvent({
    required this.taskId,
    required this.taskType,
    required this.occurredAt,
  });

  /// Task the event refers to.
  final String taskId;

  /// `DurableTask.type` for [taskId].
  final String taskType;

  /// When the event was recorded, according to the queue clock.
  final DateTime occurredAt;
}

/// A new task was stored.
final class TaskEnqueued extends QueueEvent {
  /// Creates an enqueued event.
  const TaskEnqueued({
    required super.taskId,
    required super.taskType,
    required super.occurredAt,
    this.deduplicationKey,
    this.idempotencyKey,
  });

  /// Deduplication key stored with the task, if any.
  final String? deduplicationKey;

  /// Idempotency key stored with the task, if any.
  final String? idempotencyKey;
}

/// A handler invocation has started.
final class TaskStarted extends QueueEvent {
  /// Creates a started event.
  const TaskStarted({
    required super.taskId,
    required super.taskType,
    required super.occurredAt,
    required this.attempt,
  });

  /// 1-based attempt that is starting.
  final int attempt;
}

/// A failed attempt was scheduled to run again.
final class TaskRetryScheduled extends QueueEvent {
  /// Creates a retry event.
  const TaskRetryScheduled({
    required super.taskId,
    required super.taskType,
    required super.occurredAt,
    required this.attempt,
    required this.maxAttempts,
    required this.delay,
    required this.nextAttemptAt,
    required this.error,
  });

  /// Attempt that just failed.
  final int attempt;

  /// Attempt budget for the task.
  final int maxAttempts;

  /// Delay until [nextAttemptAt].
  final Duration delay;

  /// When the task becomes eligible again.
  final DateTime nextAttemptAt;

  /// `error.toString()` from the failed attempt.
  final String error;
}

/// A handler invocation finished successfully.
final class TaskCompleted extends QueueEvent {
  /// Creates a completion event.
  const TaskCompleted({
    required super.taskId,
    required super.taskType,
    required super.occurredAt,
    required this.attempt,
  });

  /// Attempt that succeeded.
  final int attempt;
}

/// A task will not be attempted again.
final class TaskFailed extends QueueEvent {
  /// Creates a failure event.
  const TaskFailed({
    required super.taskId,
    required super.taskType,
    required super.occurredAt,
    required this.failure,
  });

  /// Failure recorded for the last attempt.
  final TaskFailure failure;
}

/// A task was cancelled.
final class TaskCancelled extends QueueEvent {
  /// Creates a cancellation event.
  const TaskCancelled({
    required super.taskId,
    required super.taskType,
    required super.occurredAt,
    required this.wasRunning,
  });

  /// True when cancellation was applied to an in-flight attempt.
  ///
  /// False when a `pending` or `retryScheduled` task was cancelled before
  /// its handler started.
  final bool wasRunning;
}

/// Why a task was deferred. See [TaskDeferred].
enum DeferralReason {
  /// A rate limit had no start left; the task waits for its reserved slot.
  rateLimit,

  /// A gate the task requires was closed.
  gateClosed,

  /// The handler failed with an error that `register(deferOn:)` attributed
  /// to a gate, for example a connection error. The attempt does not count.
  gateError,
}

/// A task was moved to a later time without using an attempt.
///
/// The task keeps its status (`pending` or `retryScheduled`), its attempt
/// count, and its place among tasks of equal priority. Its
/// `StoredTask.nextAttemptAt` is set to [until]. A task deferred by a closed
/// gate is released as soon as that gate opens, which can be before [until].
final class TaskDeferred extends QueueEvent {
  /// Creates a deferral event.
  const TaskDeferred({
    required super.taskId,
    required super.taskType,
    required super.occurredAt,
    required this.reason,
    required this.name,
    required this.until,
    this.error,
  });

  /// Why the task was deferred.
  final DeferralReason reason;

  /// The rate limit or gate responsible. A limit set with
  /// `register(rateLimit:)` is named `type:<task type>`.
  final String name;

  /// When the task becomes eligible again at the latest.
  final DateTime until;

  /// The handler error, for [DeferralReason.gateError].
  final String? error;
}
