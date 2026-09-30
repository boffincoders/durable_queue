import '../task/task_failure.dart';

/// A lifecycle notification.
///
/// The stream is broadcast and does not replay past events. Subscribe before
/// calling [start] or [enqueue] if those events matter.
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
