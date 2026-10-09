/// @docImport 'task/dependency_failure_policy.dart';
/// @docImport 'task/task_failure.dart';
library;

import 'task/task_status.dart';

/// Thrown when an operation targets a task id that is not in storage.
final class TaskNotFoundException implements Exception {
  /// Creates an exception for [taskId].
  TaskNotFoundException(this.taskId);

  /// The id that was not found.
  final String taskId;

  @override
  String toString() => 'TaskNotFoundException: no task with id "$taskId"';
}

/// Thrown when a task type has no registered handler.
final class UnknownTaskTypeException implements Exception {
  /// Creates an exception for an unregistered [type].
  UnknownTaskTypeException(this.type, {this.registered = const []});

  /// The task type that was requested.
  final String type;

  /// Task types registered on the queue at the time of the failure.
  final List<String> registered;

  @override
  String toString() {
    final known = registered.isEmpty
        ? 'none'
        : registered.map((type) => '"$type"').join(', ');
    return 'UnknownTaskTypeException: no handler registered for "$type" '
        '(registered: $known)';
  }
}

/// Thrown when a persisted payload cannot be turned back into a task.
final class TaskDecodeException implements Exception {
  /// Creates a decode exception with a human-readable [message].
  TaskDecodeException(this.message);

  /// Why decoding failed.
  final String message;

  @override
  String toString() => 'TaskDecodeException: $message';
}

/// Thrown by a handler to cooperatively acknowledge cancellation.
///
/// The queue does not interrupt a running handler. A handler that observes
/// [TaskContext.isCancellationRequested] can throw this exception to stop
/// cleanly. The task is then marked cancelled and is not retried.
final class TaskCancelledException implements Exception {
  /// Creates a cancellation acknowledgement.
  const TaskCancelledException([this.message = 'Task cancelled']);

  /// Optional detail included in [toString].
  final String message;

  @override
  String toString() => 'TaskCancelledException: $message';
}

/// Recorded when a task is still `running` after the process restarts.
///
/// The queue does not throw this to application code. It is stored on the
/// task as the failure of the interrupted attempt.
final class TaskInterruptedException implements Exception {
  /// Creates an interruption marker.
  const TaskInterruptedException();

  @override
  String toString() =>
      'TaskInterruptedException: task was still running when the queue restarted';
}

/// A worker storage operation failed and will be retried.
///
/// Delivered as a value on `DurableQueue.storageErrors`. The original error is
/// available to the application; it is not persisted as a task failure.
final class QueueStorageFailure {
  /// Creates a diagnostic for a failed storage call.
  const QueueStorageFailure({
    required this.operation,
    required this.error,
    required this.stackTrace,
    required this.occurredAt,
    required this.attempt,
  });

  /// Storage method that threw.
  final String operation;

  /// Original storage error.
  final Object error;

  /// Stack trace from the storage failure.
  final StackTrace stackTrace;

  /// Time of the failure according to the queue clock.
  final DateTime occurredAt;

  /// Number of consecutive failures for this particular operation.
  final int attempt;
}

/// Recorded when a waiting task cannot run because a dependency did not
/// complete.
///
/// The queue does not throw this to application code. It is stored as the
/// [TaskFailure] of a dependent task that was cancelled or failed by its
/// [DependencyFailurePolicy].
final class DependencyFailedException implements Exception {
  /// Creates an exception for [taskId] blocked by [dependencyId].
  const DependencyFailedException({
    required this.taskId,
    required this.dependencyId,
    required this.dependencyStatus,
  });

  /// The dependent task that will not run.
  final String taskId;

  /// The dependency that did not complete.
  final String dependencyId;

  /// Final status of the dependency, or null when it is no longer stored.
  final TaskStatus? dependencyStatus;

  @override
  String toString() {
    final status = dependencyStatus?.name ?? 'missing';
    return 'DependencyFailedException: task "$taskId" depends on '
        '"$dependencyId", which is $status';
  }
}
