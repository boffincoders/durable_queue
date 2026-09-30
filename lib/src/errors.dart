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
