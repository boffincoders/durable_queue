/// Information about the attempt currently executing.
///
/// Handlers may ignore this. It exists so a handler can read the idempotency
/// key, report the attempt, or cooperate with cancellation.
final class TaskContext {
  /// Creates a context. Application code receives instances from the queue.
  TaskContext({
    required this.taskId,
    required this.attempt,
    required this.maxAttempts,
    required this.idempotencyKey,
    required this.deduplicationKey,
    required bool Function() isCancellationRequested,
  }) : _isCancellationRequested = isCancellationRequested;

  /// Id assigned when the task was enqueued.
  final String taskId;

  /// 1-based number of this attempt.
  final int attempt;

  /// Maximum attempts configured by the task's retry policy.
  final int maxAttempts;

  /// Application-defined idempotency key, when one was provided.
  final String? idempotencyKey;

  /// Deduplication key, when one was provided.
  final String? deduplicationKey;

  final bool Function() _isCancellationRequested;

  /// Whether [DurableQueue.cancel] was called for this running task.
  ///
  /// The queue cannot abort arbitrary Dart futures. Handlers that need to
  /// stop early should poll this and throw [TaskCancelledException].
  bool get isCancellationRequested => _isCancellationRequested();
}
