/// Explicit lifecycle of a persisted task.
///
/// ```text
/// pending → running → completed
///                  ↘ retryScheduled → running
///                  ↘ failed
/// pending or retryScheduled → cancelled
/// failed or cancelled → pending   (DurableQueue.retry)
/// ```
enum TaskStatus {
  /// Stored and eligible to start when the queue is running.
  pending,

  /// A handler invocation is in progress.
  running,

  /// The last attempt failed and another attempt is scheduled.
  retryScheduled,

  /// The handler finished successfully.
  completed,

  /// The handler failed and no further attempts will be made.
  failed,

  /// The task was cancelled before it finished.
  cancelled;

  /// Whether the task can still run: [pending], [running], or
  /// [retryScheduled].
  bool get isActive => !isTerminal;

  /// Whether the task has finished: [completed], [failed], or [cancelled].
  bool get isTerminal =>
      this == completed || this == failed || this == cancelled;
}
