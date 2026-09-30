/// Explicit lifecycle of a persisted task.
///
/// ```text
/// pending → running → completed
///                  ↘ retryScheduled → pending
///                  ↘ failed
/// pending or retryScheduled → cancelled
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
  cancelled,
}
