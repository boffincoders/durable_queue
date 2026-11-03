/// How far a running task has got, reported with
/// `TaskContext.reportProgress` or `DurableQueue.reportProgress`.
///
/// Kept in memory only. It is not a `QueueEvent`, so code that switches over
/// every event type is unaffected.
final class TaskProgress {
  /// Creates a progress report.
  const TaskProgress({
    required this.taskId,
    required this.fraction,
    required this.reportedAt,
  });

  /// The task the report is about.
  final String taskId;

  /// From 0 (nothing done) to 1 (all done).
  final double fraction;

  /// When it was reported, on the queue clock.
  final DateTime reportedAt;

  @override
  String toString() =>
      'TaskProgress($taskId, ${(fraction * 100).toStringAsFixed(1)}%)';
}
