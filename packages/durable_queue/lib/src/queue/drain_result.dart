/// @docImport 'durable_queue.dart';
library;

/// Why [DurableQueue.drain] returned.
enum DrainStopReason {
  /// Nothing was left to start: no task was running and none was ready
  /// (or, with `waitForScheduled`, due before the time limit).
  idle,

  /// The time limit passed. Tasks that were running then were allowed to
  /// finish; no new task started after the limit.
  timeLimit,
}

/// What a [DurableQueue.drain] call did.
final class DrainResult {
  /// Creates a result.
  const DrainResult({
    required this.reason,
    required this.started,
    required this.completed,
    required this.failed,
    required this.retryScheduled,
    required this.elapsed,
    required this.nextWakeAt,
  });

  /// Why the drain stopped.
  final DrainStopReason reason;

  /// Attempts started during the drain.
  final int started;

  /// Tasks that completed during the drain.
  final int completed;

  /// Tasks that failed for good during the drain.
  final int failed;

  /// Attempts that failed and were scheduled to retry later.
  final int retryScheduled;

  /// Time from the call to the queue being stopped, by the queue clock.
  final Duration elapsed;

  /// The earliest time a stored task becomes eligible again (a scheduled
  /// start, a retry, or a deferral), or null when no timed work is stored.
  ///
  /// A background job can use it to ask the operating system to run again
  /// around then.
  final DateTime? nextWakeAt;

  @override
  String toString() =>
      'DrainResult(${reason.name}, started: $started, completed: '
      '$completed, failed: $failed, retryScheduled: $retryScheduled, '
      'elapsed: $elapsed, nextWakeAt: $nextWakeAt)';
}
