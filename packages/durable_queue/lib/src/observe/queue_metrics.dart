import 'dart:async';

import '../events/queue_event.dart';
import '../queue/durable_queue.dart';

/// Counts and timings for one task type, or for all of them.
final class TaskTypeMetrics {
  TaskTypeMetrics._();

  /// Tasks stored by `enqueue` or `retry`.
  int enqueued = 0;

  /// Attempts started.
  int started = 0;

  /// Tasks completed.
  int completed = 0;

  /// Tasks that failed for good.
  int failed = 0;

  /// Attempts that failed and were scheduled to run again.
  int retried = 0;

  /// Tasks cancelled, including expired ones.
  int cancelled = 0;

  /// Times a task was made to wait for a gate or a rate limit.
  int deferred = 0;

  /// Attempts whose run time was measured.
  int timedRuns = 0;

  /// Sum of measured run times.
  Duration totalRunTime = Duration.zero;

  /// Longest measured run time.
  Duration maxRunTime = Duration.zero;

  /// Tasks whose wait before their first attempt was measured.
  int timedWaits = 0;

  /// Sum of measured waits before the first attempt.
  Duration totalWaitTime = Duration.zero;

  /// Longest measured wait before the first attempt.
  Duration maxWaitTime = Duration.zero;

  /// Average run time of an attempt, or null before one was measured.
  Duration? get averageRunTime =>
      timedRuns == 0 ? null : totalRunTime ~/ timedRuns;

  /// Average wait before the first attempt, or null before one was
  /// measured.
  Duration? get averageWaitTime =>
      timedWaits == 0 ? null : totalWaitTime ~/ timedWaits;

  /// Completed tasks as a share of completed and failed ones, from 0 to 1,
  /// or null before any finished.
  double? get successRate {
    final finished = completed + failed;
    return finished == 0 ? null : completed / finished;
  }

  void _addRun(Duration run) {
    timedRuns++;
    totalRunTime += run;
    if (run > maxRunTime) maxRunTime = run;
  }

  void _addWait(Duration wait) {
    timedWaits++;
    totalWaitTime += wait;
    if (wait > maxWaitTime) maxWaitTime = wait;
  }

  TaskTypeMetrics _copy() => TaskTypeMetrics._()
    ..enqueued = enqueued
    ..started = started
    ..completed = completed
    ..failed = failed
    ..retried = retried
    ..cancelled = cancelled
    ..deferred = deferred
    ..timedRuns = timedRuns
    ..totalRunTime = totalRunTime
    ..maxRunTime = maxRunTime
    ..timedWaits = timedWaits
    ..totalWaitTime = totalWaitTime
    ..maxWaitTime = maxWaitTime;

  /// A JSON-encodable summary, with times in milliseconds.
  Map<String, Object?> toJson() => {
    'enqueued': enqueued,
    'started': started,
    'completed': completed,
    'failed': failed,
    'retried': retried,
    'cancelled': cancelled,
    'deferred': deferred,
    'successRate': successRate,
    'averageRunMs': averageRunTime?.inMilliseconds,
    'maxRunMs': timedRuns == 0 ? null : maxRunTime.inMilliseconds,
    'averageWaitMs': averageWaitTime?.inMilliseconds,
    'maxWaitMs': timedWaits == 0 ? null : maxWaitTime.inMilliseconds,
  };

  @override
  String toString() => 'TaskTypeMetrics(${toJson()})';
}

/// A copy of the numbers collected by [QueueMetrics] at one moment.
final class QueueMetricsSnapshot {
  QueueMetricsSnapshot._(this.since, this.takenAt, this.total, this.byType);

  /// When collection started, or was last reset, on the queue clock.
  final DateTime since;

  /// When this snapshot was taken, on the queue clock.
  final DateTime takenAt;

  /// All task types together.
  final TaskTypeMetrics total;

  /// Per task type, for types that had at least one event.
  final Map<String, TaskTypeMetrics> byType;

  /// A JSON-encodable summary, for example to send to an analytics service.
  Map<String, Object?> toJson() => {
    'since': since.toIso8601String(),
    'takenAt': takenAt.toIso8601String(),
    'total': total.toJson(),
    'byType': {
      for (final entry in byType.entries) entry.key: entry.value.toJson(),
    },
  };
}

/// Collects counts and timings from a queue's [DurableQueue.events], for
/// analytics, dashboards, or health checks.
///
/// ```dart
/// final metrics = QueueMetrics(queue);
/// // later, for example once a day:
/// analytics.log('queue', metrics.snapshot().toJson());
/// metrics.reset();
/// ```
///
/// Numbers are kept in memory, from the moment it is created, for this
/// process only. Times come from event times on the queue clock:
///
/// * Run time: from an attempt's [TaskStarted] to its outcome. For a task
///   handed off with `TaskContext.handOff`, it includes the time handed
///   off.
/// * Wait time: from enqueue, or a later `runAt`, to the first attempt.
///   It includes time spent waiting for a free slot, a gate, or a rate
///   limit. Only tasks enqueued while it listens are measured.
final class QueueMetrics {
  /// Starts collecting from [queue]'s events.
  QueueMetrics(this.queue) : _since = queue.clock.now() {
    _subscription = queue.events.listen(_record);
  }

  /// The queue being observed.
  final DurableQueue queue;

  late final StreamSubscription<QueueEvent> _subscription;
  DateTime _since;
  var _total = TaskTypeMetrics._();
  final _byType = <String, TaskTypeMetrics>{};

  /// Runs in progress, by task id.
  final _runStarts = <String, DateTime>{};

  /// When each task became ready, until its first attempt.
  final _readyAt = <String, DateTime>{};

  /// The numbers so far. The result does not change afterwards.
  QueueMetricsSnapshot snapshot() => QueueMetricsSnapshot._(
    _since,
    queue.clock.now(),
    _total._copy(),
    {for (final entry in _byType.entries) entry.key: entry.value._copy()},
  );

  /// Clears the numbers and starts again. Runs and waits in progress are
  /// still measured when they end.
  void reset() {
    _since = queue.clock.now();
    _total = TaskTypeMetrics._();
    _byType.clear();
  }

  /// Stops collecting.
  Future<void> dispose() => _subscription.cancel();

  void _record(QueueEvent event) {
    final type = _byType.putIfAbsent(event.taskType, TaskTypeMetrics._);
    void each(void Function(TaskTypeMetrics m) update) {
      update(_total);
      update(type);
    }

    switch (event) {
      case TaskEnqueued(:final runAt):
        each((m) => m.enqueued++);
        final at = event.occurredAt;
        _readyAt[event.taskId] = runAt != null && runAt.isAfter(at)
            ? runAt
            : at;
      case TaskStarted():
        each((m) => m.started++);
        _runStarts[event.taskId] = event.occurredAt;
        final ready = _readyAt.remove(event.taskId);
        if (ready != null) {
          final wait = _nonNegative(event.occurredAt.difference(ready));
          each((m) => m._addWait(wait));
        }
      case TaskRetryScheduled():
        each((m) => m.retried++);
        _endRun(event, each);
      case TaskCompleted():
        each((m) => m.completed++);
        _endRun(event, each);
      case TaskFailed():
        each((m) => m.failed++);
        _endRun(event, each);
        _readyAt.remove(event.taskId);
      case TaskCancelled():
        each((m) => m.cancelled++);
        _endRun(event, each);
        _readyAt.remove(event.taskId);
      case TaskDeferred():
        each((m) => m.deferred++);
        // A deferral after an error ends the attempt that hit the error.
        if (event.reason == DeferralReason.gateError) _endRun(event, each);
    }
  }

  void _endRun(
    QueueEvent event,
    void Function(void Function(TaskTypeMetrics m) update) each,
  ) {
    final start = _runStarts.remove(event.taskId);
    if (start == null) return;
    final run = _nonNegative(event.occurredAt.difference(start));
    each((m) => m._addRun(run));
  }

  static Duration _nonNegative(Duration value) =>
      value.isNegative ? Duration.zero : value;
}
