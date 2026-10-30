import 'dart:async';

import '../errors.dart';
import '../events/queue_event.dart';
import '../events/task_hold_changed.dart';
import '../queue/durable_queue.dart';

/// How important a [QueueLogRecord] is.
enum QueueLogLevel {
  /// Routine progress: enqueued, started, deferred.
  debug,

  /// Outcomes and user actions: completed, cancelled, held, released.
  info,

  /// Something went wrong but will be tried again: a retry, a storage error.
  warning,

  /// A task failed for good.
  error,
}

/// One line logged by [QueueLogger].
final class QueueLogRecord {
  /// Creates a record.
  const QueueLogRecord({
    required this.level,
    required this.message,
    required this.time,
    this.taskId,
    this.taskType,
    this.event,
    this.error,
    this.stackTrace,
  });

  /// How important it is.
  final QueueLogLevel level;

  /// A one-line description, such as `completed upload 4f2c… (attempt 2)`.
  final String message;

  /// When it happened, on the queue clock.
  final DateTime time;

  /// The task it is about, if any.
  final String? taskId;

  /// The task's type, if any.
  final String? taskType;

  /// The queue event it describes, if it came from [DurableQueue.events].
  final QueueEvent? event;

  /// The storage error, for a [DurableQueue.storageErrors] record.
  final Object? error;

  /// The storage error's stack trace.
  final StackTrace? stackTrace;

  @override
  String toString() => '[durable_queue] ${level.name}: $message';
}

/// Turns a queue's events, hold changes, and storage errors into log
/// records, one per event.
///
/// ```dart
/// QueueLogger(queue, onRecord: (record) => debugPrint('$record'));
/// ```
///
/// With `package:logging`, map [QueueLogRecord.level] to a `Level`. Pass
/// [minLevel] to drop routine records. Messages contain task ids and types,
/// never payloads. Failure messages contain the error text the handler
/// threw, so do not put secrets in exceptions.
final class QueueLogger {
  /// Starts logging [queue] to [onRecord].
  QueueLogger(
    this.queue, {
    required this.onRecord,
    this.minLevel = QueueLogLevel.debug,
  }) {
    _subscriptions = [
      queue.events.listen(_onEvent),
      queue.holdChanges.listen(_onHold),
      queue.storageErrors.listen(_onStorageError),
    ];
  }

  /// The queue being logged.
  final DurableQueue queue;

  /// Receives each record at or above [minLevel].
  final void Function(QueueLogRecord record) onRecord;

  /// The least important level that is passed on.
  final QueueLogLevel minLevel;

  late final List<StreamSubscription<Object>> _subscriptions;

  /// Stops logging.
  Future<void> dispose() async {
    for (final subscription in _subscriptions) {
      await subscription.cancel();
    }
  }

  void _emit(QueueLogRecord record) {
    if (record.level.index < minLevel.index) return;
    onRecord(record);
  }

  void _onEvent(QueueEvent event) {
    final task = '${event.taskType} ${event.taskId}';
    final (level, message) = switch (event) {
      TaskEnqueued(:final runAt) => (
        QueueLogLevel.debug,
        'enqueued $task'
            '${runAt == null ? '' : ' to run at ${runAt.toIso8601String()}'}',
      ),
      TaskStarted(:final attempt) => (
        QueueLogLevel.debug,
        'started $task (attempt $attempt)',
      ),
      TaskRetryScheduled(
        :final attempt,
        :final maxAttempts,
        :final delay,
        :final error,
      ) =>
        (
          QueueLogLevel.warning,
          'attempt $attempt of $maxAttempts failed for $task, retrying in '
              '$delay: $error',
        ),
      TaskCompleted(:final attempt) => (
        QueueLogLevel.info,
        'completed $task (attempt $attempt)',
      ),
      TaskFailed(:final failure) => (
        QueueLogLevel.error,
        'failed $task after attempt ${failure.attempt}: ${failure.error}',
      ),
      TaskCancelled(:final wasRunning) => (
        QueueLogLevel.info,
        'cancelled $task${wasRunning ? ' while running' : ''}',
      ),
      TaskDeferred(:final reason, :final name, :final until) => (
        QueueLogLevel.debug,
        'deferred $task until ${until.toIso8601String()} '
            '(${reason.name} "$name")',
      ),
    };
    _emit(
      QueueLogRecord(
        level: level,
        message: message,
        time: event.occurredAt,
        taskId: event.taskId,
        taskType: event.taskType,
        event: event,
      ),
    );
  }

  void _onHold(TaskHoldChanged change) {
    _emit(
      QueueLogRecord(
        level: QueueLogLevel.info,
        message:
            '${change.held ? 'held' : 'released'} ${change.taskType} '
            '${change.taskId} (${change.status.name})',
        time: change.occurredAt,
        taskId: change.taskId,
        taskType: change.taskType,
      ),
    );
  }

  void _onStorageError(QueueStorageFailure failure) {
    _emit(
      QueueLogRecord(
        level: QueueLogLevel.warning,
        message:
            'storage ${failure.operation} failed (attempt '
            '${failure.attempt}), retrying: ${failure.error}',
        time: failure.occurredAt,
        error: failure.error,
        stackTrace: failure.stackTrace,
      ),
    );
  }
}
