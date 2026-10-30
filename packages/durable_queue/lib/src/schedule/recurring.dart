import 'dart:async';

import '../events/queue_event.dart';
import '../queue/durable_queue.dart';
import '../retry/retry_policy.dart';
import '../task/durable_task.dart';
import '../task/stored_task.dart';
import '../task/task_status.dart';

/// Keeps one occurrence of each recurring task stored. Used by
/// `DurableQueue.schedule`.
final class RecurringScheduler {
  RecurringScheduler(this._queue);

  final DurableQueue _queue;
  final _schedules = <String, _Schedule>{};
  StreamSubscription<QueueEvent>? _events;

  static const _prefix = 'schedule:';

  /// The group and deduplication key of [name]'s occurrences.
  static String keyOf(String name) => '$_prefix$name';

  Set<String> get names => Set.unmodifiable(_schedules.keys);

  Future<String> add(
    String name, {
    required Duration every,
    required DurableTask Function() task,
    DateTime? firstRunAt,
    RetryPolicy? retryPolicy,
    int priority = 0,
  }) async {
    if (name.trim().isEmpty) {
      throw ArgumentError.value(name, 'name', 'Must not be blank');
    }
    if (every < const Duration(seconds: 1)) {
      throw ArgumentError.value(every, 'every', 'Must be at least one second');
    }
    if (_schedules.containsKey(name)) {
      throw StateError('Schedule "$name" has already been added');
    }
    final schedule = _Schedule(
      name,
      every,
      task,
      firstRunAt,
      retryPolicy,
      priority,
    );
    _schedules[name] = schedule;
    _events ??= _queue.events.listen(_onEvent);
    try {
      return await _ensureNext(schedule);
    } on Object {
      _schedules.remove(name);
      rethrow;
    }
  }

  Future<bool> remove(String name) async {
    _schedules.remove(name);
    var changed = false;
    final tasks = await _queue.getTasks(group: keyOf(name));
    for (final task in tasks) {
      if (!task.status.isActive || task.status == TaskStatus.running) continue;
      await _queue.cancel(task.id);
      changed = true;
    }
    return changed;
  }

  Future<void> close() async => _events?.cancel();

  void _onEvent(QueueEvent event) {
    if (event is! TaskCompleted &&
        event is! TaskFailed &&
        event is! TaskCancelled) {
      return;
    }
    for (final schedule in _schedules.values) {
      if (schedule.activeId != event.taskId) continue;
      schedule.activeId = null;
      unawaited(
        _ensureNext(schedule).then<void>(
          (_) {},
          onError: (Object error, StackTrace stackTrace) {
            // A queue closed meanwhile has nothing left to schedule; the
            // next launch catches up.
            if (error is StateError && _queue.isClosed) return;
            Zone.current.handleUncaughtError(error, stackTrace);
          },
        ),
      );
    }
  }

  /// Returns the id of [schedule]'s active occurrence, storing the next one
  /// if none is active.
  Future<String> _ensureNext(_Schedule schedule) async {
    final key = keyOf(schedule.name);
    final tasks = await _queue.getTasks(group: key);
    StoredTask? last;
    for (final task in tasks) {
      if (task.status.isActive) {
        schedule.activeId = task.id;
        return task.id;
      }
      last = task;
    }
    if (!identical(_schedules[schedule.name], schedule)) {
      throw StateError('Schedule "${schedule.name}" was removed');
    }
    final now = _queue.clock.now();
    final slot = last == null
        ? (schedule.firstRunAt ?? now)
        : _nextSlot(_slotOf(last) ?? last.updatedAt, schedule.every, now);
    final id = await _queue.enqueue(
      schedule.task(),
      deduplicationKey: key,
      idempotencyKey: '$key@${slot.toUtc().toIso8601String()}',
      group: key,
      runAt: slot,
      retryPolicy: schedule.retryPolicy,
      priority: schedule.priority,
    );
    schedule.activeId = id;
    return id;
  }

  /// The slot an occurrence was stored for, from its idempotency key.
  static DateTime? _slotOf(StoredTask task) {
    final key = task.idempotencyKey;
    if (key == null) return null;
    final at = key.lastIndexOf('@');
    if (at < 0) return null;
    return DateTime.tryParse(key.substring(at + 1));
  }

  /// The slot after [previous]. When slots were missed, the latest missed
  /// one, which starts at once; earlier missed slots are skipped.
  static DateTime _nextSlot(DateTime previous, Duration every, DateTime now) {
    final next = previous.add(every);
    if (next.isAfter(now)) return next;
    final missed =
        now.difference(previous).inMicroseconds ~/ every.inMicroseconds;
    return previous.add(every * missed);
  }
}

final class _Schedule {
  _Schedule(
    this.name,
    this.every,
    this.task,
    this.firstRunAt,
    this.retryPolicy,
    this.priority,
  );

  final String name;
  final Duration every;
  final DurableTask Function() task;
  final DateTime? firstRunAt;
  final RetryPolicy? retryPolicy;
  final int priority;
  String? activeId;
}
