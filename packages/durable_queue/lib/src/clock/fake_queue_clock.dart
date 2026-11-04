import 'dart:async';

import 'queue_clock.dart';

/// Manually advanced clock for deterministic tests.
///
/// From 1.0, import it from `package:durable_queue/testing.dart`; until
/// then the main library exports it too.
final class FakeQueueClock implements QueueClock {
  /// Creates a clock frozen at [initial].
  FakeQueueClock(DateTime initial) : _now = initial;

  DateTime _now;
  final List<_Waiter> _waiters = [];

  @override
  DateTime now() => _now;

  /// Moves the clock forward by [duration] and wakes due delays.
  ///
  /// [duration] must not be negative. Zero is allowed and wakes delays that
  /// are already due.
  void advance(Duration duration) {
    if (duration.isNegative) {
      throw ArgumentError.value(duration, 'duration', 'Must not be negative');
    }
    _now = _now.add(duration);
    final due = _waiters.where((waiter) => !waiter.time.isAfter(_now)).toList();
    for (final waiter in due) {
      _waiters.remove(waiter);
      if (!waiter.completer.isCompleted) waiter.completer.complete();
    }
  }

  @override
  QueueDelay delayUntil(DateTime time) {
    if (!_now.isBefore(time)) return QueueDelay.completed();
    final waiter = _Waiter(time, Completer<void>());
    _waiters.add(waiter);
    return QueueDelay(
      future: waiter.completer.future,
      onCancel: () {
        _waiters.remove(waiter);
        if (!waiter.completer.isCompleted) waiter.completer.complete();
      },
    );
  }
}

final class _Waiter {
  _Waiter(this.time, this.completer);

  final DateTime time;
  final Completer<void> completer;
}
