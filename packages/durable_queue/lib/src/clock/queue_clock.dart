import 'dart:async';

/// Time source used for timestamps and retry delays.
///
/// Production code uses [SystemQueueClock]. Tests use [FakeQueueClock] and
/// move time with [FakeQueueClock.advance], so retry delays do not wait on
/// real timers.
abstract interface class QueueClock {
  /// Current time according to this clock.
  DateTime now();

  /// Completes when [time] has been reached.
  ///
  /// Cancelling the returned delay completes its future without being treated
  /// as a successful wake by the queue. The queue distinguishes cancellation
  /// from a real wake with a generation counter.
  QueueDelay delayUntil(DateTime time);
}

/// A cancellable wait returned by [QueueClock.delayUntil].
final class QueueDelay {
  /// Creates a delay whose [future] completes when the clock reaches the
  /// requested time, or when [cancel] is called.
  QueueDelay({required this.future, required void Function() onCancel})
    : _onCancel = onCancel;

  /// Completes when the delay elapses or is cancelled.
  final Future<void> future;

  final void Function() _onCancel;
  var _cancelled = false;

  /// An already completed delay.
  factory QueueDelay.completed() {
    return QueueDelay(future: Future<void>.value(), onCancel: () {});
  }

  /// Releases any underlying timer or waiter.
  ///
  /// Safe to call more than once.
  void cancel() {
    if (_cancelled) return;
    _cancelled = true;
    _onCancel();
  }
}

/// Clock backed by [DateTime.now] and [Timer].
final class SystemQueueClock implements QueueClock {
  /// Creates a system clock.
  const SystemQueueClock();

  @override
  DateTime now() => DateTime.now();

  @override
  QueueDelay delayUntil(DateTime time) {
    final remaining = time.difference(now());
    if (remaining <= Duration.zero) return QueueDelay.completed();
    final completer = Completer<void>();
    final timer = Timer(remaining, () {
      if (!completer.isCompleted) completer.complete();
    });
    return QueueDelay(
      future: completer.future,
      onCancel: () {
        timer.cancel();
        if (!completer.isCompleted) completer.complete();
      },
    );
  }
}

/// Manually advanced clock for deterministic tests.
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
