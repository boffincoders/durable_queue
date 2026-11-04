import 'dart:async';

/// Time source used for timestamps and retry delays.
///
/// Production code uses [SystemQueueClock]. Tests use `FakeQueueClock` and
/// move time themselves, so retry delays do not wait on real timers.
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
