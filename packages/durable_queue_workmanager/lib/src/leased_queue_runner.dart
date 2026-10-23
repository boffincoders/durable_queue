import 'dart:async';

import 'package:durable_queue/durable_queue.dart';

import 'lease_keeper.dart';
import 'queue_lease.dart';

/// Runs the app's [DurableQueue] only while it holds a [QueueLease], so a
/// background job that drains the same storage never runs beside it.
///
/// * [start] takes the lease and starts the queue. If the lease is held,
///   for example by a background job that is still draining, it tries again
///   every [retryInterval] until it gets it or [stop] is called.
/// * While running, the lease is renewed every quarter of its ttl.
/// * [stop] stops the queue, waiting for running handlers, and then
///   releases the lease so a background job can take over.
/// * If the lease is lost (it could not be renewed in time and another
///   runner took it), the queue is stopped and, if still wanted, the runner
///   waits for the lease again.
///
/// Calls are applied in order. Tasks can be enqueued at any time; they wait
/// in storage until whoever holds the lease runs them.
///
/// In a Flutter app, `LeasedQueueLifecycle` calls [start] when the app comes
/// to the foreground and [stop] when it goes to the background.
final class LeasedQueueRunner {
  /// Creates a runner for [queue].
  LeasedQueueRunner(
    this.queue,
    this.lease, {
    this.retryInterval = const Duration(seconds: 5),
    this.onError,
    QueueClock clock = const SystemQueueClock(),
  }) : _clock = clock {
    if (retryInterval <= Duration.zero) {
      throw ArgumentError.value(
        retryInterval,
        'retryInterval',
        'Must be positive',
      );
    }
  }

  /// The queue being run.
  final DurableQueue queue;

  /// The lease that must be held to run [queue].
  final QueueLease lease;

  /// How often [start] tries again while the lease is held elsewhere.
  final Duration retryInterval;

  /// Receives errors that happen without a caller to return them to: when
  /// the queue is restarted after the lease was lost.
  final void Function(Object error, StackTrace stackTrace)? onError;

  final QueueClock _clock;

  Future<void> _tail = Future<void>.value();
  var _wanted = false;
  var _holding = false;
  var _disposed = false;
  LeaseKeeper? _keeper;
  QueueDelay? _retry;

  /// Whether this runner holds the lease and has started the queue.
  bool get isRunning => _holding;

  /// Whether [start] was called more recently than [stop].
  bool get isWanted => _wanted;

  /// Takes the lease, waiting for it if needed, and starts the queue.
  ///
  /// Completes when the queue is running, or earlier if [stop] or [dispose]
  /// is called first. Errors from the lease or from `DurableQueue.start`,
  /// such as a failed recovery, are returned; the lease is released and
  /// [start] can be called again.
  Future<void> start() {
    if (_disposed) return Future<void>.value();
    _wanted = true;
    return _enqueue(_reconcile);
  }

  /// Stops the queue and releases the lease. Also ends a [start] that is
  /// still waiting for the lease.
  Future<void> stop() {
    _wanted = false;
    _retry?.cancel();
    return _enqueue(_reconcile);
  }

  /// Stops like [stop]; later calls to [start] do nothing. The queue is not
  /// closed.
  Future<void> dispose() {
    _disposed = true;
    return stop();
  }

  Future<void> _reconcile() async {
    while (true) {
      if (_wanted && !_holding) {
        if (queue.isClosed) return;
        if (await lease.tryAcquire()) {
          try {
            if (queue.runState == QueueRunState.idle) await queue.start();
          } catch (_) {
            _wanted = false;
            await lease.release();
            rethrow;
          }
          _holding = true;
          _keeper = LeaseKeeper(lease, _clock, _onLeaseLost)..start();
          continue;
        }
        final retry = _retry = _clock.delayUntil(
          _clock.now().add(retryInterval),
        );
        await retry.future;
        _retry = null;
        continue;
      }
      if (!_wanted && _holding) {
        _keeper?.stop();
        _keeper = null;
        await queue.stop();
        _holding = false;
        await lease.release();
        continue;
      }
      return;
    }
  }

  void _onLeaseLost() {
    unawaited(
      _enqueue(() async {
        if (!_holding) return;
        _keeper = null;
        // Someone else may be running the storage now: start nothing new.
        await queue.stop();
        _holding = false;
        await _reconcile();
      }).catchError((Object error, StackTrace stackTrace) {
        onError?.call(error, stackTrace);
      }),
    );
  }

  Future<void> _enqueue(Future<void> Function() action) {
    final result = _tail.then((_) => action());
    _tail = result.catchError((Object _) {});
    return result;
  }
}
