import 'dart:async';

import 'package:durable_queue/durable_queue.dart';

/// Whether the app is visible to the user.
enum AppVisibility {
  /// The app is in the foreground.
  foreground,

  /// The app is hidden or in the background, where the operating system may
  /// suspend or kill it at any time.
  background,
}

/// Starts a [DurableQueue] and pauses it while the app is in the background.
///
/// This is the platform-independent part of `DurableQueueLifecycle`, which
/// feeds it Flutter's app lifecycle. It can be driven by any source of
/// [AppVisibility], which is how it is tested.
///
/// Rules:
///
/// * [start] starts the queue if it is idle.
/// * Going to the background pauses a running queue when [pauseInBackground]
///   is true. Handlers that already started keep running; nothing new
///   starts.
/// * Coming back to the foreground resumes the queue, but only if this
///   controller paused it. A pause made by the app itself is left alone.
/// * Calls are applied in order, so quick background/foreground switches do
///   not race.
final class QueueLifecycleController {
  /// Creates a controller for [queue].
  QueueLifecycleController(this.queue, {this.pauseInBackground = true});

  /// The queue being managed.
  final DurableQueue queue;

  /// Whether the queue is paused while the app is in the background.
  final bool pauseInBackground;

  Future<void> _tail = Future<void>.value();
  var _pausedByLifecycle = false;
  var _visibility = AppVisibility.foreground;
  var _disposed = false;

  /// The last visibility reported to [setVisibility].
  AppVisibility get visibility => _visibility;

  /// Whether the queue is currently paused because the app is in the
  /// background.
  bool get isPausedByLifecycle => _pausedByLifecycle;

  /// Starts the queue if it is idle, then applies the current visibility.
  ///
  /// Errors from `DurableQueue.start`, such as a failed recovery, are
  /// returned to the caller; the queue stays idle and [start] can be called
  /// again.
  Future<void> start() => _enqueue(() async {
    if (queue.isClosed) return;
    if (queue.runState == QueueRunState.idle) await queue.start();
    await _apply();
  });

  /// Reports that the app moved to the foreground or the background.
  Future<void> setVisibility(AppVisibility visibility) {
    _visibility = visibility;
    return _enqueue(_apply);
  }

  Future<void> _apply() async {
    if (_disposed || queue.isClosed) return;
    switch (_visibility) {
      case AppVisibility.background:
        if (pauseInBackground && queue.runState == QueueRunState.running) {
          await queue.pause();
          _pausedByLifecycle = true;
        }
      case AppVisibility.foreground:
        if (_pausedByLifecycle) {
          _pausedByLifecycle = false;
          if (queue.runState == QueueRunState.paused) await queue.resume();
        }
    }
  }

  Future<void> _enqueue(Future<void> Function() action) {
    final result = _tail.then((_) => action());
    // Keep the chain alive after a failure; the caller still sees the error.
    _tail = result.catchError((Object _) {});
    return result;
  }

  /// Stops reacting to visibility changes. If the controller paused the
  /// queue, the queue is resumed; it is not stopped or closed.
  Future<void> dispose() => _enqueue(() async {
    if (_disposed) return;
    _disposed = true;
    if (_pausedByLifecycle &&
        !queue.isClosed &&
        queue.runState == QueueRunState.paused) {
      _pausedByLifecycle = false;
      await queue.resume();
    }
  });
}
