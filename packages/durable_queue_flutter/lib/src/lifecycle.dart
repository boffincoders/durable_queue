import 'package:durable_queue/durable_queue.dart';
import 'package:flutter/widgets.dart';

import 'lifecycle_controller.dart';

/// Connects a [DurableQueue] to the Flutter app lifecycle.
///
/// [attach] starts the queue and, by default, pauses it while the app is
/// hidden or paused and resumes it when the app is back in the foreground.
/// See [QueueLifecycleController] for the exact rules.
///
/// Pausing in the background stops new tasks from starting while the
/// operating system may suspend or kill the app; handlers already running
/// keep going. It does not make tasks run while the app is closed.
///
/// Most apps use `DurableQueueScope` instead, which attaches and disposes
/// this for a widget subtree.
final class DurableQueueLifecycle {
  DurableQueueLifecycle._(this.controller, this._listener);

  /// Starts [queue] and follows the app lifecycle until [dispose].
  ///
  /// Requires an initialized Flutter binding
  /// (`WidgetsFlutterBinding.ensureInitialized()` or a running app). The
  /// returned future completes when the queue has started; startup errors,
  /// such as a failed recovery, are reported through it.
  static Future<DurableQueueLifecycle> attach(
    DurableQueue queue, {
    bool pauseInBackground = true,
  }) async {
    final controller = QueueLifecycleController(
      queue,
      pauseInBackground: pauseInBackground,
    );
    final listener = AppLifecycleListener(
      onStateChange: (state) {
        final visibility = visibilityOf(state);
        if (visibility != null) controller.setVisibility(visibility);
      },
    );
    final lifecycle = DurableQueueLifecycle._(controller, listener);
    final current = WidgetsBinding.instance.lifecycleState;
    final visibility = current == null ? null : visibilityOf(current);
    if (visibility != null) controller.setVisibility(visibility);
    await controller.start();
    return lifecycle;
  }

  /// The platform-independent rules this lifecycle applies.
  final QueueLifecycleController controller;
  final AppLifecycleListener _listener;

  /// Maps a Flutter lifecycle state to [AppVisibility].
  ///
  /// `resumed` is the foreground; `hidden` and `paused` are the background.
  /// `inactive` (for example during a phone call overlay or the app
  /// switcher) and `detached` keep the previous visibility, so they return
  /// null.
  static AppVisibility? visibilityOf(AppLifecycleState state) {
    switch (state) {
      case AppLifecycleState.resumed:
        return AppVisibility.foreground;
      case AppLifecycleState.hidden:
      case AppLifecycleState.paused:
        return AppVisibility.background;
      case AppLifecycleState.inactive:
      case AppLifecycleState.detached:
        return null;
    }
  }

  /// Stops following the lifecycle. A queue paused by this lifecycle is
  /// resumed; the queue is not stopped or closed.
  Future<void> dispose() {
    _listener.dispose();
    return controller.dispose();
  }
}
