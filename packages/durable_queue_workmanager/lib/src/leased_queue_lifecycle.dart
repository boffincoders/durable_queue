import 'dart:async';

import 'package:flutter/widgets.dart';

import 'leased_queue_runner.dart';

/// Runs a [LeasedQueueRunner] while the app is in the foreground.
///
/// * When the app is visible (`resumed`), the runner takes the lease and
///   starts the queue.
/// * When it is `hidden` or `paused`, the runner stops the queue, waiting
///   for running handlers, and releases the lease, so the background job can
///   drain the queue while the app is away.
/// * `inactive` and `detached` change nothing.
///
/// Use it instead of the lifecycle handling in `durable_queue_flutter`. To
/// keep `DurableQueueScope` for its builders, give it `runQueue: false`.
final class LeasedQueueLifecycle {
  LeasedQueueLifecycle._(this.runner, this._listener);

  /// Follows the app lifecycle with [runner] until [dispose].
  ///
  /// Requires an initialized Flutter binding. If the app is already visible
  /// (or its state is not known yet), the runner is started at once.
  ///
  /// [onError] receives errors from starting and stopping. Without it they
  /// are reported through [FlutterError].
  static LeasedQueueLifecycle attach(
    LeasedQueueRunner runner, {
    void Function(Object error, StackTrace stackTrace)? onError,
  }) {
    void report(Object error, StackTrace stackTrace) {
      if (onError != null) {
        onError(error, stackTrace);
        return;
      }
      FlutterError.reportError(
        FlutterErrorDetails(
          exception: error,
          stack: stackTrace,
          library: 'durable_queue_workmanager',
          context: ErrorDescription('while running a DurableQueue'),
        ),
      );
    }

    void apply(AppLifecycleState? state) {
      final Future<void> change;
      switch (state) {
        case null:
        case AppLifecycleState.resumed:
          change = runner.start();
        case AppLifecycleState.hidden:
        case AppLifecycleState.paused:
          change = runner.stop();
        case AppLifecycleState.inactive:
        case AppLifecycleState.detached:
          return;
      }
      unawaited(change.catchError(report));
    }

    final listener = AppLifecycleListener(onStateChange: apply);
    apply(WidgetsBinding.instance.lifecycleState);
    return LeasedQueueLifecycle._(runner, listener);
  }

  /// The runner being driven.
  final LeasedQueueRunner runner;
  final AppLifecycleListener _listener;

  /// Stops following the lifecycle and stops the runner, which releases the
  /// lease. The queue is not closed.
  Future<void> dispose() {
    _listener.dispose();
    return runner.stop();
  }
}
