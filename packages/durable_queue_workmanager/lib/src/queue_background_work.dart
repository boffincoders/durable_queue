import 'dart:async';
import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:workmanager/workmanager.dart'
    show
        Constraints,
        ExistingPeriodicWorkPolicy,
        ExistingWorkPolicy,
        ForegroundServiceConfig,
        Workmanager;

import 'background_drain.dart';
import 'queue_lease.dart';

/// Runs a durable_queue from Workmanager background jobs.
///
/// Create the same configuration in `main` and in the Workmanager callback
/// dispatcher, for example from one top-level function:
///
/// * In `main`, after `Workmanager().initialize`, call [schedule] to
///   register the periodic job.
/// * In the dispatcher, pass job names that [handles] accepts to [run]. It
///   takes the lease, drains the queue for up to [timeLimit], and closes
///   it again.
/// * On Android, when tasks are due before the next periodic run (a
///   scheduled task, a retry, or a deferral), [run] also registers a one-off
///   job named [wakeTaskName] for that time. See [scheduleWakeUps].
///
/// The foreground app must run its queue through a `LeasedQueueRunner` with
/// a lease on the same file, so that it and the background job never run
/// the storage at the same time.
final class QueueBackgroundWork {
  /// Creates a configuration.
  ///
  /// * [openQueue] builds the queue, with its storage, handlers, gates, and
  ///   limits, as the app does. It runs in the background isolate.
  /// * [openLease] returns the lease shared with the app, for example a
  ///   `FileQueueLease` in the application support directory.
  /// * [timeLimit] defaults to [defaultTimeLimit].
  /// * [closeQueue] runs after each drain; it defaults to `queue.close()`.
  ///   Use it to close the storage too.
  /// * [constraints] apply to the periodic job and the wake-up jobs, for
  ///   example `Constraints(networkType: NetworkType.connected)`.
  /// * [foregroundService] runs the jobs as Android foreground services
  ///   (see [foregroundService]).
  /// * [onError] receives errors from [run], which then returns false.
  /// * [workmanager] and [clock] are for tests.
  const QueueBackgroundWork({
    required this.openQueue,
    required this.openLease,
    this.taskName = defaultTaskName,
    this.wakeTaskName = defaultWakeTaskName,
    this.timeLimit,
    this.waitForScheduled = false,
    this.closeQueue,
    this.constraints,
    this.scheduleWakeUps,
    this.minWakeDelay = const Duration(minutes: 1),
    this.foregroundService,
    this.onError,
    this.workmanager,
    this.clock = const SystemQueueClock(),
  });

  /// The default name of the periodic job.
  static const defaultTaskName = 'durable_queue.drain';

  /// The default name of the one-off wake-up job.
  static const defaultWakeTaskName = 'durable_queue.wake';

  /// Builds the queue in the background isolate.
  final FutureOr<DurableQueue> Function() openQueue;

  /// Returns the lease shared with the foreground app.
  final FutureOr<QueueLease> Function() openLease;

  /// Name and unique name of the periodic job. On iOS this is also the
  /// `BGTaskSchedulerPermittedIdentifiers` entry in `Info.plist`.
  final String taskName;

  /// Name and unique name of the one-off wake-up job.
  final String wakeTaskName;

  /// How long one run may start tasks. Null means [defaultTimeLimit].
  final Duration? timeLimit;

  /// Passed to `DurableQueue.drain`: also wait for work that becomes due
  /// before the time limit.
  final bool waitForScheduled;

  /// Closes the queue after a run. Null means `queue.close()`.
  final Future<void> Function(DurableQueue queue)? closeQueue;

  /// Constraints for the periodic and wake-up jobs.
  final Constraints? constraints;

  /// Whether [run] registers a one-off job for [LeasedDrainResult.nextWakeAt],
  /// or for [minWakeDelay] from now when the time limit ended the run.
  ///
  /// Null (the default) means on Android only. On iOS a one-off job's delay
  /// is honored only while the app is alive, so iOS relies on the periodic
  /// job.
  final bool? scheduleWakeUps;

  /// The shortest delay used for a wake-up job, so that work that is due
  /// again at once (because the time limit ended a run) does not start jobs
  /// back to back.
  final Duration minWakeDelay;

  /// Runs the periodic and wake-up jobs as Android foreground services, with
  /// a notification, so a run can last longer than a normal background job.
  /// Set [timeLimit] to match, for example 30 minutes.
  ///
  /// While a run holds the lease, the app's own queue waits for it, so keep
  /// runs reasonably short and let wake-up jobs continue the work. Workmanager
  /// declares the `dataSync` service type; on Android 15 such services may
  /// run at most 6 hours a day, and on Android 13 and later the notification
  /// is shown only with the notification permission. Other platforms ignore
  /// it. For uploads and downloads, `durable_queue_transfer` lets the
  /// operating system run the transfer instead.
  final ForegroundServiceConfig? foregroundService;

  /// Receives errors from [run].
  final void Function(Object error, StackTrace stackTrace)? onError;

  /// The Workmanager instance to use. Null means `Workmanager()`.
  final Workmanager? workmanager;

  /// The clock used for wake-up delays and lease renewal.
  final QueueClock clock;

  /// The time limit used when [timeLimit] is null: 20 seconds on iOS, where
  /// a background refresh gets about 30 seconds, and 8 minutes elsewhere,
  /// where an Android worker gets 10.
  static Duration get defaultTimeLimit =>
      Platform.isIOS ? const Duration(seconds: 20) : const Duration(minutes: 8);

  Workmanager get _workmanager => workmanager ?? Workmanager();

  /// Whether [taskName] is one of this configuration's jobs.
  bool handles(String taskName) =>
      taskName == this.taskName || taskName == wakeTaskName;

  /// Registers the periodic job.
  ///
  /// Android runs it at most every [frequency] (at least 15 minutes), when
  /// [constraints] are met. iOS treats [frequency] as a hint and decides
  /// itself when to run it. By default an existing registration is updated
  /// in place, so calling this on every app start is fine.
  Future<void> schedule({
    Duration frequency = const Duration(minutes: 15),
    Duration? initialDelay,
    ExistingPeriodicWorkPolicy existingWorkPolicy =
        ExistingPeriodicWorkPolicy.update,
  }) {
    return _workmanager.registerPeriodicTask(
      taskName,
      taskName,
      frequency: frequency,
      initialDelay: initialDelay,
      constraints: constraints,
      existingWorkPolicy: existingWorkPolicy,
      foregroundServiceConfig: foregroundService,
    );
  }

  /// Cancels the periodic job and any pending wake-up job.
  Future<void> cancel() async {
    await _workmanager.cancelByUniqueName(taskName);
    await _workmanager.cancelByUniqueName(wakeTaskName);
  }

  /// Runs one background job: drains the queue under the lease and, if
  /// enabled, registers the next wake-up.
  ///
  /// Returns true when the job did its part, including when the lease was
  /// held elsewhere (the holder runs the work). Returns false after an
  /// error, which lets Android retry the job with backoff; the error goes
  /// to [onError].
  Future<bool> run(String taskName) async {
    try {
      final result = await drainWithLease(
        lease: await openLease(),
        openQueue: openQueue,
        timeLimit: timeLimit ?? defaultTimeLimit,
        waitForScheduled: waitForScheduled,
        closeQueue: closeQueue,
        clock: clock,
      );
      // A run ended by the time limit may have left ready work, which
      // nextWakeAt does not cover: come back as soon as allowed.
      final wakeAt = result.drain?.reason == DrainStopReason.timeLimit
          ? clock.now()
          : result.nextWakeAt;
      if (wakeAt != null && (scheduleWakeUps ?? Platform.isAndroid)) {
        await _scheduleWake(wakeAt, fromWake: taskName == wakeTaskName);
      }
      return true;
    } catch (error, stackTrace) {
      onError?.call(error, stackTrace);
      return false;
    }
  }

  Future<void> _scheduleWake(DateTime wakeAt, {required bool fromWake}) {
    var delay = wakeAt.difference(clock.now());
    if (delay < minWakeDelay) delay = minWakeDelay;
    return _workmanager.registerOneOffTask(
      wakeTaskName,
      wakeTaskName,
      initialDelay: delay,
      constraints: constraints,
      foregroundServiceConfig: foregroundService,
      // Inside the wake-up job itself, replacing would cancel the running
      // worker; appending queues the next one after it instead.
      existingWorkPolicy: fromWake
          ? ExistingWorkPolicy.append
          : ExistingWorkPolicy.replace,
    );
  }
}
