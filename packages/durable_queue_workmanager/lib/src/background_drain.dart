import 'dart:async';

import 'package:durable_queue/durable_queue.dart';

import 'lease_keeper.dart';
import 'queue_lease.dart';

/// What [drainWithLease] did.
final class LeasedDrainResult {
  const LeasedDrainResult._(this.drain, {required this.leaseLost});

  /// The lease was held by another runner, so nothing ran.
  const LeasedDrainResult.skipped() : drain = null, leaseLost = false;

  /// The result of `DurableQueue.drain`, or null if the drain was skipped.
  final DrainResult? drain;

  /// Whether the drain ran.
  bool get ran => drain != null;

  /// Whether the lease could not be renewed while the drain ran, so another
  /// runner may have started the same storage in the meantime. This means
  /// the drain was frozen for longer than the lease ttl.
  final bool leaseLost;

  /// When stored work is next due (see [DrainResult.nextWakeAt]), or null.
  DateTime? get nextWakeAt => drain?.nextWakeAt;

  @override
  String toString() => drain == null
      ? 'LeasedDrainResult(skipped)'
      : 'LeasedDrainResult($drain, leaseLost: $leaseLost)';
}

/// Takes [lease], opens a queue with [openQueue], drains it, closes it, and
/// gives the lease back.
///
/// This is the body of a background job. If the lease is held, for example
/// by the app's foreground queue or by another job, it returns
/// [LeasedDrainResult.skipped] without opening the queue.
///
/// * [openQueue] builds the queue: storage, handlers, gates and limits, the
///   same way the app does. It is called only after the lease is taken.
/// * [timeLimit] and [waitForScheduled] are passed to `DurableQueue.drain`.
///   Running handlers are allowed to finish after the limit, so leave
///   headroom below what the platform allows.
/// * [closeQueue] runs when the drain is over, also after errors. It
///   defaults to `queue.close()`. Use it to close the storage as well.
/// * The lease is renewed every quarter of its ttl while the drain runs, by
///   [clock].
///
/// Errors from [openQueue], the drain, and [closeQueue] are passed on; the
/// lease is released either way.
Future<LeasedDrainResult> drainWithLease({
  required QueueLease lease,
  required FutureOr<DurableQueue> Function() openQueue,
  Duration? timeLimit,
  bool waitForScheduled = false,
  Future<void> Function(DurableQueue queue)? closeQueue,
  QueueClock clock = const SystemQueueClock(),
}) async {
  if (!await lease.tryAcquire()) return const LeasedDrainResult.skipped();
  var lost = false;
  final keeper = LeaseKeeper(lease, clock, () => lost = true)..start();
  try {
    final queue = await openQueue();
    final DrainResult drain;
    try {
      drain = await queue.drain(
        timeLimit: timeLimit,
        waitForScheduled: waitForScheduled,
      );
    } finally {
      await (closeQueue ?? _close)(queue);
    }
    return LeasedDrainResult._(drain, leaseLost: lost);
  } finally {
    keeper.stop();
    if (!lost) await lease.release();
  }
}

Future<void> _close(DurableQueue queue) => queue.close();
