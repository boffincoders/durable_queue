import 'package:durable_queue/durable_queue.dart';

import 'queue_lease.dart';

/// Renews a held [QueueLease] every quarter of its ttl until [stop], and
/// calls [onLost] once if a renewal reports that the lease is gone.
///
/// A renewal that throws is skipped; the next one tries again. If renewals
/// keep failing, the lease expires and a later renewal reports it lost.
final class LeaseKeeper {
  LeaseKeeper(this.lease, this.clock, this.onLost);

  final QueueLease lease;
  final QueueClock clock;
  final void Function() onLost;

  QueueDelay? _delay;
  var _stopped = false;

  Duration get interval => lease.ttl ~/ 4;

  void start() => _run();

  Future<void> _run() async {
    while (!_stopped) {
      final delay = _delay = clock.delayUntil(clock.now().add(interval));
      await delay.future;
      if (_stopped) return;
      bool held;
      try {
        held = await lease.renew();
      } on Object {
        continue;
      }
      if (_stopped) return;
      if (!held) {
        _stopped = true;
        onLost();
        return;
      }
    }
  }

  void stop() {
    _stopped = true;
    _delay?.cancel();
  }
}
