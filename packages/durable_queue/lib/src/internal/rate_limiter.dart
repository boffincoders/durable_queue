import '../limits/rate_limit.dart';

/// Generic cell rate algorithm buckets for one [RateLimit].
///
/// Each key keeps a theoretical arrival time (TAT). A start at time `t` is
/// allowed when `TAT - tolerance <= t`; consuming moves TAT to
/// `max(TAT, t) + interval`. Consuming at a future time reserves that slot.
final class RateLimiter {
  RateLimiter(RateLimit limit)
    : _interval = limit.interval,
      _tolerance = limit.per - limit.interval;

  final Duration _interval;
  final Duration _tolerance;
  final Map<String?, DateTime> _arrival = {};

  /// Earliest time at or after [now] when [key] can start a task.
  DateTime earliest(String? key, DateTime now) {
    final arrival = _arrival[key];
    if (arrival == null) return now;
    final allowed = arrival.subtract(_tolerance);
    return allowed.isAfter(now) ? allowed : now;
  }

  /// Uses one start for [key] at [at], which must not be before
  /// [earliest] for that key.
  void consume(String? key, DateTime at) {
    final arrival = _arrival[key];
    final base = arrival == null || arrival.isBefore(at) ? at : arrival;
    _arrival[key] = base.add(_interval);
  }

  /// Forgets keys whose state no longer differs from an unused key.
  void prune(DateTime now) {
    _arrival.removeWhere((_, arrival) => !arrival.isAfter(now));
  }

  /// Number of keys tracked.
  int get size => _arrival.length;
}
