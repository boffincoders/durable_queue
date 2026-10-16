import '../task/stored_task.dart';

/// At most [count] task starts per [per].
///
/// Every attempt counts, retries included, because each attempt can reach
/// the service the limit protects. Up to [count] tasks may start back to back
/// after a quiet period; after that, starts are spaced by `per / count`.
///
/// Limits are kept in memory. After a restart they start empty.
final class RateLimit {
  /// Creates a limit of [count] starts per [per].
  ///
  /// [count] must be at least 1 and [per] must be at least [count]
  /// microseconds.
  RateLimit(this.count, {required this.per}) {
    if (count < 1) {
      throw ArgumentError.value(count, 'count', 'Must be at least 1');
    }
    if (per.inMicroseconds < count) {
      throw ArgumentError.value(
        per,
        'per',
        'Must be at least $count microseconds',
      );
    }
  }

  /// Starts allowed per [per].
  final int count;

  /// Length of the window.
  final Duration per;

  /// Spacing between starts once the burst is used up.
  Duration get interval => Duration(microseconds: per.inMicroseconds ~/ count);

  @override
  String toString() => 'RateLimit($count per $per)';
}

/// Chooses which bucket of a shared rate limit a task uses, for example the
/// host of an HTTP request. Return null to leave the task unlimited by that
/// limit.
///
/// A function that throws leaves the task unlimited by that limit.
typedef RateLimitKey = String? Function(StoredTask task);
