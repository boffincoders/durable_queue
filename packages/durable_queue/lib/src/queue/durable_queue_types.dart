part of 'durable_queue.dart';

final class _PendingHandOff {
  _PendingHandOff(this.reference);

  final String reference;
  Future<void> saved = Future<void>.value();
  _HandOffOutcome? outcome;
}

final class _HandOffOutcome {
  _HandOffOutcome(this.error, this.stackTrace);

  /// Null for success.
  final Object? error;
  final StackTrace? stackTrace;
}

final class _Transition {
  _Transition(this.task, this.event);

  final StoredTask task;
  final QueueEvent event;
}

final class _EnqueueRequest {
  _EnqueueRequest({
    required this.payloadVersion,
    required this.type,
    required this.payload,
    required this.retryPolicy,
    required this.deduplicationKey,
    required this.idempotencyKey,
    required this.priority,
    required this.dependsOn,
    required this.onDependencyFailure,
    required this.group,
    this.onDuplicate = DuplicatePolicy.keep,
    this.runAt,
    this.expiresAt,
  });

  final String type;
  final Map<String, dynamic> payload;
  final RetryPolicy retryPolicy;
  final String? deduplicationKey;
  final DuplicatePolicy onDuplicate;
  final String? idempotencyKey;
  final int priority;
  final List<String> dependsOn;
  final DependencyFailurePolicy onDependencyFailure;
  final String? group;
  final DateTime? runAt;
  final DateTime? expiresAt;
  final int payloadVersion;

  _EnqueueRequest withDependencies(
    List<String> dependencies, {
    DependencyFailurePolicy? onDependencyFailure,
  }) {
    return _EnqueueRequest(
      type: type,
      payload: payload,
      retryPolicy: retryPolicy,
      deduplicationKey: deduplicationKey,
      onDuplicate: onDuplicate,
      idempotencyKey: idempotencyKey,
      priority: priority,
      dependsOn: List<String>.unmodifiable(LinkedHashSet.of(dependencies)),
      onDependencyFailure: onDependencyFailure ?? this.onDependencyFailure,
      group: group,
      runAt: runAt,
      expiresAt: expiresAt,
      payloadVersion: payloadVersion,
    );
  }
}

enum _VerdictKind { ready, wait, cancel, fail }

/// Outcome of checking a task's dependencies.
final class _Verdict {
  const _Verdict(this.kind, [this.blocker]);

  static const ready = _Verdict(_VerdictKind.ready);
  static const wait = _Verdict(_VerdictKind.wait);

  final _VerdictKind kind;

  /// The dependency that did not complete, for [_VerdictKind.cancel] and
  /// [_VerdictKind.fail].
  final DependencyFailedException? blocker;
}

/// A record to write and the event to emit after the write, if any.
final class _Resolved {
  const _Resolved(this.task, this.event);

  final StoredTask task;
  final QueueEvent? event;
}

/// Runs one storage operation. Worker paths retry; public calls do not.
typedef _StorageIo = Future<T> Function<T>(
  Future<T> Function() operation,
  String name,
);

Future<T> _direct<T>(Future<T> Function() operation, String name) =>
    operation();

/// A gate added with [DurableQueue.addGate].
final class _GateEntry {
  _GateEntry(this.gate);

  final QueueGate gate;
  StreamSubscription<bool>? subscription;

  /// Tasks this gate deferred in this process, with the time they were
  /// deferred to. Used to release them early when the gate opens.
  final Map<String, DateTime> deferred = {};
}

/// A limit added with [DurableQueue.addRateLimit].
final class _SharedLimit {
  _SharedLimit(this.limiter, this.types, this.keyOf);

  final RateLimiter limiter;
  final Set<String>? types;
  final RateLimitKey? keyOf;
}

/// One rate-limit bucket that applies to a task.
final class _Bucket {
  _Bucket(this.limiter, this.key, this.name);

  final RateLimiter limiter;
  final String? key;
  final String name;

  /// Identifies the bucket across claims.
  String get id => key == null ? name : '$name\u0000$key';
}

/// Why and until when a ready task must wait.
final class _Deferral {
  const _Deferral(this.reason, this.name, this.until);

  final DeferralReason reason;
  final String name;
  final DateTime until;
}

/// Result of one [DurableQueue._claimNext] call.
final class _Claim {
  const _Claim.claimed(StoredTask this.claimed)
    : deferred = null,
      expired = null;
  const _Claim.deferred(TaskDeferred this.deferred)
    : claimed = null,
      expired = null;
  const _Claim.expired(List<QueueEvent> this.expired)
    : claimed = null,
      deferred = null;

  final StoredTask? claimed;
  final TaskDeferred? deferred;

  /// Events from cancelling a task that reached its expiry.
  final List<QueueEvent>? expired;
}

/// Counts events while [DurableQueue.drain] runs.
final class _DrainStats {
  var started = 0;
  var completed = 0;
  var failed = 0;
  var retryScheduled = 0;

  void count(QueueEvent event) {
    switch (event) {
      case TaskStarted():
        started++;
      case TaskCompleted():
        completed++;
      case TaskFailed():
        failed++;
      case TaskRetryScheduled():
        retryScheduled++;
      default:
        break;
    }
  }
}
