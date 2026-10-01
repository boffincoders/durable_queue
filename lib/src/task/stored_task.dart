import 'dart:collection';

import '../retry/retry_policy.dart';
import 'task_failure.dart';
import 'task_status.dart';

const Object _unset = Object();

/// Persisted record for one task.
///
/// Storage adapters persist this value. Application code normally receives it
/// from `DurableQueue.getTask` and `DurableQueue.getTasks`. The handler runs
/// the decoded [DurableTask], not this record.
///
/// Every field round-trips through [toJson] and [fromJson].
final class StoredTask {
  /// Creates a stored task.
  StoredTask({
    required this.id,
    required this.type,
    required Map<String, dynamic> payload,
    required this.status,
    required this.attempts,
    required this.retryPolicy,
    required this.createdAt,
    required this.updatedAt,
    required this.sequence,
    this.nextAttemptAt,
    this.deduplicationKey,
    this.idempotencyKey,
    this.lastFailure,
    this.cancelRequested = false,
  }) : payload = _freezeMap(payload) {
    if (id.isEmpty) {
      throw ArgumentError.value(id, 'id', 'Must not be empty');
    }
    if (type.isEmpty) {
      throw ArgumentError.value(type, 'type', 'Must not be empty');
    }
    if (attempts < 0) {
      throw ArgumentError.value(attempts, 'attempts', 'Must not be negative');
    }
    if (sequence < 0) {
      throw ArgumentError.value(sequence, 'sequence', 'Must not be negative');
    }
  }

  /// Unique task id.
  final String id;

  /// Registry key. Matches `DurableTask.type`.
  final String type;

  /// Deeply immutable JSON snapshot produced by `DurableTask.toJson`.
  /// Nested lists and maps cannot be modified through this record.
  final Map<String, dynamic> payload;

  /// Current lifecycle state.
  final TaskStatus status;

  /// Attempts that have started. An interrupted `running` task already counts.
  final int attempts;

  /// Retry configuration captured at enqueue time.
  final RetryPolicy retryPolicy;

  /// When the task was first stored, according to the queue clock.
  final DateTime createdAt;

  /// Enqueue order. Lower values were stored first.
  ///
  /// Tasks created in the same clock tick stay in this order.
  final int sequence;

  /// When the record was last changed, according to the queue clock.
  final DateTime updatedAt;

  /// Earliest time a `retryScheduled` task may start.
  final DateTime? nextAttemptAt;

  /// Key used to collapse active duplicates. Null when deduplication is off.
  final String? deduplicationKey;

  /// Application-defined idempotency key. The queue stores it and does not
  /// interpret it.
  final String? idempotencyKey;

  /// Latest attempt failure, if any.
  final TaskFailure? lastFailure;

  /// Whether cancellation was requested while the task was running.
  final bool cancelRequested;

  /// Returns a copy with the given fields replaced.
  ///
  /// Nullable fields use a sentinel so they can be cleared. Pass `null` to
  /// clear [nextAttemptAt], [deduplicationKey], [idempotencyKey], or
  /// [lastFailure].
  StoredTask copyWith({
    String? id,
    String? type,
    Map<String, dynamic>? payload,
    TaskStatus? status,
    int? attempts,
    RetryPolicy? retryPolicy,
    DateTime? createdAt,
    DateTime? updatedAt,
    int? sequence,
    Object? nextAttemptAt = _unset,
    Object? deduplicationKey = _unset,
    Object? idempotencyKey = _unset,
    Object? lastFailure = _unset,
    bool? cancelRequested,
  }) {
    return StoredTask(
      id: id ?? this.id,
      type: type ?? this.type,
      payload: payload ?? this.payload,
      status: status ?? this.status,
      attempts: attempts ?? this.attempts,
      retryPolicy: retryPolicy ?? this.retryPolicy,
      createdAt: createdAt ?? this.createdAt,
      updatedAt: updatedAt ?? this.updatedAt,
      sequence: sequence ?? this.sequence,
      nextAttemptAt: identical(nextAttemptAt, _unset)
          ? this.nextAttemptAt
          : nextAttemptAt as DateTime?,
      deduplicationKey: identical(deduplicationKey, _unset)
          ? this.deduplicationKey
          : deduplicationKey as String?,
      idempotencyKey: identical(idempotencyKey, _unset)
          ? this.idempotencyKey
          : idempotencyKey as String?,
      lastFailure: identical(lastFailure, _unset)
          ? this.lastFailure
          : lastFailure as TaskFailure?,
      cancelRequested: cancelRequested ?? this.cancelRequested,
    );
  }

  /// Serializes this record.
  Map<String, dynamic> toJson() => {
    'id': id,
    'type': type,
    'payload': payload,
    'status': status.name,
    'attempts': attempts,
    'retryPolicy': retryPolicy.toJson(),
    'createdAt': createdAt.toIso8601String(),
    'updatedAt': updatedAt.toIso8601String(),
    'sequence': sequence,
    'nextAttemptAt': nextAttemptAt?.toIso8601String(),
    'deduplicationKey': deduplicationKey,
    'idempotencyKey': idempotencyKey,
    'lastFailure': lastFailure?.toJson(),
    'cancelRequested': cancelRequested,
  };

  /// Restores a record produced by [toJson].
  factory StoredTask.fromJson(Map<String, dynamic> json) {
    final statusName = _requireString(json, 'status');
    final status = TaskStatus.values.where((value) => value.name == statusName);
    if (status.isEmpty) {
      throw FormatException('Unknown task status "$statusName"');
    }
    final policy = json['retryPolicy'];
    if (policy is! Map) {
      throw FormatException('StoredTask.retryPolicy must be an object');
    }
    final failure = json['lastFailure'];
    return StoredTask(
      id: _requireString(json, 'id'),
      type: _requireString(json, 'type'),
      payload: _requireMap(json, 'payload'),
      status: status.single,
      attempts: _requireInt(json, 'attempts'),
      retryPolicy: RetryPolicy.fromJson(Map<String, dynamic>.from(policy)),
      createdAt: _requireDateTime(json, 'createdAt'),
      updatedAt: _requireDateTime(json, 'updatedAt'),
      sequence: _requireInt(json, 'sequence'),
      nextAttemptAt: _optionalDateTime(json, 'nextAttemptAt'),
      deduplicationKey: _optionalString(json, 'deduplicationKey'),
      idempotencyKey: _optionalString(json, 'idempotencyKey'),
      lastFailure: failure == null
          ? null
          : TaskFailure.fromJson(_requireObject(failure, 'lastFailure')),
      cancelRequested: _requireBool(json, 'cancelRequested'),
    );
  }

  @override
  String toString() => 'StoredTask($id, $type, $status, attempts: $attempts)';
}

/// Oldest [StoredTask.createdAt], then [StoredTask.sequence], then id.
int compareStoredTasks(StoredTask a, StoredTask b) {
  final byTime = a.createdAt.compareTo(b.createdAt);
  if (byTime != 0) return byTime;
  final bySequence = a.sequence.compareTo(b.sequence);
  if (bySequence != 0) return bySequence;
  return a.id.compareTo(b.id);
}

String _requireString(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value is String && value.isNotEmpty) return value;
  throw FormatException('StoredTask.$key must be a non-empty string');
}

String? _optionalString(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value == null) return null;
  if (value is String) return value;
  throw FormatException('StoredTask.$key must be a string or null');
}

int _requireInt(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value is int) return value;
  throw FormatException('StoredTask.$key must be an int');
}

bool _requireBool(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value is bool) return value;
  throw FormatException('StoredTask.$key must be a bool');
}

DateTime _requireDateTime(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value is! String) {
    throw FormatException('StoredTask.$key must be an ISO-8601 string');
  }
  return DateTime.parse(value);
}

DateTime? _optionalDateTime(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value == null) return null;
  if (value is! String) {
    throw FormatException('StoredTask.$key must be an ISO-8601 string or null');
  }
  return DateTime.parse(value);
}

Map<String, dynamic> _requireMap(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value is Map) return Map<String, dynamic>.from(value);
  throw FormatException('StoredTask.$key must be an object');
}

Map<String, dynamic> _requireObject(Object? value, String key) {
  if (value is Map) return Map<String, dynamic>.from(value);
  throw FormatException('StoredTask.$key must be an object');
}

// Snapshot every container: records must not share mutable nested values with
// callers, decoders, or previous attempts.
Map<String, dynamic> _freezeMap(Map<String, dynamic> value) =>
    value is _FrozenPayload
    ? value
    : _FrozenPayload(
        value.map((key, value) => MapEntry(key, _freezeValue(value))),
      );

Object? _freezeValue(Object? value) {
  if (value is Map) return _freezeMap(Map<String, dynamic>.from(value));
  if (value is List) return List<Object?>.unmodifiable(value.map(_freezeValue));
  return value;
}

// This marker lets copyWith share an already immutable payload without copying
// a potentially large task on every lifecycle transition.
final class _FrozenPayload extends UnmodifiableMapView<String, dynamic> {
  _FrozenPayload(super.map);
}
