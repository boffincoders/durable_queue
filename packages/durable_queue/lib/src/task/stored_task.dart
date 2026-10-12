/// @docImport '../storage/queue_storage.dart';
library;

import 'dart:collection';

import '../retry/retry_policy.dart';
import 'dependency_failure_policy.dart';
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
    this.priority = 0,
    Iterable<String> dependsOn = const [],
    this.onDependencyFailure = DependencyFailurePolicy.cancel,
    this.group,
  }) : payload = _freezeMap(payload),
       dependsOn = List<String>.unmodifiable(LinkedHashSet.of(dependsOn)) {
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
    for (final dependency in this.dependsOn) {
      if (dependency.isEmpty) {
        throw ArgumentError.value(
          dependency,
          'dependsOn',
          'Must not contain an empty id',
        );
      }
      if (dependency == id) {
        throw ArgumentError.value(
          dependency,
          'dependsOn',
          'A task cannot depend on itself',
        );
      }
    }
    if (group != null && group!.isEmpty) {
      throw ArgumentError.value(group, 'group', 'Must not be empty');
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

  /// Scheduling priority. Higher values start first among eligible tasks.
  ///
  /// Defaults to zero. Tasks with equal priority keep enqueue order.
  final int priority;

  /// Ids of tasks that must finish before this one may start.
  ///
  /// Unmodifiable, without duplicates, in the order given at enqueue.
  final List<String> dependsOn;

  /// What happens to this task if a dependency does not complete.
  final DependencyFailurePolicy onDependencyFailure;

  /// Optional application label used to query or cancel related tasks.
  final String? group;

  /// Returns a copy with the given fields replaced.
  ///
  /// Nullable fields use a sentinel so they can be cleared. Pass `null` to
  /// clear [nextAttemptAt], [deduplicationKey], [idempotencyKey],
  /// [lastFailure], or [group].
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
    int? priority,
    Iterable<String>? dependsOn,
    DependencyFailurePolicy? onDependencyFailure,
    Object? group = _unset,
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
      priority: priority ?? this.priority,
      dependsOn: dependsOn ?? this.dependsOn,
      onDependencyFailure: onDependencyFailure ?? this.onDependencyFailure,
      group: identical(group, _unset) ? this.group : group as String?,
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
    'priority': priority,
    'dependsOn': dependsOn,
    'onDependencyFailure': onDependencyFailure.name,
    'group': group,
  };

  /// Restores a record produced by [toJson].
  ///
  /// Records written before 0.3.0 have no `priority`, `dependsOn`,
  /// `onDependencyFailure`, or `group` keys. They load with priority zero, no
  /// dependencies, [DependencyFailurePolicy.cancel], and no group.
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
      priority: _optionalInt(json, 'priority') ?? 0,
      dependsOn: _optionalStringList(json, 'dependsOn'),
      onDependencyFailure: _dependencyPolicy(json),
      group: _optionalString(json, 'group'),
    );
  }

  @override
  String toString() => 'StoredTask($id, $type, $status, attempts: $attempts)';
}

/// Scheduling order used by [QueueStorage.getNextReady].
///
/// Highest [StoredTask.priority] first, then [compareStoredTasks].
int compareReadyTasks(StoredTask a, StoredTask b) {
  final byPriority = b.priority.compareTo(a.priority);
  if (byPriority != 0) return byPriority;
  return compareStoredTasks(a, b);
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

int? _optionalInt(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value == null) return null;
  if (value is int) return value;
  throw FormatException('StoredTask.$key must be an int or null');
}

List<String> _optionalStringList(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value == null) return const [];
  if (value is List && value.every((item) => item is String)) {
    return value.cast<String>();
  }
  throw FormatException('StoredTask.$key must be a list of strings or null');
}

DependencyFailurePolicy _dependencyPolicy(Map<String, dynamic> json) {
  final value = json['onDependencyFailure'];
  if (value == null) return DependencyFailurePolicy.cancel;
  for (final policy in DependencyFailurePolicy.values) {
    if (policy.name == value) return policy;
  }
  throw FormatException('Unknown dependency failure policy "$value"');
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
