import 'package:durable_queue/durable_queue.dart';

/// Builds a stored record for recovery and storage tests.
StoredTask storedTask({
  required String id,
  String type = 'value',
  Map<String, dynamic>? payload,
  TaskStatus status = TaskStatus.pending,
  int attempts = 0,
  RetryPolicy? retryPolicy,
  DateTime? createdAt,
  DateTime? updatedAt,
  int sequence = 0,
  DateTime? nextAttemptAt,
  String? deduplicationKey,
  String? idempotencyKey,
  TaskFailure? lastFailure,
  bool cancelRequested = false,
  int priority = 0,
  List<String> dependsOn = const [],
  DependencyFailurePolicy onDependencyFailure = DependencyFailurePolicy.cancel,
  String? group,
}) {
  final created = createdAt ?? DateTime.utc(2026, 1, 1);
  return StoredTask(
    id: id,
    type: type,
    payload: payload ?? {'value': id},
    status: status,
    attempts: attempts,
    retryPolicy: retryPolicy ?? RetryPolicy.none(),
    createdAt: created,
    updatedAt: updatedAt ?? created,
    sequence: sequence,
    nextAttemptAt: nextAttemptAt,
    deduplicationKey: deduplicationKey,
    idempotencyKey: idempotencyKey,
    lastFailure: lastFailure,
    cancelRequested: cancelRequested,
    priority: priority,
    dependsOn: dependsOn,
    onDependencyFailure: onDependencyFailure,
    group: group,
  );
}
