import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

/// Minimal task used by the engine tests.
final class ValueTask extends DurableTask {
  ValueTask(this.value);

  final String value;

  @override
  String get type => 'value';

  @override
  Map<String, dynamic> toJson() => {'value': value};

  static ValueTask fromJson(Map<String, dynamic> json) {
    return ValueTask(json['value'] as String);
  }
}

/// A [ValueTask] whose decoded type string does not match its registration.
final class RenamedValueTask extends ValueTask {
  RenamedValueTask() : super('renamed');

  @override
  String get type => 'renamed';
}

/// Failure used to exercise retry policies.
final class TemporaryFailure implements Exception {
  @override
  String toString() => 'TemporaryFailure';
}

/// Queue, fake clock, and captured events for one test.
final class QueueHarness {
  QueueHarness({
    QueueStorage? storage,
    FakeQueueClock? clock,
    int maxConcurrentTasks = 1,
    double Function()? randomFraction,
    String Function()? idGenerator,
  }) : storage = storage ?? MemoryQueueStorage(),
       clock = clock ?? FakeQueueClock(DateTime.utc(2026, 1, 1)) {
    queue = DurableQueue(
      storage: this.storage,
      clock: this.clock,
      maxConcurrentTasks: maxConcurrentTasks,
      randomFraction: randomFraction ?? () => 0.5,
      idGenerator: idGenerator,
    );
    queue.events.listen(events.add);
  }

  final QueueStorage storage;
  final FakeQueueClock clock;
  late final DurableQueue queue;
  final List<QueueEvent> events = [];

  void register({
    String type = 'value',
    TaskDecoder<ValueTask>? decoder,
    TaskHandler<ValueTask>? handler,
    RetryPredicate? retryIf,
  }) {
    queue.register<ValueTask>(
      type: type,
      decoder: decoder ?? ValueTask.fromJson,
      handler: handler ?? (task, context) async {},
      retryIf: retryIf,
    );
  }

  List<T> of<T>() => events.whereType<T>().toList();

  Future<void> until(bool Function() condition) async {
    for (var attempt = 0; attempt < 40; attempt++) {
      if (condition()) return;
      await Future<void>.delayed(Duration.zero);
    }
    fail('Condition was not met. Events: $events');
  }
}

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

/// Reloads [source] through JSON into a new memory store.
Future<MemoryQueueStorage> reloadStorage(QueueStorage source) async {
  final tasks = await source.getAll();
  final copy = MemoryQueueStorage();
  for (final task in tasks) {
    await copy.save(StoredTask.fromJson(task.toJson()));
  }
  return copy;
}
