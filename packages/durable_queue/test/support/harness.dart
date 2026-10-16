import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

export 'package:durable_queue_test_support/durable_queue_test_support.dart'
    show storedTask;

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
    RetryDelayResolver? retryDelay,
    Set<String> requires = const {},
    RateLimit? rateLimit,
    DeferOnError? deferOn,
  }) {
    queue.register<ValueTask>(
      type: type,
      decoder: decoder ?? ValueTask.fromJson,
      handler: handler ?? (task, context) async {},
      retryIf: retryIf,
      retryDelay: retryDelay,
      requires: requires,
      rateLimit: rateLimit,
      deferOn: deferOn,
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

/// Reloads [source] through JSON into a new memory store.
Future<MemoryQueueStorage> reloadStorage(QueueStorage source) async {
  final tasks = await source.getAll();
  final copy = MemoryQueueStorage();
  for (final task in tasks) {
    await copy.save(StoredTask.fromJson(task.toJson()));
  }
  return copy;
}
