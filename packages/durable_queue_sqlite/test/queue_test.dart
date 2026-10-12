import 'dart:async';
import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:test/test.dart';

final class StepTask extends DurableTask {
  StepTask(this.name);

  final String name;

  @override
  String get type => 'step';

  @override
  Map<String, dynamic> toJson() => {'name': name};

  static StepTask fromJson(Map<String, dynamic> json) =>
      StepTask(json['name'] as String);
}

/// One "launch" of an app: a storage on the database file plus a queue.
final class Launch {
  Launch(
    String path, {
    Future<void> Function(StepTask task, TaskContext context)? handler,
  }) : storage = SqliteQueueStorage.open(path) {
    queue = DurableQueue(
      storage: storage,
      clock: clock,
      randomFraction: () => 0.5,
    );
    queue.register<StepTask>(
      type: 'step',
      decoder: StepTask.fromJson,
      handler: (task, context) async {
        ran.add(task.name);
        await handler?.call(task, context);
      },
    );
    queue.events.listen(events.add);
  }

  final SqliteQueueStorage storage;
  final clock = FakeQueueClock(DateTime.utc(2026, 1, 1));
  late final DurableQueue queue;
  final ran = <String>[];
  final events = <QueueEvent>[];

  Future<void> until(bool Function() condition) async {
    for (var i = 0; i < 200 && !condition(); i++) {
      await Future<void>.delayed(Duration.zero);
    }
    expect(condition(), isTrue, reason: 'events: $events');
  }
}

void main() {
  late Directory directory;
  late String path;

  setUp(() {
    directory = Directory.systemTemp.createTempSync('dq_sqlite_queue_');
    path = '${directory.path}/queue.db';
  });

  tearDown(() => directory.deleteSync(recursive: true));

  test('queued work survives a restart and keeps its order', () async {
    final first = Launch(path);
    await first.queue.enqueue(StepTask('low'), priority: -1);
    await first.queue.enqueueChain([
      StepTask('upload'),
      StepTask('resize'),
    ], group: 'photo');
    await first.queue.enqueue(StepTask('urgent'), priority: 5);
    await first.queue.close();
    await first.storage.close();

    final second = Launch(path);
    await second.queue.start();
    await second.until(() => second.ran.length == 4);

    expect(second.ran, ['urgent', 'upload', 'resize', 'low']);
    final photo = await second.queue.getTasks(group: 'photo');
    expect(photo.map((task) => task.status).toSet(), {TaskStatus.completed});
    await second.queue.close();
    await second.storage.close();
  });

  test(
    'a task interrupted by process death is recovered and retried',
    () async {
      final never = Completer<void>();
      final crashed = Launch(path, handler: (task, context) => never.future);
      final id = await crashed.queue.enqueue(
        StepTask('upload'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 3,
          delay: const Duration(seconds: 10),
        ),
      );
      await crashed.queue.start();
      await crashed.until(() => crashed.ran.isNotEmpty);
      // The process "dies" here: no stop, no close, the record stays running.

      final next = Launch(path);
      expect((await next.storage.get(id))?.status, TaskStatus.running);
      await next.queue.start();

      final recovered = (await next.storage.get(id))!;
      expect(recovered.status, TaskStatus.retryScheduled);
      expect(recovered.attempts, 1);
      expect(
        recovered.lastFailure?.error,
        contains('TaskInterruptedException'),
      );

      next.clock.advance(const Duration(seconds: 10));
      await next.until(() => next.events.whereType<TaskCompleted>().isNotEmpty);
      final done = (await next.storage.get(id))!;
      expect(done.status, TaskStatus.completed);
      expect(done.attempts, 2);
      await next.queue.close();
      await next.storage.close();
      await crashed.storage.close();
    },
  );

  test('a crash mid-chain settles the rest of the chain on restart', () async {
    final gate = Completer<void>();
    final first = Launch(
      path,
      handler: (task, context) async {
        if (task.name == 'two') await gate.future;
      },
    );
    final ids = await first.queue.enqueueChain([
      StepTask('one'),
      StepTask('two'),
      StepTask('three'),
    ]);
    await first.queue.start();
    await first.until(() => first.ran.contains('two'));
    expect((await first.storage.get(ids[2]))?.status, TaskStatus.waiting);
    // The process dies while step two runs. RetryPolicy.none means the
    // interrupted attempt was its only one.

    final second = Launch(path);
    await second.queue.start();

    expect((await second.storage.get(ids[0]))?.status, TaskStatus.completed);
    expect((await second.storage.get(ids[1]))?.status, TaskStatus.failed);
    final three = (await second.storage.get(ids[2]))!;
    expect(three.status, TaskStatus.cancelled);
    expect(three.lastFailure?.error, contains(ids[1]));
    expect(second.ran, isEmpty);
    await second.queue.close();
    await second.storage.close();
    await first.storage.close();
  });

  test('purge and retry work on SQLite', () async {
    var badCalls = 0;
    final launch = Launch(
      path,
      handler: (task, context) async {
        // Fails the first time only; `retry` resets the attempt counter.
        if (task.name == 'bad' && ++badCalls == 1) throw StateError('boom');
      },
    );
    final bad = await launch.queue.enqueue(StepTask('bad'));
    await launch.queue.enqueue(StepTask('good'));
    await launch.queue.start();
    await launch.until(
      () =>
          launch.events.whereType<TaskFailed>().isNotEmpty &&
          launch.events.whereType<TaskCompleted>().isNotEmpty,
    );

    expect(await launch.queue.purge(), 1);
    await launch.queue.retry(bad);
    await launch.until(
      () => launch.events.whereType<TaskCompleted>().length == 2,
    );
    expect(await launch.queue.getTasks(), hasLength(1));
    await launch.queue.close();
    await launch.storage.close();
  });
}
