import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_flutter/durable_queue_flutter.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/delegating_storage.dart';

final class Note extends DurableTask {
  Note(this.text);

  final String text;

  @override
  String get type => 'note';

  @override
  Map<String, dynamic> toJson() => {'text': text};

  static Note fromJson(Map<String, dynamic> json) =>
      Note(json['text'] as String);
}

void main() {
  late FakeQueueClock clock;
  late DurableQueue queue;
  late Completer<void> release;

  setUp(() {
    clock = FakeQueueClock(DateTime.utc(2026, 10, 20));
    release = Completer<void>();
    queue = DurableQueue(storage: MemoryQueueStorage(), clock: clock)
      ..register<Note>(
        type: 'note',
        decoder: Note.fromJson,
        handler: (task, context) async {
          if (task.text == 'slow') await release.future;
          if (task.text == 'fail') throw StateError('nope');
        },
      );
  });

  tearDown(() async {
    if (!release.isCompleted) release.complete();
    await queue.close();
  });

  group('watchTask', () {
    test('emits the record now and after each change', () async {
      final id = await queue.enqueue(Note('slow'));
      final seen = <TaskStatus?>[];
      final sub = queue.watchTask(id).listen((t) => seen.add(t?.status));
      await pumpEventQueue();
      expect(seen, [TaskStatus.pending]);

      await queue.start();
      await pumpEventQueue();
      expect(seen, [TaskStatus.pending, TaskStatus.running]);

      release.complete();
      await pumpEventQueue();
      expect(seen.last, TaskStatus.completed);
      await sub.cancel();
    });

    test('events for other tasks do not emit', () async {
      final id = await queue.enqueue(Note('a'));
      final seen = <StoredTask?>[];
      final sub = queue.watchTask(id).listen(seen.add);
      await pumpEventQueue();
      await queue.enqueue(Note('b'));
      await queue.enqueue(Note('c'));
      await pumpEventQueue();
      expect(seen, hasLength(1));
      await sub.cancel();
    });

    test('events for other tasks do not even read storage', () async {
      final storage = _CountingStorage();
      final q = DurableQueue(storage: storage)
        ..register<Note>(
          type: 'note',
          decoder: Note.fromJson,
          handler: (task, context) async {},
        );
      final id = await q.enqueue(Note('a'));
      final sub = q.watchTask(id).listen((_) {});
      await pumpEventQueue();
      final before = storage.gets;
      await q.enqueue(Note('b'));
      await q.enqueue(Note('c'));
      await pumpEventQueue();
      expect(storage.gets, before);
      await sub.cancel();
      await q.close();
    });

    test('an unknown id emits null', () async {
      final first = await queue.watchTask('missing').first;
      expect(first, isNull);
    });

    test('ends when the queue is closed', () async {
      final id = await queue.enqueue(Note('a'));
      var done = false;
      queue.watchTask(id).listen((_) {}, onDone: () => done = true);
      await pumpEventQueue();
      await queue.close();
      await pumpEventQueue();
      expect(done, isTrue);
    });

    test('deferral and retry updates are seen', () async {
      final id = await queue.enqueue(
        Note('fail'),
        retryPolicy: RetryPolicy.fixed(
          maxAttempts: 2,
          delay: const Duration(seconds: 5),
        ),
      );
      final seen = <String>[];
      final sub = queue
          .watchTask(id)
          .listen((t) => seen.add('${t!.status.name}/${t.attempts}'));
      await queue.start();
      await pumpEventQueue();
      expect(seen.last, 'retryScheduled/1');
      clock.advance(const Duration(seconds: 5));
      await pumpEventQueue();
      expect(seen.last, 'failed/2');
      await sub.cancel();
    });
  });

  group('holds', () {
    test('watchTask and watchGroup update on hold and release', () async {
      final id = await queue.enqueue(Note('a'), group: 'g');
      final task = <bool?>[];
      final group = <List<bool>>[];
      final subA = queue.watchTask(id).listen((t) => task.add(t?.held));
      final subB = queue
          .watchGroup('g')
          .listen((tasks) => group.add([for (final t in tasks) t.held]));
      await pumpEventQueue();
      await queue.hold(id);
      await pumpEventQueue();
      await queue.release(id);
      await pumpEventQueue();
      expect(task, [false, true, false]);
      expect(group, [
        [false],
        [true],
        [false],
      ]);
      await subA.cancel();
      await subB.cancel();
    });
  });

  group('watchGroup', () {
    test('emits the group now and when tasks join or change', () async {
      await queue.enqueue(Note('a'), group: 'album');
      await queue.enqueue(Note('x'), group: 'other');
      final seen = <List<String>>[];
      final sub = queue
          .watchGroup('album')
          .listen(
            (tasks) => seen.add([
              for (final t in tasks) '${t.payload['text']}:${t.status.name}',
            ]),
          );
      await pumpEventQueue();
      expect(seen, [
        ['a:pending'],
      ]);

      await queue.enqueue(Note('b'), group: 'album');
      await pumpEventQueue();
      expect(seen.last, ['a:pending', 'b:pending']);

      // Changes outside the group do not emit.
      final count = seen.length;
      await queue.enqueue(Note('y'), group: 'other');
      await pumpEventQueue();
      expect(seen, hasLength(count));

      await queue.start();
      await pumpEventQueue();
      expect(seen.last, ['a:completed', 'b:completed']);
      await sub.cancel();
    });

    test('status filters the group', () async {
      await queue.enqueue(Note('a'), group: 'g');
      await queue.enqueue(Note('fail'), group: 'g');
      await queue.start();
      await pumpEventQueue();
      final failed = await queue
          .watchGroup('g', status: TaskStatus.failed)
          .first;
      expect([for (final t in failed) t.payload['text']], ['fail']);
    });

    test('a burst of events causes few reloads', () async {
      final reads = _CountingStorage();
      final q = DurableQueue(storage: reads)
        ..register<Note>(
          type: 'note',
          decoder: Note.fromJson,
          handler: (task, context) async {},
        );
      final sub = q.watchGroup('g').listen((_) {});
      await pumpEventQueue();
      final before = reads.groupReads;
      for (var i = 0; i < 20; i++) {
        unawaited(q.enqueue(Note('$i'), group: 'g'));
      }
      await pumpEventQueue();
      // 20 enqueues, but reloads coalesce while one is in progress.
      expect(reads.groupReads - before, lessThan(20));
      final last = await q.getTasks(group: 'g');
      expect(last, hasLength(20));
      await sub.cancel();
      await q.close();
    });

    test('rejects an empty group', () {
      expect(() => queue.watchGroup(''), throwsArgumentError);
    });
  });
}

final class _CountingStorage extends DelegatingStorage {
  var groupReads = 0;
  var gets = 0;

  @override
  Future<StoredTask?> get(String id) {
    gets++;
    return super.get(id);
  }

  @override
  Future<List<StoredTask>> getByGroup(String group, {TaskStatus? status}) {
    groupReads++;
    return super.getByGroup(group, status: status);
  }
}
