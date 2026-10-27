@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_hive/durable_queue_hive.dart';
import 'package:durable_queue_test_support/durable_queue_test_support.dart';
import 'package:hive_ce/hive_ce.dart';
import 'package:test/test.dart';

import 'support/open_box.dart';

void main() {
  late HiveFixture hive;

  setUp(() => hive = HiveFixture());
  tearDown(() => hive.dispose());

  StoredTask task(
    String id, {
    TaskStatus status = TaskStatus.pending,
    int sequence = 0,
    String? group,
    String? deduplicationKey,
    int priority = 0,
  }) => storedTask(
    id: id,
    status: status,
    sequence: sequence,
    group: group,
    deduplicationKey: deduplicationKey,
    priority: priority,
    payload: {
      'text': 'hello $id',
      'nested': {
        'list': [1, 2.5, 'x', null, true],
      },
    },
  );

  test('tasks and their order survive closing and reopening the box', () async {
    final name = hive.nextName();
    var storage = await hive.open(name);
    final tasks = [
      task('c', sequence: 3, group: 'photos'),
      task('a', sequence: 1, deduplicationKey: 'k', priority: 5),
      task('b', sequence: 2, status: TaskStatus.failed),
    ];
    for (final t in tasks) {
      await storage.save(t);
    }
    await storage.update(tasks[0].copyWith(attempts: 2));
    await storage.close();

    storage = await hive.open(name);
    expect((await storage.getAll()).map((t) => t.id), ['a', 'b', 'c']);
    expect((await storage.get('c'))!.attempts, 2);
    expect((await storage.get('a'))!.toJson(), tasks[1].toJson());
    expect((await storage.findActiveByDeduplicationKey('k'))?.id, 'a');
    expect((await storage.getByGroup('photos')).single.id, 'c');
    expect((await storage.getNextReady(DateTime.utc(2030)))?.id, 'a');
    expect(await storage.getMaxSequence(), 3);
  });

  test(
    'each task is one entry: key is the id, value the record JSON',
    () async {
      final name = hive.nextName();
      final storage = await hive.open(name);
      await storage.save(task('a'));
      expect(storage.box.keys.toList(), ['a']);
      expect(
        jsonDecode(storage.box.get('a')!),
        jsonDecode(jsonEncode(task('a').toJson())),
      );
      await storage.delete('a');
      expect(storage.box.isEmpty, isTrue);
    },
  );

  test('ids longer than 255 bytes are stored under a hash', () async {
    final name = hive.nextName();
    var storage = await hive.open(name);
    final long = 'x' * 300;
    final unicode = 'é' * 200; // 400 bytes of UTF-8
    final short = 'é' * 100; // 200 bytes: stored as is
    for (final id in [long, unicode, short]) {
      await storage.save(task(id));
    }
    expect(storage.box.keys.toSet(), {
      HiveQueueStorage.boxKey(long),
      HiveQueueStorage.boxKey(unicode),
      short,
    });
    expect(HiveQueueStorage.boxKey(long), isNot(long));
    expect(
      HiveQueueStorage.boxKey(long),
      isNot(HiveQueueStorage.boxKey(unicode)),
    );
    await storage.close();

    storage = await hive.open(name);
    expect((await storage.get(long))!.id, long);
    expect((await storage.get(unicode))!.id, unicode);
    await storage.delete(long);
    await storage.close();
    storage = await hive.open(name);
    expect(await storage.get(long), isNull);
    expect((await storage.getAll()).length, 2);
  });

  test('a write cut off at the end of the file is dropped on open; earlier '
      'tasks are intact', () async {
    final name = hive.nextName();
    var storage = await hive.open(name);
    for (var i = 0; i < 20; i++) {
      await storage.save(task('t$i', sequence: i));
    }
    await storage.close();
    final file = hive.file(name);
    final length = file.lengthSync();
    // Cut the last record in half, as a crash in the middle of a write would.
    final raf = file.openSync(mode: FileMode.append)..truncateSync(length - 40);
    raf.closeSync();

    storage = await hive.open(name);
    final ids = (await storage.getAll()).map((t) => t.id).toList();
    expect(ids, [for (var i = 0; i < 19; i++) 't$i']);
    await storage.save(task('t19', sequence: 19));
    await storage.close();
    storage = await hive.open(name);
    expect((await storage.getAll()).length, 20);
  });

  test('a box holding something other than task records is refused', () async {
    final name = hive.nextName();
    final raw = await hive.openRaw(name);
    await raw.put('setting', 'dark mode');
    await raw.close();
    expect(hive.open(name), throwsFormatException);
  });

  test('an entry stored under another task\'s key is refused', () async {
    final name = hive.nextName();
    final raw = await hive.openRaw(name);
    await raw.put('b', jsonEncode(task('a').toJson()));
    await raw.close();
    expect(hive.open(name), throwsFormatException);
  });

  test('a failed write is undone in memory', () async {
    final storage = await hive.open(hive.nextName());
    await storage.save(task('a'));
    await storage.close(); // every write now fails

    await expectLater(storage.save(task('b')), throwsA(anything));
    expect(await storage.get('b'), isNull);
    expect(await storage.getPending(), hasLength(1));

    await expectLater(
      storage.update(task('a', status: TaskStatus.running)),
      throwsA(anything),
    );
    expect((await storage.get('a'))!.status, TaskStatus.pending);
    expect(await storage.getByStatus(TaskStatus.running), isEmpty);

    await expectLater(storage.delete('a'), throwsA(anything));
    expect((await storage.get('a'))?.id, 'a');
  });

  test('a duplicate id is refused without writing', () async {
    final storage = await hive.open(hive.nextName());
    await storage.save(task('a', group: 'one'));
    await expectLater(storage.save(task('a', group: 'two')), throwsStateError);
    expect((await storage.get('a'))!.group, 'one');
    expect(storage.box.length, 1);
  });

  test('flushEachWrite flushes after every write, and only then', () async {
    for (final flush in [true, false]) {
      final box = _CountingBox(await hive.openRaw(hive.nextName()));
      final storage = HiveQueueStorage(box, flushEachWrite: flush);
      await storage.save(task('a'));
      await storage.update(task('a', status: TaskStatus.running));
      await storage.delete('a');
      expect(box.flushes, flush ? 3 : 0);
      expect(box.writes, 3);
    }
  });

  test('without flushing, writes still reach the box', () async {
    final name = hive.nextName();
    var storage = await hive.open(name, flushEachWrite: false);
    await storage.save(task('a'));
    await storage.close();
    storage = await hive.open(name);
    expect((await storage.get('a'))?.id, 'a');
  });

  test('a payload JSON cannot encode is refused', () async {
    final storage = await hive.open(hive.nextName());
    await expectLater(
      storage.save(storedTask(id: 'a', payload: {'when': DateTime(2026)})),
      throwsArgumentError,
    );
    expect(await storage.get('a'), isNull);
    expect(storage.box.isEmpty, isTrue);
  });

  test('a queue keeps its work across a restart', () async {
    final name = hive.nextName();
    final seen = <String>[];

    DurableQueue build(QueueStorage storage) =>
        DurableQueue(storage: storage)..register<_Note>(
          type: _Note.taskType,
          decoder: (json) => _Note(json['text'] as String),
          handler: (note, context) async => seen.add(note.text),
        );

    var storage = await hive.open(name);
    var queue = build(storage);
    final first = await queue.enqueue(const _Note('first'), group: 'notes');
    await queue.enqueue(const _Note('second'), dependsOn: [first]);
    await queue.close();
    await storage.close();

    storage = await hive.open(name);
    queue = build(storage);
    await queue.drain();
    expect(seen, ['first', 'second']);
    final done = await queue.getTasks(status: TaskStatus.completed);
    expect(done, hasLength(2));
    await queue.close();
    await storage.close();

    storage = await hive.open(name);
    expect((await storage.getByStatus(TaskStatus.completed)).length, 2);
  });
}

final class _Note extends DurableTask {
  const _Note(this.text);

  static const taskType = 'note';

  final String text;

  @override
  String get type => taskType;

  @override
  Map<String, dynamic> toJson() => {'text': text};
}

/// Counts writes and flushes on a real box.
final class _CountingBox implements Box<String> {
  _CountingBox(this._box);

  final Box<String> _box;
  var flushes = 0;
  var writes = 0;

  @override
  String get name => _box.name;
  @override
  Iterable<dynamic> get keys => _box.keys;
  @override
  String? get(dynamic key, {String? defaultValue}) =>
      _box.get(key, defaultValue: defaultValue);
  @override
  Future<void> put(dynamic key, String value) {
    writes++;
    return _box.put(key, value);
  }

  @override
  Future<void> delete(dynamic key) {
    writes++;
    return _box.delete(key);
  }

  @override
  Future<void> flush() {
    flushes++;
    return _box.flush();
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
