@TestOn('vm')
library;

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:test/test.dart';

final class _Note extends DurableTask {
  _Note(this.text);

  final String text;

  @override
  String get type => 'note';

  @override
  Map<String, dynamic> toJson() => {'text': text};
}

void main() {
  late Database db;
  late SqliteQueueStorage storage;
  late DurableQueue queue;
  late List<String> seen;

  setUp(() {
    db = sqlite3.openInMemory();
    storage = SqliteQueueStorage.fromDatabase(db);
    seen = [];
    queue = DurableQueue(storage: storage)
      ..register<_Note>(
        type: 'note',
        decoder: (json) => _Note(json['text'] as String),
        handler: (task, context) async => seen.add(task.text),
      );
  });

  tearDown(() async {
    await queue.close();
    await storage.close();
    db.close();
  });

  void damage(String id) => db.execute(
    "UPDATE durable_queue_tasks SET record = '{not json' WHERE id = ?",
    [id],
  );

  test('a damaged row is set aside and the tasks behind it run', () async {
    final setAside = <String>[];
    storage.tasksSetAside.listen(setAside.add);
    final errors = <QueueStorageFailure>[];
    queue.storageErrors.listen(errors.add);

    final bad = await queue.enqueue(_Note('first'));
    await queue.enqueue(_Note('second'));
    damage(bad);

    final result = await queue.drain().timeout(const Duration(seconds: 5));
    expect(result.completed, 1);
    expect(seen, ['second']);
    expect(errors, isEmpty);
    expect(setAside, [bad]);

    expect(await queue.getTask(bad), isNull);
    expect(await queue.getTasks(), hasLength(1));
    expect(await storage.unreadableTaskIds(), [bad]);
    expect(await storage.getNextReady(DateTime.utc(3000)), isNull);
    // Set aside, not lost: the row is still there.
    expect(db.select('SELECT id FROM durable_queue_tasks'), hasLength(2));

    await storage.delete(bad);
    expect(await storage.unreadableTaskIds(), isEmpty);
  });

  test(
    'a damaged row with the earliest wake time is read from its column',
    () async {
      final bad = await queue.enqueue(
        _Note('later'),
        delay: const Duration(hours: 1),
      );
      damage(bad);
      expect(await storage.getNextWakeAt(), isNotNull);
    },
  );

  test('damaged rows do not use up a limited status query', () async {
    final ids = [for (var i = 0; i < 5; i++) await queue.enqueue(_Note('n$i'))];
    damage(ids[0]);
    damage(ids[1]);
    final limited = await storage.getByStatus(TaskStatus.pending, limit: 2);
    expect(limited.map((t) => t.id), [ids[2], ids[3]]);
    expect(
      (await storage.getByStatus(TaskStatus.pending, limit: 10)).length,
      3,
    );
  });
}
