import 'dart:io';

import 'package:drift/drift.dart' hide isNotNull, isNull;
import 'package:drift/native.dart';
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_drift/durable_queue_drift.dart';
import 'package:durable_queue_drift/src/sql.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:durable_queue_test_support/durable_queue_test_support.dart';
import 'package:test/test.dart';

import 'support.dart';

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
  late Directory directory;
  late String path;

  setUp(() {
    directory = Directory.systemTemp.createTempSync('dq_drift_');
    path = '${directory.path}/app.db';
  });

  tearDown(() => directory.deleteSync(recursive: true));

  TestDatabase openFile() => TestDatabase(NativeDatabase(File(path)));

  test('a queued task survives closing and reopening the app', () async {
    var db = openFile();
    var queue = DurableQueue(storage: await DriftQueueStorage.open(db))
      ..register<Note>(
        type: 'note',
        decoder: Note.fromJson,
        handler: (task, context) async {},
      );
    final id = await queue.enqueue(
      Note('hello'),
      idempotencyKey: 'k',
      priority: 3,
      group: 'notes',
    );
    await queue.close();
    await db.close();

    db = openFile();
    addTearDown(db.close);
    final ran = <String>[];
    queue = DurableQueue(storage: await DriftQueueStorage.open(db))
      ..register<Note>(
        type: 'note',
        decoder: Note.fromJson,
        handler: (task, context) async => ran.add(task.text),
      );
    addTearDown(queue.close);
    final stored = (await queue.getTask(id))!;
    expect(stored.priority, 3);
    expect(stored.group, 'notes');
    expect(stored.idempotencyKey, 'k');
    await queue.start();
    await queue.waitFor(id);
    expect(ran, ['hello']);
  });

  test('the tables are the ones durable_queue_sqlite uses', () async {
    final db = openFile();
    final storage = await DriftQueueStorage.open(db);
    await storage.save(storedTask(id: 'a', group: 'g'));
    await db.close();

    final sqlite = SqliteQueueStorage.open(path);
    addTearDown(sqlite.close);
    expect((await sqlite.get('a'))?.group, 'g');
    await sqlite.save(storedTask(id: 'b', sequence: 1));
    await sqlite.close();

    final again = openFile();
    addTearDown(again.close);
    final reopened = await DriftQueueStorage.open(again);
    expect((await reopened.getAll()).map((t) => t.id), ['a', 'b']);
  });

  test('calls inside an app transaction are refused, not deadlocked', () async {
    final db = TestDatabase.memory();
    addTearDown(db.close);
    await db.customStatement('CREATE TABLE orders (id TEXT)');
    final storage = await DriftQueueStorage.open(db);
    final queue = DurableQueue(storage: storage)
      ..register<Note>(
        type: 'note',
        decoder: Note.fromJson,
        handler: (task, context) async {},
      );
    addTearDown(queue.close);
    for (var i = 0; i < 20; i++) {
      await queue.enqueue(Note('$i'));
    }
    await queue.start();

    await expectLater(
      db.transaction(() async {
        await db.customStatement("INSERT INTO orders VALUES ('1')");
        await queue.enqueue(Note('order 1'));
      }).timeout(const Duration(seconds: 10)),
      throwsA(
        isA<StateError>().having(
          (e) => e.message,
          'message',
          contains('inside a drift transaction'),
        ),
      ),
    );
    expect(
      db.transaction(() => storage.get('x')),
      throwsA(isA<StateError>()),
    );

    // After the transaction, enqueue works and the queue keeps running.
    await db.customStatement("INSERT INTO orders VALUES ('1')");
    final id = await queue.enqueue(Note('order 1'));
    await queue.waitFor(id, timeout: const Duration(seconds: 10));
  });

  test('the app database keeps working beside the queue tables', () async {
    final db = TestDatabase.memory();
    addTearDown(db.close);
    await db.customStatement('CREATE TABLE notes (body TEXT)');
    final storage = await DriftQueueStorage.open(db);
    await db.customStatement("INSERT INTO notes VALUES ('x')");
    await storage.save(storedTask(id: 'a'));
    final rows = await db.customSelect('SELECT body FROM notes').get();
    expect(rows.single.read<String>('body'), 'x');
  });

  test('prefixes keep queues apart', () async {
    final db = TestDatabase.memory();
    addTearDown(db.close);
    final a = await DriftQueueStorage.open(db, tablePrefix: 'a_');
    final b = await DriftQueueStorage.open(db, tablePrefix: 'b_');
    await a.save(storedTask(id: 'x'));
    expect(await b.get('x'), isNull);
    expect(
      () => DriftQueueStorage.open(db, tablePrefix: 'bad-prefix'),
      throwsArgumentError,
    );
  });

  test('refuses tables written by a newer version', () async {
    final db = TestDatabase.memory();
    addTearDown(db.close);
    await DriftQueueStorage.open(db);
    await db.customStatement(
      "UPDATE durable_queue_meta SET value = '99' WHERE key = 'schema'",
    );
    expect(DriftQueueStorage.open(db), throwsStateError);
  });

  test('opening twice keeps existing data', () async {
    final db = TestDatabase.memory();
    addTearDown(db.close);
    await (await DriftQueueStorage.open(db)).save(storedTask(id: 'a'));
    expect(await (await DriftQueueStorage.open(db)).get('a'), isNotNull);
  });

  test('a closed storage rejects calls but leaves the database open', () async {
    final db = TestDatabase.memory();
    addTearDown(db.close);
    final storage = await DriftQueueStorage.open(db);
    await storage.close();
    expect(storage.isClosed, isTrue);
    expect(() => storage.get('a'), throwsStateError);
    expect(() => storage.save(storedTask(id: 'a')), throwsStateError);
    await db.customStatement('SELECT 1');
  });

  test('worker queries use an index and need no sort', () async {
    final db = TestDatabase.memory();
    addTearDown(db.close);
    await DriftQueueStorage.open(db);
    final sql = QueueSql(DriftQueueStorage.defaultTablePrefix);
    Future<String> plan(String query, List<Variable> variables) async {
      final rows = await db
          .customSelect('EXPLAIN QUERY PLAN $query', variables: variables)
          .get();
      return rows.map((r) => r.read<String>('detail')).join(' | ');
    }

    for (final (query, variables) in [
      (sql.selectNextReady, [Variable<int>(0)]),
      (sql.selectNextWake, <Variable>[]),
      (sql.selectActiveByKey, [Variable<String>('k')]),
      (sql.selectByGroup, [Variable<String>('g')]),
    ]) {
      final detail = await plan(query, variables);
      expect(detail, contains('USING INDEX'), reason: query);
      expect(detail, isNot(contains('TEMP B-TREE')), reason: query);
    }
  });
}
