import 'dart:io';
import 'dart:isolate';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:durable_queue_sqlite/src/sql.dart';
import 'package:durable_queue_test_support/durable_queue_test_support.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:test/test.dart';

void main() {
  late Directory directory;
  var files = 0;

  setUp(() {
    directory = Directory.systemTemp.createTempSync('dq_sqlite_');
  });

  tearDown(() {
    directory.deleteSync(recursive: true);
  });

  String nextPath() => '${directory.path}/queue-${files++}.db';

  test('every field survives closing and reopening the file', () async {
    final path = nextPath();
    final original = storedTask(
      id: 'a',
      payload: {
        'path': '/photos/beach.jpg',
        'sizes': [1, 2],
        'meta': {'w': 3},
      },
      status: TaskStatus.retryScheduled,
      attempts: 2,
      retryPolicy: RetryPolicy.exponential(
        maxAttempts: 5,
        initialDelay: const Duration(seconds: 1),
        jitter: true,
      ),
      createdAt: DateTime.utc(2026, 1, 1, 12),
      updatedAt: DateTime.utc(2026, 1, 1, 12, 5),
      sequence: 7,
      nextAttemptAt: DateTime.utc(2026, 1, 1, 12, 6),
      deduplicationKey: 'upload:beach',
      idempotencyKey: 'upload:beach:v1',
      lastFailure: TaskFailure(
        error: 'TimeoutException',
        stackTrace: '#0 main',
        failedAt: DateTime.utc(2026, 1, 1, 12, 5),
        attempt: 2,
      ),
      priority: 3,
      dependsOn: ['x'],
      onDependencyFailure: DependencyFailurePolicy.run,
      group: 'photos',
    );
    final first = SqliteQueueStorage.open(path);
    await first.save(original);
    await first.close();

    final reopened = SqliteQueueStorage.open(path);
    final loaded = await reopened.get('a');
    expect(loaded?.toJson(), original.toJson());
    expect(await reopened.getNextWakeAt(), DateTime.utc(2026, 1, 1, 12, 6));
    expect(await reopened.getMaxSequence(), 7);
    await reopened.close();
  });

  test('local and UTC timestamps keep their time zone', () async {
    final storage = SqliteQueueStorage.inMemory();
    final local = DateTime(2026, 3, 1, 9);
    await storage.save(
      storedTask(
        id: 'a',
        createdAt: local,
        status: TaskStatus.retryScheduled,
        nextAttemptAt: local.add(const Duration(minutes: 1)),
      ),
    );
    final loaded = (await storage.get('a'))!;
    expect(loaded.createdAt, local);
    expect(loaded.createdAt.isUtc, isFalse);
    expect(
      await storage.getNextWakeAt(),
      local.add(const Duration(minutes: 1)),
    );
    await storage.close();
  });

  test('dependency edges follow the waiting status', () async {
    final storage = SqliteQueueStorage.inMemory();
    await storage.save(
      storedTask(id: 'child', status: TaskStatus.waiting, dependsOn: ['p']),
    );
    expect((await storage.getWaitingDependents('p')).single.id, 'child');

    final child = (await storage.get('child'))!;
    await storage.update(child.copyWith(status: TaskStatus.pending));
    expect(await storage.getWaitingDependents('p'), isEmpty);

    await storage.update(child.copyWith(dependsOn: ['q']));
    expect(await storage.getWaitingDependents('p'), isEmpty);
    expect((await storage.getWaitingDependents('q')).single.id, 'child');

    await storage.delete('child');
    expect(await storage.getWaitingDependents('q'), isEmpty);
    await storage.close();
  });

  group('shared database', () {
    test('queues with different prefixes do not see each other', () async {
      final db = sqlite3.openInMemory();
      final uploads = SqliteQueueStorage.fromDatabase(db, tablePrefix: 'up_');
      final sync = SqliteQueueStorage.fromDatabase(db, tablePrefix: 'sync_');
      await uploads.save(storedTask(id: 'a'));

      expect(await uploads.get('a'), isNotNull);
      expect(await sync.get('a'), isNull);
      expect(await sync.getAll(), isEmpty);

      await uploads.close();
      await sync.close();
      // The app still owns the database.
      expect(db.select('SELECT COUNT(*) AS n FROM up_tasks').single['n'], 1);
      db.close();
    });

    test('writes join a transaction the app already started', () async {
      final db = sqlite3.openInMemory();
      final storage = SqliteQueueStorage.fromDatabase(db);
      db.execute('BEGIN');
      await storage.save(storedTask(id: 'a'));
      db.execute('ROLLBACK');
      expect(await storage.get('a'), isNull);
      db.close();
    });

    test('an invalid table prefix is rejected', () {
      final db = sqlite3.openInMemory();
      for (final prefix in ['', '1x', 'a-b', 'a b', 'x;DROP']) {
        expect(
          () => SqliteQueueStorage.fromDatabase(db, tablePrefix: prefix),
          throwsArgumentError,
          reason: prefix,
        );
      }
      db.close();
    });
  });

  group('schema', () {
    test('records its version', () {
      final db = sqlite3.openInMemory();
      SqliteQueueStorage.fromDatabase(db);
      final row = db.select(
        "SELECT value FROM durable_queue_meta WHERE key = 'schema'",
      );
      expect(row.single['value'], '${SqliteQueueStorage.schemaVersion}');
      db.close();
    });

    test('refuses tables written by a newer version', () async {
      final path = nextPath();
      await SqliteQueueStorage.open(path).close();
      final db = sqlite3.open(path);
      db.execute(
        "UPDATE durable_queue_meta SET value = '99' WHERE key = 'schema'",
      );
      db.close();

      expect(() => SqliteQueueStorage.open(path), throwsStateError);
    });

    test('opening twice keeps existing data', () async {
      final path = nextPath();
      final first = SqliteQueueStorage.open(path);
      await first.save(storedTask(id: 'a'));
      await first.close();
      final second = SqliteQueueStorage.open(path);
      expect(await second.getAll(), hasLength(1));
      await second.close();
    });
  });

  test(
    'isolates with their own connection can write at the same time',
    () async {
      final path = nextPath();
      SqliteQueueStorage.open(path).close();
      Future<int> writer(String name) => Isolate.run(() async {
        final storage = SqliteQueueStorage.open(path);
        var written = 0;
        for (var i = 0; i < 300; i++) {
          final id = '$name-$i';
          await storage.save(storedTask(id: id, sequence: i));
          final task = (await storage.get(id))!;
          await storage.update(task.copyWith(attempts: 1));
          written++;
        }
        await storage.close();
        return written;
      });
      final counts = await Future.wait([writer('a'), writer('b'), writer('c')]);
      expect(counts, [300, 300, 300]);
      final storage = SqliteQueueStorage.open(path);
      expect((await storage.getAll()).length, 900);
      await storage.close();
    },
  );

  test('a write that fails part-way leaves no partial record', () async {
    final db = sqlite3.openInMemory();
    final storage = SqliteQueueStorage.fromDatabase(db);
    // Make the second statement of a write fail: the dependency edge insert.
    db.execute(
      'CREATE TRIGGER fail_edge BEFORE INSERT ON durable_queue_waiting_edges '
      "WHEN NEW.dependency_id = 'boom' "
      "BEGIN SELECT RAISE(ABORT, 'edge write failed'); END",
    );

    await expectLater(
      storage.save(
        storedTask(id: 'a', status: TaskStatus.waiting, dependsOn: ['boom']),
      ),
      throwsA(isA<SqliteException>()),
    );
    expect(await storage.get('a'), isNull);

    final original = storedTask(id: 'b', group: 'before');
    await storage.save(original);
    await expectLater(
      storage.update(
        original.copyWith(
          status: TaskStatus.waiting,
          dependsOn: ['boom'],
          group: 'after',
        ),
      ),
      throwsA(isA<SqliteException>()),
    );
    expect((await storage.get('b'))?.toJson(), original.toJson());
    expect(db.autocommit, isTrue);
    db.close();
  });

  test('a corrupt record is reported, not silently skipped', () async {
    final db = sqlite3.openInMemory();
    final storage = SqliteQueueStorage.fromDatabase(db);
    await storage.save(storedTask(id: 'a'));
    db.execute("UPDATE durable_queue_tasks SET record = '[1]'");
    await expectLater(storage.get('a'), throwsA(isA<FormatException>()));
    db.close();
  });

  test('a closed storage rejects calls', () async {
    final storage = SqliteQueueStorage.inMemory();
    await storage.close();
    await storage.close();
    expect(storage.isClosed, isTrue);
    await expectLater(storage.get('a'), throwsStateError);
    await expectLater(storage.save(storedTask(id: 'a')), throwsStateError);
  });

  group('query plans', () {
    late Database db;
    late QueueSql sql;

    setUp(() {
      db = sqlite3.openInMemory();
      SqliteQueueStorage.fromDatabase(db);
      sql = QueueSql(SqliteQueueStorage.defaultTablePrefix);
    });

    tearDown(() => db.close());

    String plan(String query, List<Object?> parameters) => db
        .select('EXPLAIN QUERY PLAN $query', parameters)
        .map((row) => row['detail'] as String)
        .join(' | ');

    void expectIndexed(String query, List<Object?> parameters, String index) {
      final detail = plan(query, parameters);
      expect(detail, contains(index), reason: detail);
      expect(detail, isNot(contains('TEMP B-TREE')), reason: detail);
    }

    test('worker queries use an index and need no sort', () {
      expectIndexed(sql.selectNextReady, [0], 'durable_queue_tasks_ready');
      expectIndexed(sql.selectNextWake, [], 'durable_queue_tasks_wake');
      expectIndexed(sql.selectActiveByKey, ['k'], 'durable_queue_tasks_dedup');
      expectIndexed(sql.selectByStatus + QueueSql.limitClause, [
        'running',
        100,
      ], 'durable_queue_tasks_by_status');
      expectIndexed(sql.selectByGroup, ['g'], 'durable_queue_tasks_group');
      expectIndexed(sql.selectMaxSequence, [], 'durable_queue_tasks_sequence');
    });

    test('dependency lookups use the edge key', () {
      final detail = plan(sql.selectWaitingDependents, ['p']);
      // Start from the dependency's edges, then fetch each task by id. Only
      // that task's dependents are sorted, never the whole table.
      expect(
        detail,
        startsWith('SEARCH e USING PRIMARY KEY (dependency_id=?)'),
        reason: detail,
      );
      expect(detail, contains('SEARCH t USING INDEX'), reason: detail);
      expect(detail, contains('(id=?)'), reason: detail);
      expect(detail, isNot(contains('SCAN')), reason: detail);
    });
  });
}
