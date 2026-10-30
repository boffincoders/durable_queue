import 'dart:convert';

import 'package:drift/drift.dart';
import 'package:durable_queue/durable_queue.dart';

import 'sql.dart';

/// [QueueStorage] that keeps tasks in tables inside an app's existing drift
/// database.
///
/// ```dart
/// final db = AppDatabase(); // your drift database
/// final storage = await DriftQueueStorage.open(db);
/// final queue = DurableQueue(storage: storage);
/// ```
///
/// Each write runs in its own drift transaction, so tasks survive the app
/// being killed as the rest of the database does.
///
/// Do not call the queue from inside a drift transaction of the app: the
/// queue may be waiting for the database while the transaction waits for
/// the queue. Such calls throw [StateError]. Enqueue after the transaction
/// commits.
///
/// The tables are created with plain SQL when the storage opens, are not
/// part of the app's drift schema, and do not need a migration. They are
/// the same tables `durable_queue_sqlite` uses, so a database used by one
/// can be opened with the other. Every worker query uses an index.
///
/// Works wherever the drift database runs, including the web. Only one
/// `DurableQueue` should use a given database and table prefix at a time.
final class DriftQueueStorage implements QueueStorage, QueueStorageCallGuard {
  DriftQueueStorage._(this._db, String tablePrefix)
      : _sql = QueueSql(tablePrefix);

  /// Creates the tables in [database] if needed and returns a storage that
  /// uses them.
  ///
  /// [tablePrefix] names the tables (`<prefix>tasks`, ...). It must start
  /// with a letter or underscore and contain only letters, digits, and
  /// underscores. Use a distinct prefix per queue if several queues share
  /// one database.
  ///
  /// Throws [StateError] if the tables were created by a newer version of
  /// this package or of `durable_queue_sqlite`, or if called inside a drift
  /// transaction.
  static Future<DriftQueueStorage> open(
    GeneratedDatabase database, {
    String tablePrefix = defaultTablePrefix,
  }) async {
    if (!RegExp(r'^[A-Za-z_][A-Za-z0-9_]*$').hasMatch(tablePrefix)) {
      throw ArgumentError.value(
        tablePrefix,
        'tablePrefix',
        'Use letters, digits, and underscores, starting with a letter or '
            'underscore',
      );
    }
    final storage = DriftQueueStorage._(database, tablePrefix);
    await storage._migrate();
    return storage;
  }

  /// Table prefix used when none is given; the same as
  /// `durable_queue_sqlite`'s.
  static const defaultTablePrefix = 'durable_queue_';

  /// Schema version this release creates and reads.
  static const schemaVersion = currentSchemaVersion;

  final GeneratedDatabase _db;
  final QueueSql _sql;
  var _closed = false;

  /// Whether [close] has been called.
  bool get isClosed => _closed;

  /// Stops using the database. The database itself stays open; the app
  /// closes it.
  ///
  /// Afterwards every method throws [StateError].
  Future<void> close() async => _closed = true;

  @override
  Future<void> save(StoredTask task) => _write(() async {
        if (await _exists(task.id)) {
          throw StateError('Task "${task.id}" already exists');
        }
        await _db
            .customStatement(_sql.insertTask, [task.id, ..._columns(task)]);
        await _writeEdges(task);
      });

  @override
  Future<void> update(StoredTask task) => _write(() async {
        if (!await _exists(task.id)) {
          throw StateError('Task "${task.id}" does not exist');
        }
        await _db
            .customStatement(_sql.updateTask, [..._columns(task), task.id]);
        await _db.customStatement(_sql.deleteEdges, [task.id]);
        await _writeEdges(task);
      });

  @override
  Future<void> delete(String id) => _write(() async {
        await _db.customStatement(_sql.deleteTask, [id]);
        await _db.customStatement(_sql.deleteEdges, [id]);
      });

  @override
  Future<StoredTask?> get(String id) => _one(_sql.selectById, [id]);

  @override
  Future<List<StoredTask>> getAll() => _many(_sql.selectAll);

  @override
  Future<List<StoredTask>> getPending() => getByStatus(TaskStatus.pending);

  @override
  Future<List<StoredTask>> getByStatus(
    TaskStatus status, {
    int? limit,
  }) async {
    if (limit != null && limit < 1) {
      throw ArgumentError.value(limit, 'limit', 'Must be positive');
    }
    if (limit == null) return _many(_sql.selectByStatus, [status.name]);
    return _many(_sql.selectByStatus + QueueSql.limitClause, [
      status.name,
      limit,
    ]);
  }

  @override
  Future<StoredTask?> getNextReady(DateTime now) =>
      _one(_sql.selectNextReady, [now.microsecondsSinceEpoch]);

  @override
  Future<DateTime?> getNextWakeAt() async =>
      (await _one(_sql.selectNextWake))?.nextAttemptAt;

  @override
  Future<int> getMaxSequence() async {
    _ensureOpen();
    final row = await _db.customSelect(_sql.selectMaxSequence).getSingle();
    return row.readNullable<int>('max') ?? 0;
  }

  @override
  Future<StoredTask?> findActiveByDeduplicationKey(String key) =>
      _one(_sql.selectActiveByKey, [key]);

  @override
  Future<List<StoredTask>> getWaitingDependents(String id) =>
      _many(_sql.selectWaitingDependents, [id]);

  @override
  Future<List<StoredTask>> getByGroup(String group, {TaskStatus? status}) {
    if (status == null) return _many(_sql.selectByGroup, [group]);
    return _many(_sql.selectByGroupAndStatus, [group, status.name]);
  }

  // Internals.

  Future<void> _migrate() => _write(() async {
        await _db.customStatement(_sql.createMeta);
        final rows = await _db.customSelect(_sql.selectVersion).get();
        final stored =
            rows.isEmpty ? 0 : int.parse(rows.single.read<String>('value'));
        if (stored > currentSchemaVersion) {
          throw StateError(
            'The ${_sql.tasks} table uses schema version $stored, but this '
            'version of durable_queue_drift supports up to '
            '$currentSchemaVersion. Upgrade durable_queue_drift.',
          );
        }
        if (stored < 1) {
          for (final statement in _sql.schemaV1) {
            await _db.customStatement(statement);
          }
        }
        if (stored != currentSchemaVersion) {
          await _db
              .customStatement(_sql.upsertVersion, ['$currentSchemaVersion']);
        }
      });

  Future<void> _write(Future<void> Function() body) {
    _ensureOpen();
    return _db.transaction(body);
  }

  Future<bool> _exists(String id) async =>
      (await _db.customSelect(_sql.exists, variables: [_variable(id)]).get())
          .isNotEmpty;

  Future<void> _writeEdges(StoredTask task) async {
    if (task.status != TaskStatus.waiting) return;
    for (final dependency in task.dependsOn) {
      await _db.customStatement(_sql.insertEdge, [dependency, task.id]);
    }
  }

  static List<Object?> _columns(StoredTask task) => [
        task.status.name,
        task.priority,
        task.createdAt.microsecondsSinceEpoch,
        task.sequence,
        task.nextAttemptAt?.microsecondsSinceEpoch,
        task.deduplicationKey,
        task.group,
        jsonEncode(task.toJson()),
      ];

  static Variable<Object> _variable(Object value) => switch (value) {
        final int number => Variable<int>(number),
        final String text => Variable<String>(text),
        _ => throw ArgumentError.value(value, 'value', 'Unsupported parameter'),
      };

  Future<StoredTask?> _one(
    String sql, [
    List<Object> parameters = const [],
  ]) async {
    _ensureOpen();
    final rows = await _db.customSelect(sql,
        variables: [for (final p in parameters) _variable(p)]).get();
    return rows.isEmpty ? null : _decode(rows.first.read<String>('record'));
  }

  Future<List<StoredTask>> _many(
    String sql, [
    List<Object> parameters = const [],
  ]) async {
    _ensureOpen();
    final rows = await _db.customSelect(sql,
        variables: [for (final p in parameters) _variable(p)]).get();
    return List<StoredTask>.unmodifiable(
      rows.map((row) => _decode(row.read<String>('record'))),
    );
  }

  static StoredTask _decode(String record) {
    final json = jsonDecode(record);
    if (json is! Map<String, dynamic>) {
      throw const FormatException('durable_queue_drift: record is not a map');
    }
    return StoredTask.fromJson(json);
  }

  /// Throws [StateError] when called inside a drift transaction of the app.
  /// `DurableQueue` calls it before it waits for other queue work.
  @override
  void checkCall() {
    // ignore: invalid_use_of_internal_member
    if (!identical(_db.resolvedEngine, _db)) {
      throw StateError(
        'durable_queue was called inside a drift transaction. The queue may '
        'be waiting for the database while the transaction waits for the '
        'queue, so this is not allowed. Call the queue after the '
        'transaction commits.',
      );
    }
  }

  void _ensureOpen() {
    if (_closed) throw StateError('DriftQueueStorage is closed');
    checkCall();
  }
}
