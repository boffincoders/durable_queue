import 'dart:async';
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
///
/// A row whose record cannot be read does not stop the queue. [get] returns
/// null for it, the lists skip it, and when it is next in line
/// [getNextReady] sets it aside: its row stays as it is but is no longer
/// selected, and [tasksSetAside] reports its id. [unreadableTaskIds] lists
/// such rows; remove them with [delete].
final class DriftQueueStorage
    implements QueueStorage, QueueStorageCallGuard, QueueStateStorage {
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
  Future<void> close() async {
    _closed = true;
    await _setAside.close();
  }

  final _setAside = StreamController<String>.broadcast();

  /// Reports the id of each row that [getNextReady] set aside because its
  /// record could not be read.
  Stream<String> get tasksSetAside => _setAside.stream;

  /// Ids of rows whose record cannot be read, in enqueue order. The queue
  /// leaves them out; delete them with [delete].
  Future<List<String>> unreadableTaskIds() async {
    _ensureOpen();
    final rows = await _db.customSelect(_sql.selectAllRecords).get();
    return [
      for (final row in rows)
        if (_tryDecode(row.read<String?>('record')) == null)
          row.read<String>('id'),
    ];
  }

  @override
  Future<void> save(StoredTask task) => _write(() async {
    if (await _exists(task.id)) {
      throw StateError('Task "${task.id}" already exists');
    }
    await _db.customStatement(_sql.insertTask, [task.id, ..._columns(task)]);
    await _writeEdges(task);
  });

  @override
  Future<void> update(StoredTask task) => _write(() async {
    if (!await _exists(task.id)) {
      throw StateError('Task "${task.id}" does not exist');
    }
    await _db.customStatement(_sql.updateTask, [..._columns(task), task.id]);
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

  /// Reads queue state, such as rate limits, kept in the metadata table.
  @override
  Future<String?> readState(String key) async {
    _ensureOpen();
    final rows = await _db
        .customSelect(
          _sql.selectState,
          variables: [Variable<String>(QueueSql.stateKey(key))],
        )
        .get();
    return rows.isEmpty ? null : rows.single.read<String>('value');
  }

  /// Keeps queue state, such as rate limits, in the metadata table.
  @override
  Future<void> writeState(String key, String? value) => _write(() async {
    final stored = QueueSql.stateKey(key);
    if (value == null) {
      await _db.customStatement(_sql.deleteState, [stored]);
    } else {
      await _db.customStatement(_sql.upsertState, [stored, value]);
    }
  });

  @override
  Future<List<StoredTask>> getAll() => _many(_sql.selectAll);

  @override
  Future<List<StoredTask>> getPending() => getByStatus(TaskStatus.pending);

  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit}) async {
    if (limit != null && limit < 1) {
      throw ArgumentError.value(limit, 'limit', 'Must be positive');
    }
    if (limit == null) return _many(_sql.selectByStatus, [status.name]);
    // Rows that cannot be read must not use up the limit.
    var want = limit;
    while (true) {
      final rows = await _rows(_sql.selectByStatus + QueueSql.limitClause, [
        status.name,
        want,
      ]);
      final tasks = [
        for (final row in rows) ?_tryDecode(row.read<String?>('record')),
      ];
      if (tasks.length >= limit || rows.length < want) {
        return List<StoredTask>.unmodifiable(tasks.take(limit));
      }
      want = limit + rows.length - tasks.length;
    }
  }

  /// Returns the next ready task whose record can be read. A ready row that
  /// cannot be read is set aside first, so it is not returned again, and
  /// [tasksSetAside] reports it.
  @override
  Future<StoredTask?> getNextReady(DateTime now) async {
    while (true) {
      final rows = await _rows(_sql.selectNextReady, [
        now.microsecondsSinceEpoch,
      ]);
      if (rows.isEmpty) return null;
      final task = _tryDecode(rows.first.read<String?>('record'));
      if (task != null) return task;
      final id = rows.first.read<String>('id');
      await _write(() async {
        await _db.customStatement(_sql.setAside, [
          StoredTask.heldNextAttemptAt.microsecondsSinceEpoch,
          id,
        ]);
      });
      if (!_setAside.isClosed) _setAside.add(id);
    }
  }

  @override
  Future<DateTime?> getNextWakeAt() async {
    final rows = await _rows(_sql.selectNextWake);
    if (rows.isEmpty) return null;
    final task = _tryDecode(rows.first.read<String?>('record'));
    if (task != null) return task.nextAttemptAt;
    return DateTime.fromMicrosecondsSinceEpoch(
      rows.first.read<int>('next_attempt_at_us'),
      isUtc: true,
    );
  }

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
    final stored = rows.isEmpty
        ? 0
        : int.parse(rows.single.read<String>('value'));
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
      await _db.customStatement(_sql.upsertVersion, ['$currentSchemaVersion']);
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

  Future<List<QueryRow>> _rows(
    String sql, [
    List<Object> parameters = const [],
  ]) {
    _ensureOpen();
    return _db
        .customSelect(
          sql,
          variables: [for (final p in parameters) _variable(p)],
        )
        .get();
  }

  /// The first row's task, or null when there is none or its record cannot
  /// be read.
  Future<StoredTask?> _one(
    String sql, [
    List<Object> parameters = const [],
  ]) async {
    final rows = await _rows(sql, parameters);
    return rows.isEmpty ? null : _tryDecode(rows.first.read<String?>('record'));
  }

  /// The rows' tasks, skipping records that cannot be read.
  Future<List<StoredTask>> _many(
    String sql, [
    List<Object> parameters = const [],
  ]) async {
    final rows = await _rows(sql, parameters);
    return List<StoredTask>.unmodifiable([
      for (final row in rows) ?_tryDecode(row.read<String?>('record')),
    ]);
  }

  static StoredTask? _tryDecode(String? record) {
    if (record == null) return null;
    try {
      return _decode(record);
    } on Object {
      return null;
    }
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
