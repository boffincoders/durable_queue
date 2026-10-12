import 'dart:convert';

import 'package:durable_queue/durable_queue.dart';
import 'package:sqlite3/sqlite3.dart';

import 'sql.dart';

/// [QueueStorage] backed by SQLite, through `package:sqlite3`.
///
/// Tasks are written in transactions and survive process death: open the
/// same database on the next launch and call `DurableQueue.start`.
///
/// Every worker query uses an index, so claiming work, scheduling retries,
/// and settling dependencies stay fast as the backlog grows. Each row stores
/// the canonical `StoredTask.toJson` record next to indexed copies of the
/// fields the queue searches by.
///
/// `package:sqlite3` is synchronous. Each call runs on the calling isolate
/// and returns a completed future. Queue operations touch one or a few rows,
/// so this is fast, but very large `getTasks` or `purge` calls will block
/// that isolate while they run.
///
/// Only one `DurableQueue` should use a given database and table prefix at a
/// time.
final class SqliteQueueStorage implements QueueStorage {
  SqliteQueueStorage._(
    this._db, {
    required bool ownsDatabase,
    required String tablePrefix,
  }) : _ownsDatabase = ownsDatabase,
       _sql = QueueSql(tablePrefix) {
    _migrate();
  }

  /// Opens or creates the database file at [path].
  ///
  /// The database uses write-ahead logging and full synchronous writes, so a
  /// committed task survives a crash of the app or the device. Call [close]
  /// when the storage is no longer needed.
  ///
  /// [tablePrefix] names the tables (`<prefix>tasks`, ...). It must start
  /// with a letter or underscore and contain only letters, digits, and
  /// underscores.
  ///
  /// Throws [StateError] if the tables were created by a newer version of
  /// this package.
  factory SqliteQueueStorage.open(
    String path, {
    String tablePrefix = defaultTablePrefix,
  }) {
    _checkPrefix(tablePrefix);
    final db = sqlite3.open(path);
    try {
      db.execute('PRAGMA journal_mode = WAL');
      db.execute('PRAGMA synchronous = FULL');
      db.execute('PRAGMA busy_timeout = 5000');
      return SqliteQueueStorage._(
        db,
        ownsDatabase: true,
        tablePrefix: tablePrefix,
      );
    } catch (_) {
      db.close();
      rethrow;
    }
  }

  /// Creates a storage in a private in-memory database.
  ///
  /// Nothing survives the process. Useful for tests.
  factory SqliteQueueStorage.inMemory({
    String tablePrefix = defaultTablePrefix,
  }) {
    _checkPrefix(tablePrefix);
    return SqliteQueueStorage._(
      sqlite3.openInMemory(),
      ownsDatabase: true,
      tablePrefix: tablePrefix,
    );
  }

  /// Uses tables inside an existing [database] that the app already owns.
  ///
  /// The tables are created if missing. Connection settings such as the
  /// journal mode are left to the app, and [close] does not close
  /// [database]. Use a distinct [tablePrefix] per queue if several queues
  /// share one database.
  factory SqliteQueueStorage.fromDatabase(
    Database database, {
    String tablePrefix = defaultTablePrefix,
  }) {
    _checkPrefix(tablePrefix);
    return SqliteQueueStorage._(
      database,
      ownsDatabase: false,
      tablePrefix: tablePrefix,
    );
  }

  /// Table prefix used when none is given.
  static const defaultTablePrefix = 'durable_queue_';

  /// Schema version this release creates and reads.
  static const schemaVersion = currentSchemaVersion;

  final Database _db;
  final bool _ownsDatabase;
  final QueueSql _sql;
  var _closed = false;

  /// Whether [close] has been called.
  bool get isClosed => _closed;

  /// Releases the storage. Closes the database if this storage opened it.
  ///
  /// Afterwards every method throws [StateError]. Calling [close] again does
  /// nothing.
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    if (_ownsDatabase) _db.close();
  }

  @override
  Future<void> save(StoredTask task) async {
    _transaction(() {
      if (_db.select(_sql.exists, [task.id]).isNotEmpty) {
        throw StateError('Task "${task.id}" already exists');
      }
      _db.execute(_sql.insertTask, [task.id, ..._columns(task)]);
      _writeEdges(task);
    });
  }

  @override
  Future<void> update(StoredTask task) async {
    _transaction(() {
      if (_db.select(_sql.exists, [task.id]).isEmpty) {
        throw StateError('Task "${task.id}" does not exist');
      }
      _db.execute(_sql.updateTask, [..._columns(task), task.id]);
      _db.execute(_sql.deleteEdges, [task.id]);
      _writeEdges(task);
    });
  }

  @override
  Future<void> delete(String id) async {
    _transaction(() {
      _db.execute(_sql.deleteTask, [id]);
      _db.execute(_sql.deleteEdges, [id]);
    });
  }

  @override
  Future<StoredTask?> get(String id) async => _one(_sql.selectById, [id]);

  @override
  Future<List<StoredTask>> getAll() async => _many(_sql.selectAll);

  @override
  Future<List<StoredTask>> getPending() => getByStatus(TaskStatus.pending);

  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit}) async {
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
  Future<StoredTask?> getNextReady(DateTime now) async =>
      _one(_sql.selectNextReady, [now.microsecondsSinceEpoch]);

  @override
  Future<DateTime?> getNextWakeAt() async =>
      _one(_sql.selectNextWake)?.nextAttemptAt;

  @override
  Future<int> getMaxSequence() async {
    _ensureOpen();
    final value = _db.select(_sql.selectMaxSequence).single['max'];
    return value == null ? 0 : value as int;
  }

  @override
  Future<StoredTask?> findActiveByDeduplicationKey(String key) async =>
      _one(_sql.selectActiveByKey, [key]);

  @override
  Future<List<StoredTask>> getWaitingDependents(String id) async =>
      _many(_sql.selectWaitingDependents, [id]);

  @override
  Future<List<StoredTask>> getByGroup(
    String group, {
    TaskStatus? status,
  }) async {
    if (status == null) return _many(_sql.selectByGroup, [group]);
    return _many(_sql.selectByGroupAndStatus, [group, status.name]);
  }

  // Internals.

  void _migrate() {
    _transaction(() {
      _db.execute(_sql.createMeta);
      final rows = _db.select(_sql.selectVersion);
      final stored = rows.isEmpty
          ? 0
          : int.parse(rows.single['value'] as String);
      if (stored > currentSchemaVersion) {
        throw StateError(
          'The ${_sql.tasks} table uses schema version $stored, but this '
          'version of durable_queue_sqlite supports up to '
          '$currentSchemaVersion. Upgrade durable_queue_sqlite.',
        );
      }
      if (stored < 1) {
        for (final statement in _sql.schemaV1) {
          _db.execute(statement);
        }
      }
      if (stored != currentSchemaVersion) {
        _db.execute(_sql.upsertVersion, ['$currentSchemaVersion']);
      }
    });
  }

  /// Runs [body] atomically. Uses a savepoint, so it also works when the
  /// app already has a transaction open on a shared database.
  void _transaction(void Function() body) {
    _ensureOpen();
    _db.execute('SAVEPOINT durable_queue');
    try {
      body();
      _db.execute('RELEASE durable_queue');
    } catch (_) {
      _db.execute('ROLLBACK TO durable_queue');
      _db.execute('RELEASE durable_queue');
      rethrow;
    }
  }

  void _writeEdges(StoredTask task) {
    if (task.status != TaskStatus.waiting) return;
    for (final dependency in task.dependsOn) {
      _db.execute(_sql.insertEdge, [dependency, task.id]);
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

  StoredTask? _one(String sql, [List<Object?> parameters = const []]) {
    _ensureOpen();
    final rows = _db.select(sql, parameters);
    return rows.isEmpty ? null : _decode(rows.first['record']);
  }

  List<StoredTask> _many(String sql, [List<Object?> parameters = const []]) {
    _ensureOpen();
    return List<StoredTask>.unmodifiable(
      _db.select(sql, parameters).map((row) => _decode(row['record'])),
    );
  }

  static StoredTask _decode(Object? record) {
    if (record is! String) {
      throw const FormatException('durable_queue_sqlite: missing record');
    }
    final json = jsonDecode(record);
    if (json is! Map<String, dynamic>) {
      throw const FormatException('durable_queue_sqlite: record is not a map');
    }
    return StoredTask.fromJson(json);
  }

  void _ensureOpen() {
    if (_closed) throw StateError('SqliteQueueStorage is closed');
  }

  static void _checkPrefix(String prefix) {
    if (!RegExp(r'^[A-Za-z_][A-Za-z0-9_]*$').hasMatch(prefix)) {
      throw ArgumentError.value(
        prefix,
        'tablePrefix',
        'Use letters, digits, and underscores, starting with a letter or '
            'underscore',
      );
    }
  }
}
