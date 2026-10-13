import 'dart:convert';

import 'package:durable_queue/durable_queue.dart';
import 'package:sqlite3/common.dart';

import 'platform/open_stub.dart'
    if (dart.library.ffi) 'platform/open_native.dart'
    if (dart.library.js_interop) 'platform/open_web.dart'
    as platform;
import 'platform/opened_database.dart';
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
/// Works on Android, iOS, macOS, Windows, and Linux ([open]) and on the web
/// ([openWeb], with SQLite compiled to WebAssembly and data kept in
/// IndexedDB).
///
/// `package:sqlite3` is synchronous. Each query runs on the calling isolate
/// (the page's main thread on the web). Queue operations touch one or a few
/// rows, so this is fast, but very large `getTasks` or `purge` calls block
/// while they run.
///
/// Only one `DurableQueue` should use a given database and table prefix at a
/// time.
final class SqliteQueueStorage implements QueueStorage {
  SqliteQueueStorage._(
    this._db, {
    required bool ownsDatabase,
    required String tablePrefix,
    Future<void> Function()? flush,
    Future<void> Function()? release,
  }) : _ownsDatabase = ownsDatabase,
       _flush = flush,
       _release = release,
       _sql = QueueSql(tablePrefix) {
    _migrate();
  }

  SqliteQueueStorage._opened(
    OpenedDatabase opened, {
    required String tablePrefix,
  }) : this._(
         opened.database,
         ownsDatabase: true,
         tablePrefix: tablePrefix,
         flush: opened.flush,
         release: opened.release,
       );

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
  /// this package, and [UnsupportedError] on the web (use [openWeb]).
  factory SqliteQueueStorage.open(
    String path, {
    String tablePrefix = defaultTablePrefix,
  }) {
    _checkPrefix(tablePrefix);
    final opened = platform.openFile(path);
    try {
      return SqliteQueueStorage._opened(opened, tablePrefix: tablePrefix);
    } catch (_) {
      opened.database.close();
      rethrow;
    }
  }

  /// Opens or creates a database in the browser. Web only.
  ///
  /// SQLite runs as WebAssembly, loaded from [sqliteWasm] (default:
  /// `sqlite3.wasm` next to your app's `index.html`). Download the
  /// `sqlite3.wasm` from the `package:sqlite3` GitHub release that matches
  /// the `sqlite3` version in your `pubspec.lock`, and put it in your app's
  /// `web/` folder.
  ///
  /// The database is stored in IndexedDB under [name]. Every write is
  /// flushed to IndexedDB before its future completes, so queued tasks
  /// survive a page reload or a closed tab. Use one [name] per queue, and
  /// open it in one tab at a time.
  ///
  /// With [inMemory], nothing is stored; useful for tests.
  ///
  /// Throws [UnsupportedError] on native platforms (use [open]).
  static Future<SqliteQueueStorage> openWeb(
    String name, {
    Uri? sqliteWasm,
    String tablePrefix = defaultTablePrefix,
    bool inMemory = false,
  }) async {
    _checkPrefix(tablePrefix);
    if (name.isEmpty || name.contains('/')) {
      throw ArgumentError.value(name, 'name', 'Must be non-empty, without /');
    }
    final opened = await platform.openWeb(
      name,
      sqliteWasm: sqliteWasm ?? Uri.parse('sqlite3.wasm'),
      inMemory: inMemory,
    );
    final SqliteQueueStorage storage;
    try {
      storage = SqliteQueueStorage._opened(opened, tablePrefix: tablePrefix);
    } catch (_) {
      opened.database.close();
      await opened.release?.call();
      rethrow;
    }
    await storage._flush?.call();
    return storage;
  }

  /// Creates a storage in a private in-memory database.
  ///
  /// Nothing survives the process. Useful for tests. Native only; on the
  /// web use `openWeb(name, inMemory: true)`.
  factory SqliteQueueStorage.inMemory({
    String tablePrefix = defaultTablePrefix,
  }) {
    _checkPrefix(tablePrefix);
    return SqliteQueueStorage._opened(
      platform.openMemory(),
      tablePrefix: tablePrefix,
    );
  }

  /// Uses tables inside an existing [database] that the app already owns.
  ///
  /// The tables are created if missing. Connection settings such as the
  /// journal mode are left to the app, and [close] does not close
  /// [database]. Use a distinct [tablePrefix] per queue if several queues
  /// share one database.
  ///
  /// Works with native and web databases. If writes to [database] are
  /// buffered (for example an IndexedDB file system on the web), pass
  /// [flush]; it is awaited after every write.
  factory SqliteQueueStorage.fromDatabase(
    CommonDatabase database, {
    String tablePrefix = defaultTablePrefix,
    Future<void> Function()? flush,
  }) {
    _checkPrefix(tablePrefix);
    return SqliteQueueStorage._(
      database,
      ownsDatabase: false,
      tablePrefix: tablePrefix,
      flush: flush,
    );
  }

  /// Table prefix used when none is given.
  static const defaultTablePrefix = 'durable_queue_';

  /// Schema version this release creates and reads.
  static const schemaVersion = currentSchemaVersion;

  final CommonDatabase _db;
  final bool _ownsDatabase;
  final Future<void> Function()? _flush;
  final Future<void> Function()? _release;
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
    if (_ownsDatabase) {
      _db.close();
      await _release?.call();
    }
  }

  @override
  Future<void> save(StoredTask task) async {
    await _write(() {
      if (_db.select(_sql.exists, [task.id]).isNotEmpty) {
        throw StateError('Task "${task.id}" already exists');
      }
      _db.execute(_sql.insertTask, [task.id, ..._columns(task)]);
      _writeEdges(task);
    });
  }

  @override
  Future<void> update(StoredTask task) async {
    await _write(() {
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
    await _write(() {
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

  /// Runs [body] atomically, then persists it if the platform buffers
  /// writes.
  Future<void> _write(void Function() body) async {
    _transaction(body);
    await _flush?.call();
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
