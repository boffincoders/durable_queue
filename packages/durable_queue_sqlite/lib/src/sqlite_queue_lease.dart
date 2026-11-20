import 'dart:convert';
import 'dart:math';

import 'package:durable_queue/durable_queue.dart';
import 'package:sqlite3/common.dart';

import 'platform/open_stub.dart'
    if (dart.library.ffi) 'platform/open_native.dart'
    if (dart.library.js_interop) 'platform/open_web.dart'
    as platform;
import 'sql.dart';
import 'sqlite_queue_storage.dart';

/// A [QueueLease] kept in the queue's SQLite database.
///
/// Use it instead of `durable_queue_workmanager`'s `FileQueueLease` when the
/// queue's storage is a [SqliteQueueStorage] file: the app and the
/// background job open the database at the same [path] anyway, so the lease
/// needs no file of its own.
///
/// ```dart
/// QueueLease openLease() => SqliteQueueLease.open(databasePath);
/// ```
///
/// Each check and change of the lease is one SQLite transaction, which
/// SQLite keeps exclusive between connections, isolates, and processes.
/// There is no window between reading the lease and taking it, and no guard
/// file to clean up after a runner that died.
///
/// Each [SqliteQueueLease] is one holder. Create one per runner and do not
/// share it between isolates. It opens a connection only for each check or
/// change and closes it afterwards, so a runner that never calls [close],
/// such as a Workmanager job, leaves nothing open.
///
/// Like any lease that expires, it depends on the device clock and is
/// limited by [ttl]: if a holder is frozen for longer than [ttl] while it
/// runs a task, another runner can take over and run that task again. Keep
/// handlers idempotent, as every durable_queue handler should be.
///
/// Native platforms only.
final class SqliteQueueLease implements QueueLease {
  SqliteQueueLease._(
    this.path,
    String tablePrefix,
    this.ttl,
    DateTime Function()? now,
  ) : _sql = QueueSql(tablePrefix),
      _now = now ?? DateTime.now,
      _owner = _newOwner();

  /// Opens the lease in the database file at [path], the one the queue's
  /// [SqliteQueueStorage] uses, with the same [tablePrefix].
  ///
  /// [ttl] defaults to two minutes and must be positive. [now] is for tests.
  ///
  /// Throws [UnsupportedError] on the web.
  factory SqliteQueueLease.open(
    String path, {
    String tablePrefix = SqliteQueueStorage.defaultTablePrefix,
    Duration ttl = const Duration(minutes: 2),
    DateTime Function()? now,
  }) {
    if (ttl <= Duration.zero) {
      throw ArgumentError.value(ttl, 'ttl', 'Must be positive');
    }
    if (!RegExp(r'^[A-Za-z_][A-Za-z0-9_]*$').hasMatch(tablePrefix)) {
      throw ArgumentError.value(
        tablePrefix,
        'tablePrefix',
        'Use letters, digits, and underscores, starting with a letter or '
            'underscore',
      );
    }
    final opened = platform.openFile(path);
    try {
      opened.database.execute(QueueSql(tablePrefix).createMeta);
    } finally {
      opened.database.close();
    }
    return SqliteQueueLease._(path, tablePrefix, ttl, now);
  }

  /// The database file that keeps the lease.
  final String path;

  @override
  final Duration ttl;

  final QueueSql _sql;
  final DateTime Function() _now;
  final String _owner;
  var _closed = false;

  static const _key = 'lease';

  static String _newOwner() {
    final random = Random.secure();
    return List.generate(
      16,
      (_) => random.nextInt(256).toRadixString(16).padLeft(2, '0'),
    ).join();
  }

  @override
  Future<bool> tryAcquire() async => _transaction((db) {
    final current = _read(db);
    final now = _now();
    if (current != null &&
        current.owner != _owner &&
        current.expiresAt.isAfter(now)) {
      return false;
    }
    _write(db, now);
    return true;
  });

  @override
  Future<bool> renew() async => _transaction((db) {
    final now = _now();
    if (!_holds(_read(db), now)) return false;
    _write(db, now);
    return true;
  });

  @override
  Future<void> release() async => _transaction((db) {
    // An expired lease may be being taken over; leave it.
    if (_holds(_read(db), _now())) db.execute(_sql.deleteState, [_key]);
  });

  /// Stops using this lease. Release it first if it is held. Afterwards
  /// every method throws [StateError]. Holds no connection, so calling it
  /// is optional.
  void close() => _closed = true;

  bool _holds(({String owner, DateTime expiresAt})? lease, DateTime now) =>
      lease != null && lease.owner == _owner && lease.expiresAt.isAfter(now);

  ({String owner, DateTime expiresAt})? _read(CommonDatabase db) {
    final rows = db.select(_sql.selectState, [_key]);
    if (rows.isEmpty) return null;
    try {
      final data = jsonDecode(rows.single['value'] as String);
      if (data case {'owner': final String owner, 'expiresAt': final int at}) {
        return (
          owner: owner,
          expiresAt: DateTime.fromMillisecondsSinceEpoch(at),
        );
      }
    } on FormatException {
      // Treated as free below.
    }
    return null;
  }

  void _write(CommonDatabase db, DateTime now) {
    db.execute(_sql.upsertState, [
      _key,
      jsonEncode({
        'owner': _owner,
        'expiresAt': now.add(ttl).millisecondsSinceEpoch,
      }),
    ]);
  }

  T _transaction<T>(T Function(CommonDatabase db) body) {
    if (_closed) throw StateError('SqliteQueueLease is closed');
    final db = platform.openFile(path).database;
    try {
      // IMMEDIATE takes the write lock before reading, so two runners
      // cannot both see the lease free.
      db.execute('BEGIN IMMEDIATE');
      try {
        final result = body(db);
        db.execute('COMMIT');
        return result;
      } catch (_) {
        db.execute('ROLLBACK');
        rethrow;
      }
    } finally {
      db.close();
    }
  }
}
