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
/// Each [SqliteQueueLease] is one holder, with its own connection to the
/// database. Create one per runner, do not share it between isolates, and
/// [close] it when the runner is done.
///
/// Like any lease that expires, it depends on the device clock and is
/// limited by [ttl]: if a holder is frozen for longer than [ttl] while it
/// runs a task, another runner can take over and run that task again. Keep
/// handlers idempotent, as every durable_queue handler should be.
///
/// Native platforms only.
final class SqliteQueueLease implements QueueLease {
  SqliteQueueLease._(
    this._db,
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
    } catch (_) {
      opened.database.close();
      rethrow;
    }
    return SqliteQueueLease._(opened.database, path, tablePrefix, ttl, now);
  }

  /// The database file that keeps the lease.
  final String path;

  @override
  final Duration ttl;

  final CommonDatabase _db;
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
  Future<bool> tryAcquire() async => _transaction(() {
    final current = _read();
    final now = _now();
    if (current != null &&
        current.owner != _owner &&
        current.expiresAt.isAfter(now)) {
      return false;
    }
    _write(now);
    return true;
  });

  @override
  Future<bool> renew() async => _transaction(() {
    final now = _now();
    if (!_holds(_read(), now)) return false;
    _write(now);
    return true;
  });

  @override
  Future<void> release() async => _transaction(() {
    // An expired lease may be being taken over; leave it.
    if (_holds(_read(), _now())) _db.execute(_sql.deleteState, [_key]);
  });

  /// Closes this lease's connection. Release the lease first if it is held.
  /// Afterwards every method throws [StateError].
  void close() {
    if (_closed) return;
    _closed = true;
    _db.close();
  }

  bool _holds(({String owner, DateTime expiresAt})? lease, DateTime now) =>
      lease != null && lease.owner == _owner && lease.expiresAt.isAfter(now);

  ({String owner, DateTime expiresAt})? _read() {
    final rows = _db.select(_sql.selectState, [_key]);
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

  void _write(DateTime now) {
    _db.execute(_sql.upsertState, [
      _key,
      jsonEncode({
        'owner': _owner,
        'expiresAt': now.add(ttl).millisecondsSinceEpoch,
      }),
    ]);
  }

  T _transaction<T>(T Function() body) {
    if (_closed) throw StateError('SqliteQueueLease is closed');
    // IMMEDIATE takes the write lock before reading, so two runners cannot
    // both see the lease free.
    _db.execute('BEGIN IMMEDIATE');
    try {
      final result = body();
      _db.execute('COMMIT');
      return result;
    } catch (_) {
      _db.execute('ROLLBACK');
      rethrow;
    }
  }
}
