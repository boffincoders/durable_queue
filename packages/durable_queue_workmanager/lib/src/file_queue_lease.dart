import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'queue_lease.dart';

/// A [QueueLease] kept in a small file.
///
/// Every isolate and process that opens the same queue storage must use a
/// lease at the same [path], for example a file next to the database in the
/// application support directory. The Android background job runs in a
/// separate isolate of the same process, where `RandomAccessFile.lock` does
/// not exclude anyone, so this lease relies on exclusive file creation
/// instead:
///
/// * The holder is whoever created the file. Its content names the holder
///   and when the lease expires.
/// * [renew] rewrites the expiry. A holder that let the lease expire has
///   lost it, even if nobody took it over yet.
/// * An expired lease is taken over under a short-lived guard file
///   (`<path>.takeover`), so two runners cannot both take it.
///
/// Each [FileQueueLease] object is one holder. Create one per runner and do
/// not share it between isolates.
///
/// This is a cooperative lock between parts of one app, not a security
/// boundary, and it depends on the device clock. Its guarantee is limited
/// by [ttl]: if a holder is frozen for longer than [ttl] while it runs a
/// task, another runner can take over and run that task again. Pick a [ttl]
/// that is longer than any pause you expect, and keep handlers idempotent,
/// as every durable_queue handler should be.
final class FileQueueLease implements QueueLease {
  /// Creates a lease kept at [path].
  ///
  /// [now] is for tests; it defaults to [DateTime.now].
  FileQueueLease(
    this.path, {
    this.ttl = const Duration(minutes: 2),
    DateTime Function()? now,
  }) : _file = File(path),
       _guard = File('$path.takeover'),
       _now = now ?? DateTime.now,
       _owner = _newOwner() {
    if (ttl <= Duration.zero) {
      throw ArgumentError.value(ttl, 'ttl', 'Must be positive');
    }
  }

  /// Where the lease is kept.
  final String path;

  @override
  final Duration ttl;

  final File _file;
  final File _guard;
  final DateTime Function() _now;
  final String _owner;

  static String _newOwner() {
    final random = Random.secure();
    return List.generate(
      16,
      (_) => random.nextInt(256).toRadixString(16).padLeft(2, '0'),
    ).join();
  }

  @override
  Future<bool> tryAcquire() async {
    if (await _create()) return true;
    final current = await _read();
    if (current == null) return _create();
    if (current.owner == _owner) return renew();
    if (!current.isExpiredAt(_now())) return false;
    return _takeOver(current);
  }

  @override
  Future<bool> renew() async {
    final current = await _read();
    if (current == null ||
        current.owner != _owner ||
        current.isExpiredAt(_now())) {
      return false;
    }
    final temp = File('$path.$_owner.tmp');
    await temp.writeAsString(_content(), flush: true);
    await temp.rename(path);
    return true;
  }

  @override
  Future<void> release() async {
    final current = await _read();
    // An expired lease may be being taken over right now; leave it.
    if (current == null ||
        current.owner != _owner ||
        current.isExpiredAt(_now())) {
      return;
    }
    await _deleteIfPresent(_file);
  }

  String _content() => jsonEncode({
    'owner': _owner,
    'expiresAt': _now().add(ttl).millisecondsSinceEpoch,
  });

  /// Creates the lease file if there is none. Returns whether it did.
  Future<bool> _create() async {
    try {
      await _file.create(recursive: true, exclusive: true);
    } on PathExistsException {
      return false;
    } on FileSystemException {
      if (await _file.exists()) return false;
      rethrow;
    }
    // Until this write lands the file is empty; readers treat an unreadable
    // file as held until its modification time is [ttl] old.
    await _file.writeAsString(_content(), flush: true);
    return true;
  }

  Future<_LeaseRecord?> _read() async {
    final String text;
    try {
      text = await _file.readAsString();
    } on PathNotFoundException {
      return null;
    }
    final record = _LeaseRecord.parse(text);
    if (record != null) return record;
    try {
      final modified = await _file.lastModified();
      return _LeaseRecord(null, modified.add(ttl), text);
    } on FileSystemException {
      return null;
    }
  }

  Future<bool> _takeOver(_LeaseRecord expired) async {
    try {
      await _guard.create(recursive: true, exclusive: true);
    } on FileSystemException catch (error) {
      if (error is! PathExistsException && !await _guard.exists()) rethrow;
      await _clearAbandonedGuard();
      return false;
    }
    try {
      final current = await _read();
      if (current != null) {
        // Someone renewed or replaced it since we looked.
        if (current.text != expired.text || !current.isExpiredAt(_now())) {
          return false;
        }
        await _deleteIfPresent(_file);
      }
      return await _create();
    } finally {
      await _deleteIfPresent(_guard);
    }
  }

  /// Removes a guard left by a runner that died while taking over.
  Future<void> _clearAbandonedGuard() async {
    try {
      final modified = await _guard.lastModified();
      if (!modified.add(ttl).isAfter(_now())) await _deleteIfPresent(_guard);
    } on FileSystemException {
      return;
    }
  }

  static Future<void> _deleteIfPresent(File file) async {
    try {
      await file.delete();
    } on PathNotFoundException {
      return;
    }
  }
}

final class _LeaseRecord {
  _LeaseRecord(this.owner, this.expiresAt, this.text);

  static _LeaseRecord? parse(String text) {
    try {
      final data = jsonDecode(text);
      if (data is! Map) return null;
      final owner = data['owner'];
      final expiresAt = data['expiresAt'];
      if (owner is! String || expiresAt is! int) return null;
      return _LeaseRecord(
        owner,
        DateTime.fromMillisecondsSinceEpoch(expiresAt),
        text,
      );
    } on FormatException {
      return null;
    }
  }

  /// Null when the file could not be read as a lease.
  final String? owner;
  final DateTime expiresAt;
  final String text;

  bool isExpiredAt(DateTime now) => !expiresAt.isAfter(now);
}
