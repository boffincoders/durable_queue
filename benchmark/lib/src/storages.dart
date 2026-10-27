import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_encryption/durable_queue_encryption.dart';
import 'package:durable_queue_hive/durable_queue_hive.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:hive_ce/hive_ce.dart';
import 'package:sqlite3/sqlite3.dart';

/// A storage under test, opened on files in [directory].
abstract base class StorageKind {
  const StorageKind(this.name);

  /// Shown in the results.
  final String name;

  /// Whether it keeps tasks across a restart.
  bool get persistent => true;

  /// Opens the storage named [file] in [directory]. Opening it again with
  /// the same arguments must see the same tasks.
  Future<QueueStorage> open(Directory directory, String file);

  /// Writes [tasks] quickly, without the durability the storage normally
  /// gives each write. Used only to set up large data sets.
  Future<void> fill(
    Directory directory,
    String file,
    Iterable<StoredTask> tasks,
  );

  /// Closes a storage returned by [open].
  Future<void> close(QueueStorage storage);
}

final class MemoryKind extends StorageKind {
  const MemoryKind() : super('Memory');

  static final _stores = <String, MemoryQueueStorage>{};

  @override
  bool get persistent => false;

  @override
  Future<QueueStorage> open(Directory directory, String file) async =>
      _stores.putIfAbsent('${directory.path}/$file', MemoryQueueStorage.new);

  @override
  Future<void> fill(
    Directory directory,
    String file,
    Iterable<StoredTask> tasks,
  ) async {
    final storage = await open(directory, file);
    for (final task in tasks) {
      await storage.save(task);
    }
  }

  @override
  Future<void> close(QueueStorage storage) async {}
}

final class SqliteKind extends StorageKind {
  const SqliteKind({this.encrypted = false})
    : super(encrypted ? 'SQLite + encryption' : 'SQLite');

  final bool encrypted;

  static final _key = List<int>.generate(32, (i) => i);

  @override
  Future<QueueStorage> open(Directory directory, String file) async {
    final sqlite = SqliteQueueStorage.open('${directory.path}/$file.db');
    return encrypted
        ? EncryptedQueueStorage(sqlite, cipher: AesGcmCipher(_key))
        : sqlite;
  }

  @override
  Future<void> fill(
    Directory directory,
    String file,
    Iterable<StoredTask> tasks,
  ) async {
    // One transaction for the whole set instead of one per task.
    final db = sqlite3.open('${directory.path}/$file.db');
    final sqlite = SqliteQueueStorage.fromDatabase(db);
    final QueueStorage storage = encrypted
        ? EncryptedQueueStorage(sqlite, cipher: AesGcmCipher(_key))
        : sqlite;
    db.execute('BEGIN');
    for (final task in tasks) {
      await storage.save(task);
    }
    db.execute('COMMIT');
    await sqlite.close();
    db.close();
  }

  @override
  Future<void> close(QueueStorage storage) async {
    final sqlite = storage is EncryptedQueueStorage
        ? storage.inner as SqliteQueueStorage
        : storage as SqliteQueueStorage;
    await sqlite.close();
  }
}

final class HiveKind extends StorageKind {
  const HiveKind({required this.flushEachWrite})
    : super(flushEachWrite ? 'Hive' : 'Hive (no flush)');

  final bool flushEachWrite;

  @override
  Future<QueueStorage> open(Directory directory, String file) =>
      HiveQueueStorage.open(
        file,
        path: directory.path,
        flushEachWrite: flushEachWrite,
      );

  @override
  Future<void> fill(
    Directory directory,
    String file,
    Iterable<StoredTask> tasks,
  ) async {
    final storage = await HiveQueueStorage.open(
      file,
      path: directory.path,
      flushEachWrite: false,
    );
    for (final task in tasks) {
      await storage.save(task);
    }
    await storage.box.flush();
    await storage.close();
  }

  @override
  Future<void> close(QueueStorage storage) async {
    await (storage as HiveQueueStorage).close();
    await Hive.close();
  }
}

/// Every storage, in the order of the result tables.
const allKinds = <StorageKind>[
  MemoryKind(),
  SqliteKind(),
  SqliteKind(encrypted: true),
  HiveKind(flushEachWrite: true),
  HiveKind(flushEachWrite: false),
];
