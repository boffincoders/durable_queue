import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_hive/durable_queue_hive.dart';
import 'package:hive_ce/hive_ce.dart';

/// Opens Hive boxes in a temporary directory for one test file.
final class HiveFixture {
  HiveFixture() : directory = Directory.systemTemp.createTempSync('dq_hive_');

  final Directory directory;
  final _opened = <HiveQueueStorage>[];
  var _boxes = 0;

  /// A new box name.
  String nextName() => 'box${_boxes++}';

  Future<HiveQueueStorage> open(
    String name, {
    bool flushEachWrite = true,
  }) async {
    final storage = await HiveQueueStorage.open(
      name,
      path: directory.path,
      flushEachWrite: flushEachWrite,
    );
    _opened.add(storage);
    return storage;
  }

  Future<Box<String>> openRaw(String name) =>
      Hive.openBox<String>(name, path: directory.path);

  File file(String name) => File('${directory.path}/$name.hive');

  Future<void> dispose() async {
    for (final storage in _opened) {
      if (storage.box.isOpen) await storage.close();
    }
    await Hive.close();
    directory.deleteSync(recursive: true);
  }
}

/// A storage whose box is still opening; every call waits for it. Lets the
/// synchronous contract-suite factory use Hive's asynchronous open.
final class OpeningStorage implements QueueStorage {
  OpeningStorage(this._storage);

  final Future<QueueStorage> _storage;

  @override
  Future<void> save(StoredTask task) async => (await _storage).save(task);
  @override
  Future<void> update(StoredTask task) async => (await _storage).update(task);
  @override
  Future<void> delete(String id) async => (await _storage).delete(id);
  @override
  Future<StoredTask?> get(String id) async => (await _storage).get(id);
  @override
  Future<List<StoredTask>> getPending() async => (await _storage).getPending();
  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit}) async =>
      (await _storage).getByStatus(status, limit: limit);
  @override
  Future<StoredTask?> getNextReady(DateTime now) async =>
      (await _storage).getNextReady(now);
  @override
  Future<DateTime?> getNextWakeAt() async => (await _storage).getNextWakeAt();
  @override
  Future<int> getMaxSequence() async => (await _storage).getMaxSequence();
  @override
  Future<List<StoredTask>> getAll() async => (await _storage).getAll();
  @override
  Future<StoredTask?> findActiveByDeduplicationKey(String key) async =>
      (await _storage).findActiveByDeduplicationKey(key);
  @override
  Future<List<StoredTask>> getWaitingDependents(String id) async =>
      (await _storage).getWaitingDependents(id);
  @override
  Future<List<StoredTask>> getByGroup(
    String group, {
    TaskStatus? status,
  }) async => (await _storage).getByGroup(group, status: status);
}
