import 'dart:convert';

import 'package:crypto/crypto.dart';
import 'package:durable_queue/durable_queue.dart';
import 'package:hive_ce/hive_ce.dart';

/// [QueueStorage] on a Hive box (`hive_ce`).
///
/// Every task is one entry in a dedicated `Box<String>`: its key is the task
/// id and its value the JSON of [StoredTask.toJson]. When the storage is
/// created, all tasks are read into an indexed in-memory view (a
/// [MemoryQueueStorage]), so queries do not scan the box. Every write goes to
/// the view and to the box; a write that fails on disk is undone in the view.
///
/// Open it with [open], or wrap a box you opened yourself:
///
/// ```dart
/// Hive.init(directory);
/// final storage = await HiveQueueStorage.open('durable_queue');
/// ```
///
/// Rules:
///
/// * Use a box only for the queue. Do not write it elsewhere while the
///   storage is open.
/// * Open it in one isolate only. Hive must not open a box from two isolates
///   at once, and this storage keeps its view in the isolate that opened it.
///   A background job in another isolate (`durable_queue_workmanager`) needs
///   a storage that supports that, such as `durable_queue_sqlite`.
final class HiveQueueStorage implements QueueStorage {
  /// Wraps an open [box] and reads its tasks.
  ///
  /// When [flushEachWrite] is true, each write is flushed to disk before it
  /// completes, so a stored task survives a power loss as well as an app
  /// kill. When false, writes reach the operating system but the last ones
  /// can be lost on power loss; they still survive the app being killed.
  ///
  /// Throws [FormatException] if an entry is not a task record.
  HiveQueueStorage(Box<String> box, {bool flushEachWrite = true})
    : _box = box,
      _flushEachWrite = flushEachWrite {
    for (final key in box.keys) {
      final value = box.get(key);
      final StoredTask task;
      try {
        task = StoredTask.fromJson(jsonDecode(value!) as Map<String, dynamic>);
      } on Object catch (error) {
        throw FormatException(
          'Hive box "${box.name}": entry "$key" is not a task record: $error',
        );
      }
      if (key != boxKey(task.id)) {
        throw FormatException(
          'Hive box "${box.name}": entry "$key" holds task "${task.id}"',
        );
      }
      // The view is empty and saves complete synchronously.
      _view.save(task);
    }
  }

  /// Opens the box [name] (see `Hive.openBox`) and wraps it.
  ///
  /// [path] is the directory for the box file; without it, the directory
  /// given to `Hive.init` (or `Hive.initFlutter` in Flutter) is used.
  /// [encryptionCipher] encrypts the whole box with Hive's own encryption.
  static Future<HiveQueueStorage> open(
    String name, {
    String? path,
    HiveCipher? encryptionCipher,
    bool flushEachWrite = true,
  }) async {
    final box = await Hive.openBox<String>(
      name,
      path: path,
      encryptionCipher: encryptionCipher,
    );
    return HiveQueueStorage(box, flushEachWrite: flushEachWrite);
  }

  /// The key a task is stored under: its id, or a hash of it when the id is
  /// longer than Hive allows (255 bytes of UTF-8).
  static String boxKey(String id) {
    final bytes = utf8.encode(id);
    if (bytes.length <= 255) return id;
    return '$_hashedKeyPrefix${sha256.convert(bytes)}';
  }

  static const _hashedKeyPrefix = 'durable_queue:sha256:';

  final Box<String> _box;
  final bool _flushEachWrite;
  final _view = MemoryQueueStorage();

  /// The wrapped box.
  Box<String> get box => _box;

  /// Closes the box. The storage cannot be used afterwards.
  Future<void> close() => _box.close();

  @override
  Future<void> save(StoredTask task) async {
    final value = _encode(task);
    // Rejects a duplicate id before anything is written.
    await _view.save(task);
    try {
      await _write(task.id, value);
    } on Object {
      await _view.delete(task.id);
      rethrow;
    }
  }

  @override
  Future<void> update(StoredTask task) async {
    final value = _encode(task);
    final previous = await _view.get(task.id);
    await _view.update(task);
    try {
      await _write(task.id, value);
    } on Object {
      if (identical(await _view.get(task.id), task)) {
        await _view.update(previous!);
      }
      rethrow;
    }
  }

  @override
  Future<void> delete(String id) async {
    final previous = await _view.get(id);
    if (previous == null) return;
    await _view.delete(id);
    try {
      await _box.delete(boxKey(id));
      if (_flushEachWrite) await _box.flush();
    } on Object {
      if (await _view.get(id) == null) await _view.save(previous);
      rethrow;
    }
  }

  @override
  Future<StoredTask?> get(String id) => _view.get(id);

  @override
  Future<List<StoredTask>> getPending() => _view.getPending();

  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit}) =>
      _view.getByStatus(status, limit: limit);

  @override
  Future<StoredTask?> getNextReady(DateTime now) => _view.getNextReady(now);

  @override
  Future<DateTime?> getNextWakeAt() => _view.getNextWakeAt();

  @override
  Future<int> getMaxSequence() => _view.getMaxSequence();

  @override
  Future<List<StoredTask>> getAll() => _view.getAll();

  @override
  Future<StoredTask?> findActiveByDeduplicationKey(String key) =>
      _view.findActiveByDeduplicationKey(key);

  @override
  Future<List<StoredTask>> getWaitingDependents(String id) =>
      _view.getWaitingDependents(id);

  @override
  Future<List<StoredTask>> getByGroup(String group, {TaskStatus? status}) =>
      _view.getByGroup(group, status: status);

  Future<void> _write(String id, String value) async {
    await _box.put(boxKey(id), value);
    if (_flushEachWrite) await _box.flush();
  }

  static String _encode(StoredTask task) {
    try {
      return jsonEncode(task.toJson());
    } on JsonUnsupportedObjectError catch (error) {
      throw ArgumentError.value(
        error.unsupportedObject,
        'payload',
        'Task ${task.id}: payloads must contain only JSON values',
      );
    }
  }
}
