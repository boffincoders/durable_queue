import 'dart:convert';
import 'dart:io';

import 'package:durable_queue/durable_queue.dart';

/// Example [QueueStorage] adapter that keeps every task in one JSON file.
///
/// It shows the shape of a durable adapter in about a hundred lines:
///
/// * Reads and queries are answered by an in-memory [MemoryQueueStorage],
///   which already implements the ordering and indexes the contract needs.
/// * Every write updates memory, then rewrites the whole file atomically
///   (write a temporary file, then rename it over the old one). If the file
///   write fails, the in-memory change is undone and the error is rethrown,
///   so memory never claims something the disk does not have.
/// * Writes are serialized, so concurrent calls cannot interleave.
///
/// Tasks survive a process restart: open the same path on the next launch
/// and call `DurableQueue.start`.
///
/// Rewriting the file on every change is fine for hundreds of tasks. For
/// large queues, use a database-backed adapter that writes one row at a
/// time. This class lives in `example/` and is not part of the package API.
final class JsonFileStorage implements QueueStorage {
  /// Uses the file at [path], creating it on the first write.
  ///
  /// The file is loaded lazily; a corrupt file surfaces as a
  /// [FormatException] from the first operation.
  JsonFileStorage(String path) : _file = File(path) {
    _ready = _load();
  }

  final File _file;
  final _memory = MemoryQueueStorage();
  late final Future<void> _ready;
  Future<void> _tail = Future<void>.value();

  Future<void> _load() async {
    if (!await _file.exists()) return;
    final decoded = jsonDecode(await _file.readAsString());
    if (decoded is! Map || decoded['tasks'] is! List) {
      throw FormatException('Not a durable_queue file: ${_file.path}');
    }
    for (final record in decoded['tasks'] as List) {
      if (record is! Map) throw FormatException('Bad task record: $record');
      await _memory.save(
        StoredTask.fromJson(Map<String, dynamic>.from(record)),
      );
    }
  }

  // Reads.

  Future<T> _read<T>(Future<T> Function() query) async {
    await _ready;
    return query();
  }

  @override
  Future<StoredTask?> get(String id) => _read(() => _memory.get(id));

  @override
  Future<List<StoredTask>> getAll() => _read(_memory.getAll);

  @override
  Future<List<StoredTask>> getPending() => _read(_memory.getPending);

  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit}) =>
      _read(() => _memory.getByStatus(status, limit: limit));

  @override
  Future<StoredTask?> getNextReady(DateTime now) =>
      _read(() => _memory.getNextReady(now));

  @override
  Future<DateTime?> getNextWakeAt() => _read(_memory.getNextWakeAt);

  @override
  Future<int> getMaxSequence() => _read(_memory.getMaxSequence);

  @override
  Future<StoredTask?> findActiveByDeduplicationKey(String key) =>
      _read(() => _memory.findActiveByDeduplicationKey(key));

  @override
  Future<List<StoredTask>> getWaitingDependents(String id) =>
      _read(() => _memory.getWaitingDependents(id));

  @override
  Future<List<StoredTask>> getByGroup(String group, {TaskStatus? status}) =>
      _read(() => _memory.getByGroup(group, status: status));

  // Writes.

  @override
  Future<void> save(StoredTask task) => _write(
    apply: () => _memory.save(task),
    undo: () => _memory.delete(task.id),
  );

  @override
  Future<void> update(StoredTask task) async {
    await _ready;
    final previous = await _memory.get(task.id);
    return _write(
      apply: () => _memory.update(task),
      undo: () async {
        if (previous != null) await _memory.update(previous);
      },
    );
  }

  @override
  Future<void> delete(String id) async {
    await _ready;
    final previous = await _memory.get(id);
    if (previous == null) return;
    return _write(
      apply: () => _memory.delete(id),
      undo: () => _memory.save(previous),
    );
  }

  /// Applies a change in memory, then persists it. Runs one at a time.
  Future<void> _write({
    required Future<void> Function() apply,
    required Future<void> Function() undo,
  }) {
    final result = _tail.then((_) async {
      await _ready;
      await apply();
      try {
        await _flush();
      } catch (_) {
        await undo();
        rethrow;
      }
    });
    _tail = result.then<void>((_) {}, onError: (Object _) {});
    return result;
  }

  Future<void> _flush() async {
    final tasks = await _memory.getAll();
    final temp = File('${_file.path}.tmp');
    await temp.writeAsString(
      jsonEncode({
        'format': 1,
        'tasks': [for (final task in tasks) task.toJson()],
      }),
      flush: true,
    );
    await temp.rename(_file.path);
  }
}
