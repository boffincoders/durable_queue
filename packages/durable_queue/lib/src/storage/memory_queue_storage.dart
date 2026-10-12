import 'dart:collection';

import '../task/stored_task.dart';
import '../task/task_status.dart';
import 'queue_storage.dart';

/// Indexed in-memory [QueueStorage].
///
/// Records live until this instance is discarded and do not survive process
/// death. Each operation updates all indexes synchronously in one isolate.
final class MemoryQueueStorage implements QueueStorage {
  /// Creates an empty in-memory store.
  MemoryQueueStorage();

  final Map<String, StoredTask> _tasks = {};
  final _byStatus = <TaskStatus, SplayTreeSet<StoredTask>>{};
  final _byKey = <String, SplayTreeSet<StoredTask>>{};
  final _byGroup = <String, SplayTreeSet<StoredTask>>{};
  final _waitingOn = <String, SplayTreeSet<StoredTask>>{};
  final _sequences = SplayTreeMap<int, int>();
  final _ready = SplayTreeSet<StoredTask>(compareReadyTasks);
  final _future = SplayTreeSet<StoredTask>(_compareTime);
  final _due = SplayTreeSet<StoredTask>(_compareTime);
  DateTime? _indexedAt;

  @override
  Future<void> save(StoredTask task) async {
    if (_tasks.containsKey(task.id)) {
      throw StateError('Task "${task.id}" already exists');
    }
    _tasks[task.id] = task;
    _add(task);
  }

  @override
  Future<StoredTask?> get(String id) async => _tasks[id];

  @override
  Future<List<StoredTask>> getPending() => getByStatus(TaskStatus.pending);

  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit}) async {
    if (limit != null && limit < 1) {
      throw ArgumentError.value(limit, 'limit', 'Must be positive');
    }
    final tasks = _byStatus[status] ?? const <StoredTask>[];
    return List<StoredTask>.unmodifiable(
      limit == null ? tasks : tasks.take(limit),
    );
  }

  @override
  Future<List<StoredTask>> getAll() async => List<StoredTask>.unmodifiable(
    _tasks.values.toList()..sort(compareStoredTasks),
  );

  @override
  Future<StoredTask?> getNextReady(DateTime now) async {
    // Move only tasks that crossed the eligibility boundary. Keep both time
    // indexes so a wall-clock correction backwards is safe as well.
    while (_due.isNotEmpty && _due.last.nextAttemptAt!.isAfter(now)) {
      final task = _due.last;
      _due.remove(task);
      _ready.remove(task);
      _future.add(task);
    }
    while (_future.isNotEmpty && !_future.first.nextAttemptAt!.isAfter(now)) {
      final task = _future.first;
      _future.remove(task);
      _due.add(task);
      _ready.add(task);
    }
    _indexedAt = now;
    return _ready.isEmpty ? null : _ready.first;
  }

  @override
  Future<DateTime?> getNextWakeAt() async {
    if (_due.isNotEmpty) return _due.first.nextAttemptAt;
    return _future.isEmpty ? null : _future.first.nextAttemptAt;
  }

  @override
  Future<int> getMaxSequence() async => _sequences.lastKey() ?? 0;

  @override
  Future<StoredTask?> findActiveByDeduplicationKey(String key) async {
    final tasks = _byKey[key];
    return tasks == null || tasks.isEmpty ? null : tasks.first;
  }

  @override
  Future<List<StoredTask>> getWaitingDependents(String id) async {
    final tasks = _waitingOn[id];
    return List<StoredTask>.unmodifiable(tasks ?? const <StoredTask>[]);
  }

  @override
  Future<List<StoredTask>> getByGroup(
    String group, {
    TaskStatus? status,
  }) async {
    final tasks = _byGroup[group] ?? const <StoredTask>[];
    return List<StoredTask>.unmodifiable(
      status == null ? tasks : tasks.where((task) => task.status == status),
    );
  }

  @override
  Future<void> update(StoredTask task) async {
    final previous = _tasks[task.id];
    if (previous == null) {
      throw StateError('Task "${task.id}" does not exist');
    }
    _remove(previous);
    _tasks[task.id] = task;
    _add(task);
  }

  @override
  Future<void> delete(String id) async {
    final task = _tasks.remove(id);
    if (task != null) _remove(task);
  }

  void _add(StoredTask task) {
    (_byStatus[task.status] ??= SplayTreeSet(compareStoredTasks)).add(task);
    _sequences.update(task.sequence, (count) => count + 1, ifAbsent: () => 1);
    if (_active.contains(task.status) && task.deduplicationKey != null) {
      (_byKey[task.deduplicationKey!] ??= SplayTreeSet(
        compareStoredTasks,
      )).add(task);
    }
    final group = task.group;
    if (group != null) {
      (_byGroup[group] ??= SplayTreeSet(compareStoredTasks)).add(task);
    }
    if (task.status == TaskStatus.waiting) {
      for (final dependency in task.dependsOn) {
        (_waitingOn[dependency] ??= SplayTreeSet(compareStoredTasks)).add(task);
      }
    }
    if (!_schedulable.contains(task.status)) return;
    final at = task.nextAttemptAt;
    if (at == null) {
      _ready.add(task);
    } else if (_indexedAt != null && !at.isAfter(_indexedAt!)) {
      _due.add(task);
      _ready.add(task);
    } else {
      _future.add(task);
    }
  }

  void _remove(StoredTask task) {
    _byStatus[task.status]?.remove(task);
    final key = task.deduplicationKey;
    if (key != null) {
      final tasks = _byKey[key];
      tasks?.remove(task);
      if (tasks != null && tasks.isEmpty) _byKey.remove(key);
    }
    final group = task.group;
    if (group != null) _removeFrom(_byGroup, group, task);
    if (task.status == TaskStatus.waiting) {
      for (final dependency in task.dependsOn) {
        _removeFrom(_waitingOn, dependency, task);
      }
    }
    final count = _sequences[task.sequence]!;
    if (count == 1) {
      _sequences.remove(task.sequence);
    } else {
      _sequences[task.sequence] = count - 1;
    }
    _ready.remove(task);
    if (task.nextAttemptAt != null) {
      _future.remove(task);
      _due.remove(task);
    }
  }

  static void _removeFrom(
    Map<String, SplayTreeSet<StoredTask>> index,
    String key,
    StoredTask task,
  ) {
    final tasks = index[key];
    if (tasks == null) return;
    tasks.remove(task);
    if (tasks.isEmpty) index.remove(key);
  }

  static const _schedulable = {TaskStatus.pending, TaskStatus.retryScheduled};
  static const _active = {
    ..._schedulable,
    TaskStatus.running,
    TaskStatus.waiting,
  };

  static int _compareTime(StoredTask a, StoredTask b) {
    final order = a.nextAttemptAt!.compareTo(b.nextAttemptAt!);
    return order == 0 ? compareStoredTasks(a, b) : order;
  }
}
