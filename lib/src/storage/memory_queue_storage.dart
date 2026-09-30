import '../task/stored_task.dart';
import '../task/task_status.dart';
import 'queue_storage.dart';

/// In-memory [QueueStorage].
///
/// Useful for tests, examples, and applications that do not need tasks to
/// survive process death. Records live until this instance is discarded.
///
/// Each operation mutates the map synchronously, so a single isolate cannot
/// observe a torn write.
final class MemoryQueueStorage implements QueueStorage {
  final Map<String, StoredTask> _tasks = {};

  @override
  Future<void> save(StoredTask task) async {
    if (_tasks.containsKey(task.id)) {
      throw StateError('Task "${task.id}" already exists');
    }
    _tasks[task.id] = task;
  }

  @override
  Future<StoredTask?> get(String id) async => _tasks[id];

  @override
  Future<List<StoredTask>> getPending() => getByStatus(TaskStatus.pending);

  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status) async {
    return _sorted(_tasks.values.where((task) => task.status == status));
  }

  @override
  Future<List<StoredTask>> getAll() async => _sorted(_tasks.values);

  @override
  Future<StoredTask?> findActiveByDeduplicationKey(String key) async {
    for (final task in _sorted(_tasks.values)) {
      if (task.deduplicationKey != key) continue;
      if (_active.contains(task.status)) return task;
    }
    return null;
  }

  @override
  Future<void> update(StoredTask task) async {
    if (!_tasks.containsKey(task.id)) {
      throw StateError('Task "${task.id}" does not exist');
    }
    _tasks[task.id] = task;
  }

  @override
  Future<void> delete(String id) async {
    _tasks.remove(id);
  }

  static const Set<TaskStatus> _active = {
    TaskStatus.pending,
    TaskStatus.running,
    TaskStatus.retryScheduled,
  };

  static List<StoredTask> _sorted(Iterable<StoredTask> tasks) {
    final copy = tasks.toList()..sort(compareStoredTasks);
    return List<StoredTask>.unmodifiable(copy);
  }
}
