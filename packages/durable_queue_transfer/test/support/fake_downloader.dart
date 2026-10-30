import 'package:background_downloader/background_downloader.dart';

final class FakeDatabase implements Database {
  final records = <String, TaskRecord>{};

  @override
  Future<TaskRecord?> recordForId(String taskId) async => records[taskId];

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

/// Records calls instead of talking to the platform.
final class FakeDownloader implements FileDownloader {
  final enqueued = <Task>[];
  var enqueueResult = true;
  final callbacks = <String, TaskStatusCallback>{};
  final tracked = <String>[];
  var resumeCalls = 0;
  var rescheduleCalls = 0;
  final native = <String, Task>{};
  final cancelled = <String>[];
  final pauses = <String>[];
  final resumes = <String>[];
  var resumeResult = true;
  final unregistered = <String>[];
  final fakeDatabase = FakeDatabase();

  @override
  Database get database => fakeDatabase;

  @override
  FileDownloader registerCallbacks({
    String group = FileDownloader.defaultGroup,
    TaskStatusCallback? taskStatusCallback,
    Object? taskProgressCallback,
    Object? taskNotificationTapCallback,
  }) {
    if (taskStatusCallback != null) callbacks[group] = taskStatusCallback;
    return this;
  }

  @override
  FileDownloader unregisterCallbacks({
    String group = FileDownloader.defaultGroup,
    Function? callback,
  }) {
    unregistered.add(group);
    callbacks.remove(group);
    return this;
  }

  @override
  Future<bool> enqueue(Task task) async {
    enqueued.add(task);
    if (enqueueResult) native[task.taskId] = task;
    return enqueueResult;
  }

  @override
  Future<FileDownloader> trackTasksInGroup(
    String group, {
    bool markDownloadedComplete = true,
  }) async {
    tracked.add(group);
    return this;
  }

  @override
  Future<void> resumeFromBackground() async => resumeCalls++;

  @override
  Future<(List<Task>, List<Task>)> rescheduleKilledTasks() async {
    rescheduleCalls++;
    return (<Task>[], <Task>[]);
  }

  @override
  Future<Task?> taskForId(String taskId) async => native[taskId];

  @override
  Future<bool> cancelTaskWithId(String taskId) async {
    cancelled.add(taskId);
    return true;
  }

  @override
  Future<bool> pause(DownloadTask task) async {
    pauses.add(task.taskId);
    if (!task.allowPause) return false;
    native.remove(task.taskId);
    fakeDatabase.records[task.taskId] = TaskRecord(
      task,
      TaskStatus.paused,
      0.5,
      100,
    );
    return true;
  }

  @override
  Future<bool> resume(DownloadTask task) async {
    resumes.add(task.taskId);
    fakeDatabase.records.remove(task.taskId);
    if (!resumeResult) return false;
    native[task.taskId] = task;
    return true;
  }

  /// Delivers [update] as background_downloader would.
  void emit(TaskStatusUpdate update) {
    if (update.status.isFinalState) native.remove(update.task.taskId);
    callbacks[update.task.group]?.call(update);
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
