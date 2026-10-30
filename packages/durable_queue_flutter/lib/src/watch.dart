import 'dart:async';

import 'package:durable_queue/durable_queue.dart';

/// Streams of stored tasks for building UI.
extension DurableQueueWatch on DurableQueue {
  /// The stored record of task [id], now and after every event about it.
  ///
  /// Emits the current record when listened to (null if it is not stored),
  /// then again after each event for [id]: enqueued, started, retry
  /// scheduled, deferred, completed, failed, or cancelled, and after it is
  /// held or released (see `DurableQueue.holdChanges`). Records that did
  /// not change are not repeated. `delete` and `purge` emit no events, so a
  /// deleted task keeps its last value until the next event for [id].
  ///
  /// The stream ends when the queue is closed.
  Stream<StoredTask?> watchTask(String id) {
    return _watch<StoredTask?>(
      read: () => getTask(id),
      affects: (taskId, isEnqueue) => taskId == id,
      same: (a, b) => _sameRecord(a, b),
    );
  }

  /// The tasks in [group], optionally only those with [status], now and
  /// whenever one of them may have changed.
  ///
  /// Emits the current list when listened to (oldest first), then again
  /// after events and hold changes for tasks in the last list and after
  /// every enqueue (a new task may join the group). Bursts of events cause one reload. Each
  /// reload runs `getTasks(group:)`, so keep groups to the size of a screen,
  /// for example one album's uploads.
  ///
  /// The stream ends when the queue is closed.
  Stream<List<StoredTask>> watchGroup(String group, {TaskStatus? status}) {
    if (group.isEmpty) {
      throw ArgumentError.value(group, 'group', 'Must not be empty');
    }
    var ids = <String>{};
    return _watch<List<StoredTask>>(
      read: () async {
        final tasks = await getTasks(group: group, status: status);
        ids = {for (final task in tasks) task.id};
        return tasks;
      },
      affects: (taskId, isEnqueue) => isEnqueue || ids.contains(taskId),
      same: (a, b) {
        if (a.length != b.length) return false;
        for (var i = 0; i < a.length; i++) {
          if (!_sameRecord(a[i], b[i])) return false;
        }
        return true;
      },
    );
  }

  Stream<T> _watch<T>({
    required Future<T> Function() read,
    required bool Function(String taskId, bool isEnqueue) affects,
    required bool Function(T a, T b) same,
  }) {
    late StreamController<T> controller;
    StreamSubscription<QueueEvent>? subscription;
    StreamSubscription<TaskHoldChanged>? holds;
    var hasValue = false;
    late T last;
    var loading = false;
    var again = false;

    Future<void> reload() async {
      if (loading) {
        again = true;
        return;
      }
      loading = true;
      try {
        do {
          again = false;
          final T value;
          try {
            value = await read();
          } on Object catch (error, stackTrace) {
            if (!controller.isClosed) controller.addError(error, stackTrace);
            return;
          }
          if (controller.isClosed) return;
          if (hasValue && same(last, value)) continue;
          hasValue = true;
          last = value;
          controller.add(value);
        } while (again);
      } finally {
        loading = false;
      }
    }

    controller = StreamController<T>(
      onListen: () {
        holds = holdChanges.listen((change) {
          if (affects(change.taskId, false)) reload();
        });
        subscription = events.listen(
          (event) {
            if (affects(event.taskId, event is TaskEnqueued)) reload();
          },
          onDone: () async {
            // Let a reload in progress deliver its value first.
            while (loading) {
              await Future<void>.delayed(Duration.zero);
            }
            await controller.close();
          },
        );
        reload();
      },
      onCancel: () async {
        await holds?.cancel();
        await subscription?.cancel();
      },
    );
    return controller.stream;
  }
}

bool _sameRecord(StoredTask? a, StoredTask? b) {
  if (identical(a, b)) return true;
  if (a == null || b == null) return false;
  return a.id == b.id &&
      a.status == b.status &&
      a.attempts == b.attempts &&
      a.updatedAt == b.updatedAt &&
      a.nextAttemptAt == b.nextAttemptAt &&
      a.held == b.held;
}
