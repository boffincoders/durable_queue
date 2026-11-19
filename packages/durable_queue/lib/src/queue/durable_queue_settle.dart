part of 'durable_queue.dart';

/// Dependencies and state changes: checks a task's dependencies, writes a
/// change with the tasks it settles, and recovers interrupted attempts.
extension _Settle on DurableQueue {
  /// Checks [dependsOn] for a task with [taskId] and [policy].
  ///
  /// [overlay] holds statuses decided in this operation but not yet written.
  Future<_Verdict> _evaluate(
    String taskId,
    List<String> dependsOn,
    DependencyFailurePolicy policy,
    _StorageIo io, [
    Map<String, TaskStatus> overlay = const {},
  ]) async {
    var allFinished = true;
    DependencyFailedException? blocker;
    for (final id in dependsOn) {
      final status =
          overlay[id] ?? (await io(() => _storage.get(id), 'get'))?.status;
      if (status == TaskStatus.completed) continue;
      if (status == null || status.isTerminal) {
        blocker ??= DependencyFailedException(
          taskId: taskId,
          dependencyId: id,
          dependencyStatus: status,
        );
        if (policy != DependencyFailurePolicy.run) break;
        continue;
      }
      allFinished = false;
    }
    if (blocker != null) {
      switch (policy) {
        case DependencyFailurePolicy.cancel:
          return _Verdict(_VerdictKind.cancel, blocker);
        case DependencyFailurePolicy.fail:
          return _Verdict(_VerdictKind.fail, blocker);
        case DependencyFailurePolicy.run:
          break;
      }
    }
    return allFinished ? _Verdict.ready : _Verdict.wait;
  }

  /// Applies [verdict] to a task that is, or is about to be, waiting.
  _Resolved _applyVerdict(StoredTask task, _Verdict verdict, DateTime now) {
    switch (verdict.kind) {
      case _VerdictKind.wait:
        return _Resolved(
          task.status == TaskStatus.waiting
              ? task
              : task.copyWith(status: TaskStatus.waiting, updatedAt: now),
          null,
        );
      case _VerdictKind.ready:
        // A start time from enqueue(runAt:) is kept; otherwise
        // nextAttemptAt is null and the task is ready at once.
        return _Resolved(
          task.copyWith(status: TaskStatus.pending, updatedAt: now),
          null,
        );
      case _VerdictKind.cancel:
      case _VerdictKind.fail:
        final failure = TaskFailure(
          error: verdict.blocker.toString(),
          stackTrace: null,
          failedAt: now,
          attempt: task.attempts,
        );
        final cancel = verdict.kind == _VerdictKind.cancel;
        return _Resolved(
          task.copyWith(
            status: cancel ? TaskStatus.cancelled : TaskStatus.failed,
            updatedAt: now,
            nextAttemptAt: null,
            lastFailure: failure,
            cancelRequested: false,
          ),
          cancel
              ? TaskCancelled(
                  taskId: task.id,
                  taskType: task.type,
                  occurredAt: now,
                  wasRunning: false,
                )
              : TaskFailed(
                  taskId: task.id,
                  taskType: task.type,
                  occurredAt: now,
                  failure: failure,
                ),
        );
    }
  }

  /// Writes [root] together with every change it causes in waiting tasks,
  /// and returns the events in causal order.
  ///
  /// When [root] is terminal, its dependents are released, cancelled, or
  /// failed, cascading through their own dependents. Those records are
  /// written deepest first and [root] last, so a crash part-way through can
  /// never leave a task waiting behind a dependency that already finished:
  /// the unfinished part of the cascade is redone when [root] itself is
  /// recovered or finishes again. Must run inside [_lock].
  Future<List<QueueEvent>> _commit(_Resolved root, _StorageIo io) async {
    final cascade = root.task.status.isTerminal
        ? await _planDependents(root.task.id, root.task.status, io)
        : const <_Resolved>[];
    for (final step in cascade.reversed) {
      final task = _withHistory(DurableQueue._unheldIfFinished(step.task));
      await io(() => _storage.update(task), 'update');
    }
    final task = _withHistory(DurableQueue._unheldIfFinished(root.task));
    await io(() => _storage.update(task), 'update');
    return [?root.event, for (final step in cascade) ?step.event];
  }

  /// Keeps [StoredTask.failures] ending with the current last failure, up to
  /// [failureHistory] entries.
  StoredTask _withHistory(StoredTask task) {
    final failure = task.lastFailure;
    if (failureHistory == 0) {
      return task.failures.isEmpty ? task : task.copyWith(failures: const []);
    }
    if (failure == null) return task;
    final history = task.failures;
    if (history.isNotEmpty &&
        DurableQueue._sameFailure(history.last, failure)) {
      return task;
    }
    final next = [...history, failure];
    final start = next.length > failureHistory
        ? next.length - failureHistory
        : 0;
    return task.copyWith(failures: next.sublist(start));
  }

  /// Decides, without writing, how tasks waiting on [id] change when [id]
  /// moves to the terminal [status]. Returns changes in discovery order:
  /// every task appears after the dependency that released it.
  Future<List<_Resolved>> _planDependents(
    String id,
    TaskStatus status,
    _StorageIo io,
  ) async {
    final overlay = <String, TaskStatus>{id: status};
    final planned = <_Resolved>[];
    final work = [id];
    final now = _clock.now();
    while (work.isNotEmpty) {
      final finished = work.removeLast();
      final dependents = await io(
        () => _storage.getWaitingDependents(finished),
        'getWaitingDependents',
      );
      for (final listed in dependents) {
        if (overlay.containsKey(listed.id)) continue;
        final current = await io(() => _storage.get(listed.id), 'get');
        if (current == null || current.status != TaskStatus.waiting) continue;
        final verdict = await _evaluate(
          current.id,
          current.dependsOn,
          current.onDependencyFailure,
          io,
          overlay,
        );
        if (verdict.kind == _VerdictKind.wait) continue;
        final resolved = _applyVerdict(current, verdict, now);
        planned.add(resolved);
        overlay[current.id] = resolved.task.status;
        if (resolved.task.status.isTerminal) work.add(current.id);
      }
    }
    return planned;
  }

  void _emitAll(List<QueueEvent> events) {
    for (final event in events) {
      _emit(event);
    }
  }

  /// Decides the record and event for a task left running by a previous
  /// process. Writes nothing; [start] commits the result.
  _Resolved _recoverInterrupted(StoredTask task) {
    final now = _clock.now();
    if (task.cancelRequested) {
      _cancelRequested.remove(task.id);
      return _Resolved(
        task.copyWith(
          status: TaskStatus.cancelled,
          updatedAt: now,
          nextAttemptAt: null,
        ),
        TaskCancelled(
          taskId: task.id,
          taskType: task.type,
          occurredAt: now,
          wasRunning: true,
        ),
      );
    }

    if (task.held) {
      // The user held it; the interrupted attempt does not count.
      final attempts = task.attempts > 0 ? task.attempts - 1 : 0;
      return _Resolved(
        task.copyWith(
          status: attempts > 0 ? TaskStatus.retryScheduled : TaskStatus.pending,
          attempts: attempts,
          updatedAt: now,
          nextAttemptAt: StoredTask.heldNextAttemptAt,
          releaseAt: null,
        ),
        null,
      );
    }

    final attempts = task.attempts < 1 ? 1 : task.attempts;
    const interrupted = TaskInterruptedException();
    final failure = TaskFailure(
      error: interrupted.toString(),
      stackTrace: null,
      failedAt: now,
      attempt: attempts,
    );
    final normalized = task.attempts == attempts
        ? task
        : task.copyWith(attempts: attempts);

    if (attempts >= task.retryPolicy.maxAttempts) {
      return _Resolved(
        normalized.copyWith(
          status: TaskStatus.failed,
          updatedAt: now,
          nextAttemptAt: null,
          lastFailure: failure,
          cancelRequested: false,
        ),
        TaskFailed(
          taskId: task.id,
          taskType: task.type,
          occurredAt: now,
          failure: failure,
        ),
      );
    }

    // The worker is still idle here; the first pump arms the wake-up timer.
    final delay = task.retryPolicy.delayAfter(attempts, _nextFraction());
    final nextAttemptAt = now.add(delay);
    return _Resolved(
      normalized.copyWith(
        status: TaskStatus.retryScheduled,
        updatedAt: now,
        nextAttemptAt: nextAttemptAt,
        lastFailure: failure,
      ),
      TaskRetryScheduled(
        taskId: task.id,
        taskType: task.type,
        occurredAt: now,
        attempt: attempts,
        maxAttempts: task.retryPolicy.maxAttempts,
        delay: delay,
        nextAttemptAt: nextAttemptAt,
        error: failure.error,
      ),
    );
  }
}
