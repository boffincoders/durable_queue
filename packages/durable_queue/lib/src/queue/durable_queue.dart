import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:math';

import '../clock/queue_clock.dart';
import '../errors.dart';
import '../events/queue_event.dart';
import '../internal/async_lock.dart';
import '../registry/task_handler.dart';
import '../registry/task_registry.dart';
import '../retry/retry_policy.dart';
import '../storage/queue_storage.dart';
import '../task/dependency_failure_policy.dart';
import '../task/durable_task.dart';
import '../task/stored_task.dart';
import '../task/task_context.dart';
import '../task/task_failure.dart';
import '../task/task_status.dart';
import 'queue_run_state.dart';

/// Supplies the `[0, 1)` fraction used by jitter.
typedef RandomFraction = double Function();

/// Supplies a new task id.
typedef TaskIdGenerator = String Function();

final _secureRandom = Random.secure();

String _defaultTaskId() {
  final bytes = List<int>.generate(16, (_) => _secureRandom.nextInt(256));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  final buffer = StringBuffer();
  for (var i = 0; i < bytes.length; i++) {
    if (i == 4 || i == 6 || i == 8 || i == 10) buffer.write('-');
    buffer.write(bytes[i].toRadixString(16).padLeft(2, '0'));
  }
  return buffer.toString();
}

/// Durable task queue.
///
/// Give the queue a serializable [DurableTask] and a registered handler. It
/// persists the task, runs it, and owns retries, concurrency, deduplication,
/// cancellation, and restart recovery.
///
/// ## Delivery
///
/// Execution is **at least once**. A crash after a handler's side effect but
/// before the completion write will run that task again. Use idempotent
/// handlers or [enqueue]'s `idempotencyKey` when a duplicate side effect
/// would be harmful. The key is stored and exposed on [TaskContext]; this
/// package does not contact a backend.
///
/// ## Process death is not background execution
///
/// Tasks stay in [storage] when the process dies. Nothing in this package
/// asks the operating system to relaunch a terminated app. Call [start] again
/// on the next launch. Tasks left in [TaskStatus.running] are treated as
/// interrupted attempts: the attempt counts, and the retry policy decides
/// whether to schedule another try or fail the task.
///
/// ## Controls
///
/// * [start] recovers interrupted tasks, then starts eligible work.
/// * [pause] lets in-flight handlers finish and starts nothing new.
/// * [resume] continues a paused queue.
/// * [stop] starts nothing new, waits for in-flight handlers, and leaves
///   every other task stored. [stop] does not complete if a handler never
///   returns.
///
/// Only one [DurableQueue] should consume a given [storage] at a time.
final class DurableQueue {
  /// Creates a queue that persists tasks in [storage].
  ///
  /// [maxConcurrentTasks] must be at least 1. [clock] defaults to the system
  /// clock. [randomFraction] must return values in `[0, 1)` and is used only
  /// when a retry policy enables jitter. [idGenerator] defaults to a random
  /// UUID v4 string. [storageRetryDelay] must be positive and defaults to one
  /// second; worker storage errors are reported through [storageErrors].
  DurableQueue({
    required QueueStorage storage,
    QueueClock? clock,
    this.maxConcurrentTasks = 1,
    this.storageRetryDelay = const Duration(seconds: 1),
    RandomFraction? randomFraction,
    TaskIdGenerator? idGenerator,
  }) : _storage = storage,
       _clock = clock ?? const SystemQueueClock(),
       _randomFraction = randomFraction ?? Random().nextDouble,
       _idGenerator = idGenerator ?? _defaultTaskId {
    if (storageRetryDelay <= Duration.zero) {
      throw ArgumentError.value(
        storageRetryDelay,
        'storageRetryDelay',
        'Must be positive',
      );
    }
    if (maxConcurrentTasks < 1) {
      throw ArgumentError.value(
        maxConcurrentTasks,
        'maxConcurrentTasks',
        'Must be at least 1',
      );
    }
  }

  /// Storage this queue reads and writes.
  final QueueStorage _storage;

  final QueueClock _clock;
  final RandomFraction _randomFraction;
  final TaskIdGenerator _idGenerator;

  /// Maximum number of handlers running at once.
  final int maxConcurrentTasks;

  /// Delay between retries of worker storage operations that throw.
  ///
  /// The worker retains its outcome and slot until the write succeeds. Public
  /// calls and startup recovery report storage errors to their caller instead.
  final Duration storageRetryDelay;

  final TaskRegistry _registry = TaskRegistry();
  final AsyncLock _lock = AsyncLock();
  final StreamController<QueueEvent> _events =
      StreamController<QueueEvent>.broadcast();
  final StreamController<QueueStorageFailure> _storageErrors =
      StreamController<QueueStorageFailure>.broadcast();
  final Set<String> _cancelRequested = {};

  QueueRunState _runState = QueueRunState.idle;
  var _inFlight = 0;
  var _pumping = false;
  var _pumpAgain = false;
  Completer<void>? _drain;
  var _sleepGeneration = 0;
  int? _sequence;
  DateTime? _wakeAt;
  QueueDelay? _activeDelay;
  var _closed = false;
  Future<void>? _closing;

  /// Whether the worker is idle, running, paused, or stopping.
  QueueRunState get runState => _runState;

  /// Whether [close] has been called.
  bool get isClosed => _closed;

  /// Lifecycle events. This is a broadcast stream and does not replay.
  Stream<QueueEvent> get events => _events.stream;

  /// Worker storage failures, reported before each automatic retry.
  ///
  /// A broadcast stream of values, not uncaught stream errors. Subscribe before
  /// starting the worker. An outage holds serialized storage operations and
  /// may delay pause, stop, enqueue, cancellation, and queries until recovery.
  Stream<QueueStorageFailure> get storageErrors => _storageErrors.stream;

  /// Registers the handler for [type].
  ///
  /// [type] must be non-empty and must match `DurableTask.type` for tasks of
  /// this class. Registering the same [type] twice throws [StateError].
  ///
  /// [retryIf] defaults to retrying every handler error until the task's
  /// [RetryPolicy] is exhausted. [TaskCancelledException] is never retried.
  /// Decode failures are never retried. A [retryIf] that throws rejects the
  /// retry.
  void register<T extends DurableTask>({
    required String type,
    required TaskDecoder<T> decoder,
    required TaskHandler<T> handler,
    RetryPredicate? retryIf,
  }) {
    _ensureOpen();
    _registry.register<T>(
      type: type,
      decoder: decoder,
      handler: handler,
      retryIf: retryIf,
    );
  }

  /// Persists [task] and returns its id.
  ///
  /// The returned future completes when the task is stored, not when it
  /// finishes. The task does not start until [start] is called.
  ///
  /// When [deduplicationKey] is set and another task with that key is
  /// `pending`, `waiting`, `running`, or `retryScheduled`, this returns the
  /// existing id and leaves that task unchanged. Completed, failed, and
  /// cancelled tasks do not count, so the same key can be enqueued again
  /// after a terminal state. The new payload is not merged into the existing
  /// task.
  ///
  /// [idempotencyKey] is stored and passed to the handler on [TaskContext].
  ///
  /// [retryPolicy] defaults to [RetryPolicy.none].
  ///
  /// [priority] orders eligible tasks: higher values start first, and tasks
  /// with equal priority keep enqueue order. It does not pre-empt a handler
  /// that is already running. Defaults to zero.
  ///
  /// [dependsOn] lists ids of stored tasks that must finish first. Until then
  /// the task is [TaskStatus.waiting]. It becomes [TaskStatus.pending] when
  /// every dependency is [TaskStatus.completed]. If a dependency fails, is
  /// cancelled, or is missing, [onDependencyFailure] decides whether this
  /// task is cancelled (the default), failed, or run anyway once every
  /// dependency has finished.
  ///
  /// [group] is a label for [getTasks] and [cancelGroup].
  ///
  /// Throws [UnknownTaskTypeException] if [task]'s type is not registered,
  /// [TaskNotFoundException] if a [dependsOn] id is not stored, and
  /// [ArgumentError] if the payload is not JSON-encodable or a key, group,
  /// or dependency id is empty.
  Future<String> enqueue(
    DurableTask task, {
    RetryPolicy? retryPolicy,
    String? deduplicationKey,
    String? idempotencyKey,
    int priority = 0,
    Iterable<String> dependsOn = const [],
    DependencyFailurePolicy onDependencyFailure =
        DependencyFailurePolicy.cancel,
    String? group,
  }) async {
    _ensureOpen();
    final request = _prepare(
      task,
      retryPolicy: retryPolicy,
      deduplicationKey: deduplicationKey,
      idempotencyKey: idempotencyKey,
      priority: priority,
      dependsOn: dependsOn,
      onDependencyFailure: onDependencyFailure,
      group: group,
    );
    final events = <QueueEvent>[];
    final id = await _lock.synchronized(() => _store(request, events));
    _emitAll(events);
    if (events.isNotEmpty && _runState == QueueRunState.running) {
      unawaited(_pump());
    }
    return id;
  }

  /// Persists [tasks] as a chain and returns their ids in order.
  ///
  /// Each task depends on the one before it, so they run one after another
  /// even when [maxConcurrentTasks] is greater than one. The first task
  /// depends on [dependsOn], if given. [retryPolicy], [priority], [group],
  /// and [onDependencyFailure] apply to every task. With the default
  /// [DependencyFailurePolicy.cancel], a step that fails or is cancelled
  /// cancels the rest of the chain.
  ///
  /// Every task is validated before anything is stored, and the chain is
  /// stored in one serialized operation. A storage error part-way through
  /// can still leave the first steps stored; they are returned by
  /// [getTasks] like any other task.
  ///
  /// Throws [ArgumentError] if [tasks] is empty, plus everything [enqueue]
  /// throws.
  Future<List<String>> enqueueChain(
    List<DurableTask> tasks, {
    RetryPolicy? retryPolicy,
    int priority = 0,
    Iterable<String> dependsOn = const [],
    DependencyFailurePolicy onDependencyFailure =
        DependencyFailurePolicy.cancel,
    String? group,
  }) async {
    _ensureOpen();
    if (tasks.isEmpty) {
      throw ArgumentError.value(tasks, 'tasks', 'Must not be empty');
    }
    final requests = [
      for (var i = 0; i < tasks.length; i++)
        _prepare(
          tasks[i],
          retryPolicy: retryPolicy,
          priority: priority,
          onDependencyFailure: onDependencyFailure,
          group: group,
          dependsOn: i == 0 ? dependsOn : const [],
        ),
    ];
    final events = <QueueEvent>[];
    final ids = await _lock.synchronized(() async {
      final stored = <String>[];
      for (var i = 0; i < requests.length; i++) {
        final request = i == 0
            ? requests.first
            : requests[i].withDependencies([stored.last]);
        stored.add(await _store(request, events));
      }
      return stored;
    });
    _emitAll(events);
    if (_runState == QueueRunState.running) unawaited(_pump());
    return ids;
  }

  _EnqueueRequest _prepare(
    DurableTask task, {
    RetryPolicy? retryPolicy,
    String? deduplicationKey,
    String? idempotencyKey,
    required int priority,
    required Iterable<String> dependsOn,
    required DependencyFailurePolicy onDependencyFailure,
    required String? group,
  }) {
    if (task.type.isEmpty) {
      throw ArgumentError.value(task.type, 'type', 'Must not be empty');
    }
    _rejectBlank(deduplicationKey, 'deduplicationKey');
    _rejectBlank(idempotencyKey, 'idempotencyKey');
    _rejectBlank(group, 'group');
    final dependencies = List<String>.unmodifiable(
      LinkedHashSet<String>.of(dependsOn),
    );
    for (final dependency in dependencies) {
      _rejectBlank(dependency, 'dependsOn');
    }
    if (!_registry.contains(task.type)) {
      throw _registry.unknownType(task.type);
    }
    return _EnqueueRequest(
      type: task.type,
      payload: _jsonPayload(task.toJson()),
      retryPolicy: retryPolicy ?? RetryPolicy.none(),
      deduplicationKey: deduplicationKey,
      idempotencyKey: idempotencyKey,
      priority: priority,
      dependsOn: dependencies,
      onDependencyFailure: onDependencyFailure,
      group: group,
    );
  }

  /// Stores one prepared task. Must run inside [_lock].
  Future<String> _store(
    _EnqueueRequest request,
    List<QueueEvent> events,
  ) async {
    final key = request.deduplicationKey;
    if (key != null) {
      final existing = await _storage.findActiveByDeduplicationKey(key);
      if (existing != null) return existing.id;
    }
    for (final dependency in request.dependsOn) {
      if (await _storage.get(dependency) == null) {
        throw TaskNotFoundException(dependency);
      }
    }

    final id = _idGenerator();
    final verdict = await _evaluate(
      id,
      request.dependsOn,
      request.onDependencyFailure,
      _direct,
    );
    final now = _clock.now();
    final stored = _applyVerdict(
      StoredTask(
        id: id,
        type: request.type,
        payload: request.payload,
        status: TaskStatus.waiting,
        attempts: 0,
        retryPolicy: request.retryPolicy,
        createdAt: now,
        updatedAt: now,
        sequence: await _allocateSequence(),
        deduplicationKey: request.deduplicationKey,
        idempotencyKey: request.idempotencyKey,
        priority: request.priority,
        dependsOn: request.dependsOn,
        onDependencyFailure: request.onDependencyFailure,
        group: request.group,
      ),
      verdict,
      now,
    );
    await _storage.save(stored.task);
    events.add(
      TaskEnqueued(
        taskId: id,
        taskType: request.type,
        occurredAt: now,
        deduplicationKey: request.deduplicationKey,
        idempotencyKey: request.idempotencyKey,
      ),
    );
    final event = stored.event;
    if (event != null) events.add(event);
    return id;
  }

  /// Recovers interrupted work and starts executing eligible tasks.
  ///
  /// A task still marked [TaskStatus.running] was claimed by a previous
  /// process that never recorded a result. That attempt counts. When the
  /// attempt budget is spent, the task fails with [TaskInterruptedException].
  /// Otherwise it is scheduled using its retry policy. A running task whose
  /// cancellation was already requested is marked cancelled and is not
  /// retried.
  ///
  /// A recovered task that fails or is cancelled also settles the tasks
  /// waiting on it.
  ///
  /// Recovery errors propagate and leave the queue idle so this call can be
  /// retried. Successfully recovered records stay updated.
  ///
  /// Throws [StateError] unless the queue is [QueueRunState.idle], or if the
  /// queue was closed.
  Future<void> start() async {
    _ensureOpen();
    await _lock.synchronized(() async {
      _ensureOpen();
      if (_runState != QueueRunState.idle) {
        throw StateError('Cannot start a queue from $_runState');
      }
      // Recovery is serialized while still idle. No timers or handlers start
      // until every batch succeeds; a failure leaves start safe to retry.
      while (true) {
        final running = await _storage.getByStatus(
          TaskStatus.running,
          limit: 100,
        );
        if (running.isEmpty) break;
        for (final task in running) {
          _emitAll(await _commit(_recoverInterrupted(task), _direct));
        }
      }
      _runState = QueueRunState.running;
    });
    await _pump();
  }

  /// Stops starting tasks. Handlers that are already running continue.
  ///
  /// Due retries stay stored until [resume]. Throws [StateError] unless the
  /// queue is [QueueRunState.running].
  Future<void> pause() {
    return _lock.synchronized(() async {
      if (_runState != QueueRunState.running) {
        throw StateError('Cannot pause a queue from $_runState');
      }
      _runState = QueueRunState.paused;
      _cancelWake();
    });
  }

  /// Continues a paused queue.
  ///
  /// Throws [StateError] unless the queue is [QueueRunState.paused].
  Future<void> resume() async {
    await _lock.synchronized(() async {
      if (_runState != QueueRunState.paused) {
        throw StateError('Cannot resume a queue from $_runState');
      }
      _runState = QueueRunState.running;
    });
    await _pump();
  }

  /// Stops the worker and waits for in-flight handlers to return.
  ///
  /// Pending and retry-scheduled tasks remain stored. Calling [stop] on an
  /// idle queue does nothing. [start] may be called again afterwards.
  /// Outstanding worker storage retries must also finish; an unavailable
  /// adapter can keep this future pending until storage recovers.
  Future<void> stop() async {
    final shouldWait = await _lock.synchronized(() async {
      switch (_runState) {
        case QueueRunState.idle:
          return false;
        case QueueRunState.stopping:
          return true;
        case QueueRunState.running:
        case QueueRunState.paused:
          _runState = QueueRunState.stopping;
          _cancelWake();
          return true;
      }
    });
    if (!shouldWait) return;
    while (_inFlight > 0) {
      await _waitForDrain();
    }
    await _lock.synchronized(() async {
      if (_runState == QueueRunState.stopping && _inFlight == 0) {
        _runState = QueueRunState.idle;
      }
    });
  }

  /// Cancels [id].
  ///
  /// * `pending`, `waiting`, and `retryScheduled` become
  ///   [TaskStatus.cancelled] and will not start. Tasks waiting on this one
  ///   follow their [DependencyFailurePolicy].
  /// * `running` is left running. [TaskContext.isCancellationRequested]
  ///   becomes true. If the handler later throws, including
  ///   [TaskCancelledException], the task is cancelled and not retried. If
  ///   the handler returns normally, the task is completed because the work
  ///   finished.
  /// * Terminal tasks are left unchanged.
  ///
  /// Throws [TaskNotFoundException] when [id] is not stored.
  Future<void> cancel(String id) async {
    _ensureOpen();
    final events = <QueueEvent>[];
    await _lock.synchronized(() async {
      final task = await _storage.get(id);
      if (task == null) throw TaskNotFoundException(id);
      await _cancelLocked(task, events);
    });
    _afterPublicChange(events);
  }

  /// Cancels every active task in [group] and returns how many were
  /// affected.
  ///
  /// Each task is handled as by [cancel]: queued and waiting tasks become
  /// [TaskStatus.cancelled] immediately, and running tasks get a
  /// cancellation request. Terminal tasks are not counted. Dependents of
  /// cancelled tasks follow their [DependencyFailurePolicy], even when they
  /// are outside [group].
  Future<int> cancelGroup(String group) async {
    _ensureOpen();
    _rejectBlank(group, 'group');
    final events = <QueueEvent>[];
    final count = await _lock.synchronized(() async {
      var affected = 0;
      for (final listed in await _storage.getByGroup(group)) {
        // An earlier cancellation in this loop may have cascaded here.
        final current = await _storage.get(listed.id);
        if (current == null) continue;
        if (await _cancelLocked(current, events)) affected++;
      }
      return affected;
    });
    _afterPublicChange(events);
    return count;
  }

  /// Cancels or requests cancellation of [task]. Must run inside [_lock].
  ///
  /// Returns whether the task was active.
  Future<bool> _cancelLocked(StoredTask task, List<QueueEvent> events) async {
    final now = _clock.now();
    switch (task.status) {
      case TaskStatus.pending:
      case TaskStatus.waiting:
      case TaskStatus.retryScheduled:
        final cancelled = _Resolved(
          task.copyWith(
            status: TaskStatus.cancelled,
            updatedAt: now,
            nextAttemptAt: null,
            cancelRequested: false,
          ),
          TaskCancelled(
            taskId: task.id,
            taskType: task.type,
            occurredAt: now,
            wasRunning: false,
          ),
        );
        events.addAll(await _commit(cancelled, _direct));
        return true;
      case TaskStatus.running:
        if (!task.cancelRequested) {
          await _storage.update(
            task.copyWith(cancelRequested: true, updatedAt: now),
          );
        }
        _cancelRequested.add(task.id);
        return true;
      case TaskStatus.completed:
      case TaskStatus.failed:
      case TaskStatus.cancelled:
        return false;
    }
  }

  /// Emits [events] and lets a running queue pick up released work.
  void _afterPublicChange(List<QueueEvent> events) {
    _emitAll(events);
    if (events.isNotEmpty && _runState == QueueRunState.running) {
      unawaited(_pump());
    }
  }

  /// Returns the stored record for [id], or null.
  Future<StoredTask?> getTask(String id) {
    return _lock.synchronized(() => _storage.get(id));
  }

  /// Returns stored tasks, oldest first.
  ///
  /// Pass [status] and/or [group] to restrict the result. Without either,
  /// every task is returned.
  Future<List<StoredTask>> getTasks({TaskStatus? status, String? group}) {
    return _lock.synchronized(() {
      if (group != null) return _storage.getByGroup(group, status: status);
      if (status == null) return _storage.getAll();
      return _storage.getByStatus(status);
    });
  }

  /// Puts a [TaskStatus.failed] or [TaskStatus.cancelled] task back in the
  /// queue and returns its id.
  ///
  /// The task keeps its id, payload, keys, priority, group, dependencies,
  /// `createdAt`, and `sequence`, so it runs in its original order relative
  /// to other stored tasks. It becomes [TaskStatus.pending], or
  /// [TaskStatus.waiting] while a dependency is still active, with a fresh
  /// attempt budget: `attempts` resets to zero. Tasks that were cancelled
  /// because this one failed are not revived; retry them as well. [StoredTask.lastFailure] is kept for reference until the next
  /// attempt replaces or clears it. Pass [retryPolicy] to replace the stored
  /// policy.
  ///
  /// A [TaskEnqueued] event is emitted, and a running queue starts the task
  /// when a slot is free.
  ///
  /// Throws [TaskNotFoundException] when [id] is not stored,
  /// [UnknownTaskTypeException] when its type is not registered, and
  /// [StateError] when the task is not failed or cancelled, when another
  /// active task already holds its deduplication key, or when one of its
  /// dependencies did not complete and its policy is not
  /// [DependencyFailurePolicy.run]. Retry that dependency first.
  Future<String> retry(String id, {RetryPolicy? retryPolicy}) async {
    _ensureOpen();
    final event = await _lock.synchronized(() async {
      final task = await _storage.get(id);
      if (task == null) throw TaskNotFoundException(id);
      if (task.status != TaskStatus.failed &&
          task.status != TaskStatus.cancelled) {
        throw StateError(
          'Only failed or cancelled tasks can be retried. '
          'Task "$id" is ${task.status.name}.',
        );
      }
      if (!_registry.contains(task.type)) {
        throw _registry.unknownType(task.type);
      }
      final key = task.deduplicationKey;
      if (key != null) {
        final active = await _storage.findActiveByDeduplicationKey(key);
        if (active != null) {
          throw StateError(
            'Task "${active.id}" is already active with deduplication key '
            '"$key".',
          );
        }
      }

      final verdict = await _evaluate(
        task.id,
        task.dependsOn,
        task.onDependencyFailure,
        _direct,
      );
      final blocker = verdict.blocker;
      if (blocker != null) {
        throw StateError(
          'Cannot retry task "$id": ${blocker.dependencyId} is '
          '${blocker.dependencyStatus?.name ?? 'missing'}. '
          'Retry that dependency first.',
        );
      }

      final now = _clock.now();
      await _storage.update(
        task.copyWith(
          status: verdict.kind == _VerdictKind.ready
              ? TaskStatus.pending
              : TaskStatus.waiting,
          attempts: 0,
          retryPolicy: retryPolicy,
          updatedAt: now,
          nextAttemptAt: null,
          cancelRequested: false,
        ),
      );
      _cancelRequested.remove(task.id);
      return TaskEnqueued(
        taskId: task.id,
        taskType: task.type,
        occurredAt: now,
        deduplicationKey: task.deduplicationKey,
        idempotencyKey: task.idempotencyKey,
      );
    });
    _emit(event);
    if (_runState == QueueRunState.running) unawaited(_pump());
    return id;
  }

  /// Removes a finished task from storage.
  ///
  /// Only [TaskStatus.completed], [TaskStatus.failed], and
  /// [TaskStatus.cancelled] tasks can be deleted. Cancel an active task
  /// first. No event is emitted.
  ///
  /// Throws [TaskNotFoundException] when [id] is not stored and [StateError]
  /// when the task is still active, or when a waiting task still depends on
  /// it.
  Future<void> delete(String id) async {
    _ensureOpen();
    await _lock.synchronized(() async {
      final task = await _storage.get(id);
      if (task == null) throw TaskNotFoundException(id);
      if (!task.status.isTerminal) {
        throw StateError(
          'Only finished tasks can be deleted. '
          'Task "$id" is ${task.status.name}.',
        );
      }
      final dependents = await _storage.getWaitingDependents(id);
      if (dependents.isNotEmpty) {
        throw StateError(
          'Task "$id" cannot be deleted while task '
          '"${dependents.first.id}" is waiting on it.',
        );
      }
      await _storage.delete(id);
    });
  }

  /// Deletes finished tasks and returns how many were removed.
  ///
  /// [statuses] defaults to [TaskStatus.completed] only. Every status must
  /// be terminal (completed, failed, or cancelled); active statuses throw
  /// [ArgumentError]. When [olderThan] is set, only tasks whose
  /// [StoredTask.updatedAt] is at least that long before the queue clock's
  /// current time are removed.
  ///
  /// Tasks that a waiting task still depends on are skipped, so a dependent
  /// never loses the record it is waiting for.
  ///
  /// Matching records are loaded with [QueueStorage.getByStatus] and
  /// deleted one at a time while the queue lock is held. No events are
  /// emitted.
  Future<int> purge({
    Set<TaskStatus> statuses = const {TaskStatus.completed},
    Duration? olderThan,
  }) async {
    _ensureOpen();
    for (final status in statuses) {
      if (!status.isTerminal) {
        throw ArgumentError.value(
          status,
          'statuses',
          'Only completed, failed, and cancelled tasks can be purged',
        );
      }
    }
    if (olderThan != null && olderThan.isNegative) {
      throw ArgumentError.value(olderThan, 'olderThan', 'Must not be negative');
    }
    return _lock.synchronized(() async {
      final cutoff = olderThan == null
          ? null
          : _clock.now().subtract(olderThan);
      var removed = 0;
      for (final status in statuses) {
        final tasks = await _storage.getByStatus(status);
        for (final task in tasks) {
          if (cutoff != null && task.updatedAt.isAfter(cutoff)) continue;
          if ((await _storage.getWaitingDependents(task.id)).isNotEmpty) {
            continue;
          }
          await _storage.delete(task.id);
          removed++;
        }
      }
      return removed;
    });
  }

  /// Stops the queue, then closes [events] and [storageErrors].
  ///
  /// Waits like [stop]. Afterwards [register], [enqueue], [enqueueChain],
  /// [start], [cancel], [cancelGroup], [retry], [delete], and [purge] throw
  /// [StateError]. [getTask] and
  /// [getTasks] keep working. The storage is not closed; it belongs to the
  /// caller. Calling [close] again returns the same future.
  Future<void> close() => _closing ??= _close();

  Future<void> _close() async {
    _closed = true;
    await stop();
    await _events.close();
    await _storageErrors.close();
  }

  void _ensureOpen() {
    if (_closed) throw StateError('DurableQueue is closed');
  }

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
        return _Resolved(
          task.copyWith(
            status: TaskStatus.pending,
            updatedAt: now,
            nextAttemptAt: null,
          ),
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
      await io(() => _storage.update(step.task), 'update');
    }
    await io(() => _storage.update(root.task), 'update');
    return [?root.event, for (final step in cascade) ?step.event];
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

  Future<void> _pump() async {
    if (_pumping) {
      _pumpAgain = true;
      return;
    }
    _pumping = true;
    try {
      do {
        _pumpAgain = false;
        while (_runState == QueueRunState.running &&
            _inFlight < maxConcurrentTasks) {
          final claimed = await _lock.synchronized(_claimNext);
          if (claimed == null) break;
          if (_runState != QueueRunState.running) {
            await _lock.synchronized(() => _revertClaim(claimed));
            break;
          }
          _enterFlight();
          unawaited(_execute(claimed));
        }
      } while (_pumpAgain && _runState == QueueRunState.running);
    } finally {
      _pumping = false;
    }
    if (_runState == QueueRunState.running) await _armNextWake();
  }

  Future<StoredTask?> _claimNext() async {
    if (_runState != QueueRunState.running) return null;
    final task = await _retryStorage(
      () => _storage.getNextReady(_clock.now()),
      'getNextReady',
    );
    if (task == null) return null;
    // Claim only one due retry rather than promoting the whole backlog.
    final claimed = task.copyWith(
      status: TaskStatus.running,
      attempts: task.attempts + 1,
      updatedAt: _clock.now(),
      nextAttemptAt: null,
    );
    await _retryStorage(() => _storage.update(claimed), 'update');
    return claimed;
  }

  Future<void> _revertClaim(StoredTask claimed) async {
    final current = await _retryStorage(() => _storage.get(claimed.id), 'get');
    if (current == null || current.status != TaskStatus.running) return;
    if (current.attempts != claimed.attempts) return;
    await _retryStorage(
      () => _storage.update(
        current.copyWith(
          status: TaskStatus.pending,
          attempts: current.attempts - 1,
          updatedAt: _clock.now(),
        ),
      ),
      'update',
    );
  }

  Future<void> _execute(StoredTask claimed) async {
    try {
      _emit(
        TaskStarted(
          taskId: claimed.id,
          taskType: claimed.type,
          occurredAt: _clock.now(),
          attempt: claimed.attempts,
        ),
      );
      await _runClaimed(claimed);
    } on Object catch (error, stackTrace) {
      Zone.current.handleUncaughtError(error, stackTrace);
    } finally {
      _cancelRequested.remove(claimed.id);
      _exitFlight();
      if (_runState == QueueRunState.running) unawaited(_pump());
    }
  }

  Future<void> _runClaimed(StoredTask claimed) async {
    final registration = _registry.find(claimed.type);
    if (registration == null) {
      await _failPermanent(
        claimed,
        _registry.unknownType(claimed.type),
        StackTrace.current,
      );
      return;
    }

    final DurableTask decoded;
    try {
      decoded = registration.decode(_jsonPayload(claimed.payload));
      if (decoded.type != claimed.type) {
        throw TaskDecodeException(
          'Decoded task type "${decoded.type}" does not match stored type '
          '"${claimed.type}"',
        );
      }
    } on Object catch (error, stackTrace) {
      await _failPermanent(claimed, error, stackTrace);
      return;
    }

    final context = TaskContext(
      taskId: claimed.id,
      attempt: claimed.attempts,
      maxAttempts: claimed.retryPolicy.maxAttempts,
      idempotencyKey: claimed.idempotencyKey,
      deduplicationKey: claimed.deduplicationKey,
      isCancellationRequested: () => _cancelRequested.contains(claimed.id),
    );

    try {
      await registration.handle(decoded, context);
    } on TaskCancelledException catch (error, stackTrace) {
      await _markCancelled(claimed, error, stackTrace);
      return;
    } on Object catch (error, stackTrace) {
      if (_cancelRequested.contains(claimed.id)) {
        await _markCancelled(claimed, error, stackTrace);
        return;
      }
      await _handleFailure(claimed, registration, error, stackTrace);
      return;
    }
    await _markCompleted(claimed);
  }

  Future<void> _markCompleted(StoredTask claimed) async {
    final events = await _lock.synchronized(
      () => _transition(claimed, (current, now, _) {
        return _Transition(
          current.copyWith(
            status: TaskStatus.completed,
            updatedAt: now,
            nextAttemptAt: null,
            lastFailure: null,
            cancelRequested: false,
          ),
          TaskCompleted(
            taskId: current.id,
            taskType: current.type,
            occurredAt: now,
            attempt: current.attempts,
          ),
        );
      }),
    );
    _emitAll(events);
  }

  Future<void> _markCancelled(
    StoredTask claimed,
    Object error,
    StackTrace stackTrace,
  ) async {
    final events = await _lock.synchronized(
      () => _transition(claimed, (current, now, failureOf) {
        return _Transition(
          current.copyWith(
            status: TaskStatus.cancelled,
            updatedAt: now,
            nextAttemptAt: null,
            lastFailure: failureOf(error, stackTrace),
            cancelRequested: false,
          ),
          TaskCancelled(
            taskId: current.id,
            taskType: current.type,
            occurredAt: now,
            wasRunning: true,
          ),
        );
      }),
    );
    _emitAll(events);
  }

  Future<void> _failPermanent(
    StoredTask claimed,
    Object error,
    StackTrace stackTrace,
  ) async {
    final events = await _lock.synchronized(
      () => _transition(claimed, (current, now, failureOf) {
        if (current.cancelRequested || _cancelRequested.contains(current.id)) {
          return _cancelledTransition(current, now, error, stackTrace);
        }
        return _Transition(
          current.copyWith(
            status: TaskStatus.failed,
            updatedAt: now,
            nextAttemptAt: null,
            lastFailure: failureOf(error, stackTrace),
            cancelRequested: false,
          ),
          TaskFailed(
            taskId: current.id,
            taskType: current.type,
            occurredAt: now,
            failure: failureOf(error, stackTrace),
          ),
        );
      }),
    );
    _emitAll(events);
  }

  Future<void> _handleFailure(
    StoredTask claimed,
    RegisteredTask registration,
    Object error,
    StackTrace stackTrace,
  ) async {
    final events = await _lock.synchronized(
      () => _transition(claimed, (current, now, failureOf) {
        if (current.cancelRequested || _cancelRequested.contains(current.id)) {
          return _cancelledTransition(current, now, error, stackTrace);
        }
        final failure = failureOf(error, stackTrace);
        final retry =
            current.attempts < current.retryPolicy.maxAttempts &&
            _allowsRetry(registration, error, stackTrace);
        if (!retry) {
          return _Transition(
            current.copyWith(
              status: TaskStatus.failed,
              updatedAt: now,
              nextAttemptAt: null,
              lastFailure: failure,
              cancelRequested: false,
            ),
            TaskFailed(
              taskId: current.id,
              taskType: current.type,
              occurredAt: now,
              failure: failure,
            ),
          );
        }

        final delay = current.retryPolicy.delayAfter(
          current.attempts,
          _nextFraction(),
        );
        final nextAttemptAt = now.add(delay);
        _considerWake(nextAttemptAt);
        return _Transition(
          current.copyWith(
            status: TaskStatus.retryScheduled,
            updatedAt: now,
            nextAttemptAt: nextAttemptAt,
            lastFailure: failure,
          ),
          TaskRetryScheduled(
            taskId: current.id,
            taskType: current.type,
            occurredAt: now,
            attempt: current.attempts,
            maxAttempts: current.retryPolicy.maxAttempts,
            delay: delay,
            nextAttemptAt: nextAttemptAt,
            error: failure.error,
          ),
        );
      }),
    );
    _emitAll(events);
  }

  _Transition _cancelledTransition(
    StoredTask current,
    DateTime now,
    Object error,
    StackTrace stackTrace,
  ) {
    return _Transition(
      current.copyWith(
        status: TaskStatus.cancelled,
        updatedAt: now,
        nextAttemptAt: null,
        lastFailure: _failure(current, error, stackTrace, now),
        cancelRequested: false,
      ),
      TaskCancelled(
        taskId: current.id,
        taskType: current.type,
        occurredAt: now,
        wasRunning: true,
      ),
    );
  }

  Future<List<QueueEvent>> _transition(
    StoredTask claimed,
    _Transition Function(
      StoredTask current,
      DateTime now,
      TaskFailure Function(Object error, StackTrace stackTrace) failureOf,
    )
    change,
  ) async {
    final current = await _retryStorage(() => _storage.get(claimed.id), 'get');
    if (current == null || current.status != TaskStatus.running) {
      return const [];
    }
    final now = _clock.now();
    TaskFailure failureOf(Object error, StackTrace stackTrace) {
      return _failure(current, error, stackTrace, now);
    }

    final transition = change(current, now, failureOf);
    final events = await _commit(
      _Resolved(transition.task, transition.event),
      _retryStorage,
    );
    _cancelRequested.remove(current.id);
    return events;
  }

  bool _allowsRetry(
    RegisteredTask registration,
    Object error,
    StackTrace stackTrace,
  ) {
    final predicate = registration.retryIf;
    if (predicate == null) return true;
    try {
      return predicate(error, stackTrace);
    } on Object {
      return false;
    }
  }

  TaskFailure _failure(
    StoredTask current,
    Object error,
    StackTrace stackTrace,
    DateTime now,
  ) {
    return TaskFailure(
      error: error.toString(),
      stackTrace: stackTrace.toString(),
      failedAt: now,
      attempt: current.attempts,
    );
  }

  Future<void> _armNextWake() async {
    if (_runState != QueueRunState.running) return;
    if (_inFlight >= maxConcurrentTasks) return;
    final earliest = await _lock.synchronized(() async {
      if (_runState != QueueRunState.running) return null;
      return await _retryStorage(
        () => _storage.getNextWakeAt(),
        'getNextWakeAt',
      );
    });
    if (earliest == null || _runState != QueueRunState.running) return;
    if (_inFlight >= maxConcurrentTasks) return;
    _considerWake(earliest);
  }

  void _considerWake(DateTime when) {
    if (_runState != QueueRunState.running) return;
    if (_wakeAt != null && !when.isBefore(_wakeAt!)) return;
    _sleepGeneration++;
    final generation = _sleepGeneration;
    _activeDelay?.cancel();
    _wakeAt = when;
    final delay = _clock.delayUntil(when);
    _activeDelay = delay;
    unawaited(
      delay.future.then((_) {
        if (generation != _sleepGeneration) return;
        if (!identical(_activeDelay, delay)) return;
        _wakeAt = null;
        _activeDelay = null;
        if (_runState == QueueRunState.running) unawaited(_pump());
      }),
    );
  }

  void _cancelWake() {
    _sleepGeneration++;
    _wakeAt = null;
    _activeDelay?.cancel();
    _activeDelay = null;
  }

  void _enterFlight() => _inFlight++;

  void _exitFlight() {
    _inFlight--;
    if (_inFlight < 0) _inFlight = 0;
    if (_inFlight == 0) {
      final drain = _drain;
      _drain = null;
      if (drain != null && !drain.isCompleted) drain.complete();
    }
  }

  Future<void> _waitForDrain() {
    if (_inFlight == 0) return Future<void>.value();
    return (_drain ??= Completer<void>()).future;
  }

  Future<T> _retryStorage<T>(
    Future<T> Function() operation,
    String name,
  ) async {
    var attempt = 0;
    while (true) {
      try {
        return await operation();
      } on Object catch (error, stackTrace) {
        attempt++;
        if (!_storageErrors.isClosed) {
          _storageErrors.add(
            QueueStorageFailure(
              operation: name,
              error: error,
              stackTrace: stackTrace,
              occurredAt: _clock.now(),
              attempt: attempt,
            ),
          );
        }
        // This is storage recovery, not a handler retry. It neither consumes
        // task attempts nor re-invokes business logic.
        await _clock.delayUntil(_clock.now().add(storageRetryDelay)).future;
      }
    }
  }

  double _nextFraction() {
    final value = _randomFraction();
    if (value.isNaN || value < 0 || value >= 1) {
      throw StateError(
        'randomFraction must return a value in [0, 1). Got $value',
      );
    }
    return value;
  }

  void _emit(QueueEvent event) {
    if (!_events.isClosed) _events.add(event);
  }

  Future<int> _allocateSequence() async {
    _sequence ??= await _storage.getMaxSequence();
    final next = _sequence! + 1;
    _sequence = next;
    return next;
  }
}

Map<String, dynamic> _jsonPayload(Map<String, dynamic> payload) {
  try {
    final decoded = jsonDecode(jsonEncode(payload));
    if (decoded is! Map) {
      throw ArgumentError('Task payload must be a JSON object');
    }
    return Map<String, dynamic>.from(decoded);
  } on ArgumentError {
    rethrow;
  } on Object {
    throw ArgumentError(
      'Task payload must be JSON-encodable '
      '(null, bool, num, String, List, or Map).',
    );
  }
}

void _rejectBlank(String? value, String name) {
  if (value != null && value.isEmpty) {
    throw ArgumentError.value(value, name, 'Must not be empty');
  }
}

final class _Transition {
  _Transition(this.task, this.event);

  final StoredTask task;
  final QueueEvent event;
}

final class _EnqueueRequest {
  _EnqueueRequest({
    required this.type,
    required this.payload,
    required this.retryPolicy,
    required this.deduplicationKey,
    required this.idempotencyKey,
    required this.priority,
    required this.dependsOn,
    required this.onDependencyFailure,
    required this.group,
  });

  final String type;
  final Map<String, dynamic> payload;
  final RetryPolicy retryPolicy;
  final String? deduplicationKey;
  final String? idempotencyKey;
  final int priority;
  final List<String> dependsOn;
  final DependencyFailurePolicy onDependencyFailure;
  final String? group;

  _EnqueueRequest withDependencies(List<String> dependencies) {
    return _EnqueueRequest(
      type: type,
      payload: payload,
      retryPolicy: retryPolicy,
      deduplicationKey: deduplicationKey,
      idempotencyKey: idempotencyKey,
      priority: priority,
      dependsOn: List<String>.unmodifiable(dependencies),
      onDependencyFailure: onDependencyFailure,
      group: group,
    );
  }
}

enum _VerdictKind { ready, wait, cancel, fail }

/// Outcome of checking a task's dependencies.
final class _Verdict {
  const _Verdict(this.kind, [this.blocker]);

  static const ready = _Verdict(_VerdictKind.ready);
  static const wait = _Verdict(_VerdictKind.wait);

  final _VerdictKind kind;

  /// The dependency that did not complete, for [_VerdictKind.cancel] and
  /// [_VerdictKind.fail].
  final DependencyFailedException? blocker;
}

/// A record to write and the event to emit after the write, if any.
final class _Resolved {
  const _Resolved(this.task, this.event);

  final StoredTask task;
  final QueueEvent? event;
}

/// Runs one storage operation. Worker paths retry; public calls do not.
typedef _StorageIo = Future<T> Function<T>(
  Future<T> Function() operation,
  String name,
);

Future<T> _direct<T>(Future<T> Function() operation, String name) =>
    operation();
