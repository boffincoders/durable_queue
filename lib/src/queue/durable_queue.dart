import 'dart:async';
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
  /// `pending`, `running`, or `retryScheduled`, this returns the existing id
  /// and leaves that task unchanged. Completed, failed, and cancelled tasks
  /// do not count, so the same key can be enqueued again after a terminal
  /// state. The new payload is not merged into the existing task.
  ///
  /// [idempotencyKey] is stored and passed to the handler on [TaskContext].
  ///
  /// [retryPolicy] defaults to [RetryPolicy.none].
  ///
  /// Throws [UnknownTaskTypeException] if [task]'s type is not registered,
  /// and [ArgumentError] if the payload is not JSON-encodable or a key is
  /// empty.
  Future<String> enqueue(
    DurableTask task, {
    RetryPolicy? retryPolicy,
    String? deduplicationKey,
    String? idempotencyKey,
  }) async {
    _ensureOpen();
    if (task.type.isEmpty) {
      throw ArgumentError.value(task.type, 'type', 'Must not be empty');
    }
    _rejectBlank(deduplicationKey, 'deduplicationKey');
    _rejectBlank(idempotencyKey, 'idempotencyKey');
    if (!_registry.contains(task.type)) {
      throw _registry.unknownType(task.type);
    }

    final payload = _jsonPayload(task.toJson());
    final policy = retryPolicy ?? RetryPolicy.none();
    final outcome = await _lock.synchronized(() async {
      if (deduplicationKey != null) {
        final existing = await _storage.findActiveByDeduplicationKey(
          deduplicationKey,
        );
        if (existing != null) return _EnqueueOutcome(existing.id, null);
      }

      final now = _clock.now();
      final stored = StoredTask(
        id: _idGenerator(),
        type: task.type,
        payload: payload,
        status: TaskStatus.pending,
        attempts: 0,
        retryPolicy: policy,
        createdAt: now,
        updatedAt: now,
        sequence: await _allocateSequence(),
        deduplicationKey: deduplicationKey,
        idempotencyKey: idempotencyKey,
      );
      await _storage.save(stored);
      return _EnqueueOutcome(
        stored.id,
        TaskEnqueued(
          taskId: stored.id,
          taskType: stored.type,
          occurredAt: now,
          deduplicationKey: deduplicationKey,
          idempotencyKey: idempotencyKey,
        ),
      );
    });

    final event = outcome.event;
    if (event != null) {
      _emit(event);
      if (_runState == QueueRunState.running) unawaited(_pump());
    }
    return outcome.id;
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
          final event = await _recoverInterrupted(task);
          if (event != null) _emit(event);
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
  /// * `pending` and `retryScheduled` become [TaskStatus.cancelled] and will
  ///   not start.
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
    final event = await _lock.synchronized(() async {
      final task = await _storage.get(id);
      if (task == null) throw TaskNotFoundException(id);
      final now = _clock.now();
      switch (task.status) {
        case TaskStatus.pending:
        case TaskStatus.retryScheduled:
          await _storage.update(
            task.copyWith(
              status: TaskStatus.cancelled,
              updatedAt: now,
              nextAttemptAt: null,
              cancelRequested: false,
            ),
          );
          return TaskCancelled(
            taskId: task.id,
            taskType: task.type,
            occurredAt: now,
            wasRunning: false,
          );
        case TaskStatus.running:
          if (!task.cancelRequested) {
            await _storage.update(
              task.copyWith(cancelRequested: true, updatedAt: now),
            );
          }
          _cancelRequested.add(task.id);
          return null;
        case TaskStatus.completed:
        case TaskStatus.failed:
        case TaskStatus.cancelled:
          return null;
      }
    });
    if (event != null) _emit(event);
  }

  /// Returns the stored record for [id], or null.
  Future<StoredTask?> getTask(String id) {
    return _lock.synchronized(() => _storage.get(id));
  }

  /// Returns stored tasks, oldest first.
  ///
  /// Pass [status] to restrict the result. Without it, every task is
  /// returned.
  Future<List<StoredTask>> getTasks({TaskStatus? status}) {
    return _lock.synchronized(() {
      if (status == null) return _storage.getAll();
      return _storage.getByStatus(status);
    });
  }

  /// Puts a [TaskStatus.failed] or [TaskStatus.cancelled] task back in the
  /// queue and returns its id.
  ///
  /// The task keeps its id, payload, keys, `createdAt`, and `sequence`, so it
  /// runs in its original order relative to other stored tasks. It becomes
  /// [TaskStatus.pending] with a fresh attempt budget: `attempts` resets to
  /// zero. [StoredTask.lastFailure] is kept for reference until the next
  /// attempt replaces or clears it. Pass [retryPolicy] to replace the stored
  /// policy.
  ///
  /// A [TaskEnqueued] event is emitted, and a running queue starts the task
  /// when a slot is free.
  ///
  /// Throws [TaskNotFoundException] when [id] is not stored,
  /// [UnknownTaskTypeException] when its type is not registered, and
  /// [StateError] when the task is not failed or cancelled, or when another
  /// active task already holds its deduplication key.
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

      final now = _clock.now();
      await _storage.update(
        task.copyWith(
          status: TaskStatus.pending,
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
  /// when the task is still pending, running, or retry-scheduled.
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
          await _storage.delete(task.id);
          removed++;
        }
      }
      return removed;
    });
  }

  /// Stops the queue, then closes [events] and [storageErrors].
  ///
  /// Waits like [stop]. Afterwards [register], [enqueue], [start], [cancel],
  /// [retry], [delete], and [purge] throw [StateError]. [getTask] and
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

  Future<QueueEvent?> _recoverInterrupted(StoredTask task) async {
    final now = _clock.now();
    if (task.cancelRequested) {
      await _storage.update(
        task.copyWith(
          status: TaskStatus.cancelled,
          updatedAt: now,
          nextAttemptAt: null,
        ),
      );
      _cancelRequested.remove(task.id);
      return TaskCancelled(
        taskId: task.id,
        taskType: task.type,
        occurredAt: now,
        wasRunning: true,
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
      await _storage.update(
        normalized.copyWith(
          status: TaskStatus.failed,
          updatedAt: now,
          nextAttemptAt: null,
          lastFailure: failure,
          cancelRequested: false,
        ),
      );
      return TaskFailed(
        taskId: task.id,
        taskType: task.type,
        occurredAt: now,
        failure: failure,
      );
    }

    final delay = task.retryPolicy.delayAfter(attempts, _nextFraction());
    final nextAttemptAt = now.add(delay);
    await _storage.update(
      normalized.copyWith(
        status: TaskStatus.retryScheduled,
        updatedAt: now,
        nextAttemptAt: nextAttemptAt,
        lastFailure: failure,
      ),
    );
    _considerWake(nextAttemptAt);
    return TaskRetryScheduled(
      taskId: task.id,
      taskType: task.type,
      occurredAt: now,
      attempt: attempts,
      maxAttempts: task.retryPolicy.maxAttempts,
      delay: delay,
      nextAttemptAt: nextAttemptAt,
      error: failure.error,
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
    final event = await _lock.synchronized(
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
    if (event != null) _emit(event);
  }

  Future<void> _markCancelled(
    StoredTask claimed,
    Object error,
    StackTrace stackTrace,
  ) async {
    final event = await _lock.synchronized(
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
    if (event != null) _emit(event);
  }

  Future<void> _failPermanent(
    StoredTask claimed,
    Object error,
    StackTrace stackTrace,
  ) async {
    final event = await _lock.synchronized(
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
    if (event != null) _emit(event);
  }

  Future<void> _handleFailure(
    StoredTask claimed,
    RegisteredTask registration,
    Object error,
    StackTrace stackTrace,
  ) async {
    final event = await _lock.synchronized(
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
    if (event != null) _emit(event);
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

  Future<QueueEvent?> _transition(
    StoredTask claimed,
    _Transition Function(
      StoredTask current,
      DateTime now,
      TaskFailure Function(Object error, StackTrace stackTrace) failureOf,
    )
    change,
  ) async {
    final current = await _retryStorage(() => _storage.get(claimed.id), 'get');
    if (current == null || current.status != TaskStatus.running) return null;
    final now = _clock.now();
    TaskFailure failureOf(Object error, StackTrace stackTrace) {
      return _failure(current, error, stackTrace, now);
    }

    final transition = change(current, now, failureOf);
    await _retryStorage(() => _storage.update(transition.task), 'update');
    _cancelRequested.remove(current.id);
    return transition.event;
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

final class _EnqueueOutcome {
  _EnqueueOutcome(this.id, this.event);

  final String id;
  final QueueEvent? event;
}

final class _Transition {
  _Transition(this.task, this.event);

  final StoredTask task;
  final QueueEvent event;
}
