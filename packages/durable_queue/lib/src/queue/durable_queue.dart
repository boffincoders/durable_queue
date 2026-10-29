import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:math';

import '../clock/queue_clock.dart';
import '../errors.dart';
import '../events/queue_event.dart';
import '../gates/queue_gate.dart';
import '../internal/async_lock.dart';
import '../internal/rate_limiter.dart';
import '../limits/rate_limit.dart';
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
import 'drain_result.dart';
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
/// ## Work that continues outside the queue
///
/// A handler can start work that the operating system finishes, such as a
/// background upload, and call [TaskContext.handOff]. The task then stays
/// running, without a concurrency slot and across restarts, until
/// [completeHandOff] or [failHandOff] reports the outcome.
///
/// ## Rate limits and gates
///
/// [register] takes a `rateLimit` for one task type, and [addRateLimit] adds
/// a limit shared by several types or split by a key such as an API host.
/// [addGate] adds a condition, such as network reachability, that task types
/// list in `register(requires:)`. A task that may not start yet is deferred:
/// it keeps its status, attempt count, and place in line, and a
/// [TaskDeferred] event says why and until when.
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
  /// [gateRecheckInterval] and [gateErrorDelay] must be positive; see their
  /// documentation.
  DurableQueue({
    required QueueStorage storage,
    QueueClock? clock,
    this.maxConcurrentTasks = 1,
    this.storageRetryDelay = const Duration(seconds: 1),
    this.gateRecheckInterval = const Duration(minutes: 5),
    this.gateErrorDelay = const Duration(seconds: 30),
    RandomFraction? randomFraction,
    TaskIdGenerator? idGenerator,
  }) : _storage = storage,
       _clock = clock ?? const SystemQueueClock(),
       _randomFraction = randomFraction ?? Random().nextDouble,
       _idGenerator = idGenerator ?? _defaultTaskId {
    if (gateRecheckInterval <= Duration.zero) {
      throw ArgumentError.value(
        gateRecheckInterval,
        'gateRecheckInterval',
        'Must be positive',
      );
    }
    if (gateErrorDelay <= Duration.zero) {
      throw ArgumentError.value(
        gateErrorDelay,
        'gateErrorDelay',
        'Must be positive',
      );
    }
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

  /// How long a task deferred by a closed gate waits before it is checked
  /// again, in case the gate never reports opening. A gate that reports
  /// opening releases its deferred tasks at once. Defaults to five minutes.
  final Duration gateRecheckInterval;

  /// How long a task waits after `register(deferOn:)` attributed its error
  /// to a gate that is still open. Closing and reopening the gate releases it
  /// sooner. Defaults to 30 seconds.
  final Duration gateErrorDelay;

  final TaskRegistry _registry = TaskRegistry();
  final Map<String, _GateEntry> _gates = {};
  final Map<String, _SharedLimit> _sharedLimits = {};
  final Map<String, RateLimiter> _typeLimiters = {};

  Completer<void>? _activity;
  _DrainStats? _drainStats;

  /// Rate-limit slots already taken for deferred tasks, by task id.
  final Map<String, ({DateTime slot, Set<String> buckets})> _reservations = {};
  final AsyncLock _lock = AsyncLock();
  final StreamController<QueueEvent> _events =
      StreamController<QueueEvent>.broadcast();
  final StreamController<QueueStorageFailure> _storageErrors =
      StreamController<QueueStorageFailure>.broadcast();
  final Set<String> _cancelRequested = {};
  final Map<String, Completer<void>> _cancelSignals = {};

  /// Hand-offs made by handlers that have not returned yet, by task id.
  final Map<String, _PendingHandOff> _pendingHandOffs = {};

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
  ///
  /// [retryDelay] lets an error choose the wait before the next attempt, for
  /// example from an HTTP `Retry-After` header. When it returns null, or is
  /// omitted, the task's [RetryPolicy] decides. See [RetryDelayResolver].
  ///
  /// [requires] names gates added with [addGate] that must be open before a
  /// task of this type starts. [rateLimit] limits how often tasks of this
  /// type start; every attempt counts. [deferOn] maps a handler error to one
  /// of [requires], for example a connection error to `network`: the task is
  /// then deferred instead of failing, the attempt does not count, and the
  /// gate is asked to [QueueGate.recheck]. [deferOn] is consulted before
  /// [retryIf].
  ///
  /// [timeout] limits how long one attempt's handler may run. When it runs
  /// longer, the attempt fails with [TaskTimeoutException] and [retryIf] and
  /// the retry policy decide what happens next. Dart cannot stop a running
  /// function, so the handler keeps running and its result is ignored;
  /// [TaskContext.whenCancellationRequested] completes so a handler that
  /// listens for cancellation can stop. Time a transfer spends handed off to the operating system does
  /// not count.
  ///
  /// Throws [ArgumentError] if a gate in [requires] has not been added, if
  /// [deferOn] is given without [requires], or if [timeout] is not
  /// positive.
  void register<T extends DurableTask>({
    required String type,
    required TaskDecoder<T> decoder,
    required TaskHandler<T> handler,
    RetryPredicate? retryIf,
    RetryDelayResolver? retryDelay,
    Set<String> requires = const {},
    RateLimit? rateLimit,
    DeferOnError? deferOn,
    Duration? timeout,
  }) {
    _ensureOpen();
    for (final gate in requires) {
      if (!_gates.containsKey(gate)) {
        throw ArgumentError.value(
          requires,
          'requires',
          'Gate "$gate" has not been added. Call addGate first',
        );
      }
    }
    if (deferOn != null && requires.isEmpty) {
      throw ArgumentError.value(
        deferOn,
        'deferOn',
        'Needs at least one gate in requires',
      );
    }
    _registry.register<T>(
      type: type,
      decoder: decoder,
      handler: handler,
      retryIf: retryIf,
      retryDelay: retryDelay,
      requires: requires,
      rateLimit: rateLimit,
      deferOn: deferOn,
      timeout: timeout,
    );
    if (rateLimit != null) _typeLimiters[type] = RateLimiter(rateLimit);
  }

  /// Adds a gate named [name].
  ///
  /// Task types list gates in `register(requires:)`, so add gates before
  /// registering those types. When the gate reports opening, tasks it
  /// deferred become eligible at once. Closing a gate does not affect tasks
  /// that are already running.
  ///
  /// Throws [ArgumentError] if [name] is empty and [StateError] if a gate
  /// with that name exists or the queue is closed.
  void addGate(String name, QueueGate gate) {
    _ensureOpen();
    _rejectBlank(name, 'name');
    if (_gates.containsKey(name)) {
      throw StateError('Gate "$name" has already been added');
    }
    final entry = _GateEntry(gate);
    _gates[name] = entry;
    entry.subscription = gate.changes.listen((open) {
      if (open) unawaited(_releaseGate(name));
    });
  }

  /// Adds a rate limit shared by several task types.
  ///
  /// The limit applies to tasks whose type is in [types], or to every task
  /// when [types] is null. [keyOf] splits the limit into independent buckets,
  /// for example one per API host; a task for which it returns null is not
  /// limited by this limit. Without [keyOf], all matching tasks share one
  /// bucket. A task must satisfy every limit that applies to it, including
  /// its type's `register(rateLimit:)`. Every attempt counts.
  ///
  /// [name] identifies the limit in [TaskDeferred] events. Throws
  /// [ArgumentError] if [name] is empty or [types] is empty, and
  /// [StateError] if a limit with that name exists or the queue is closed.
  void addRateLimit(
    String name,
    RateLimit limit, {
    Set<String>? types,
    RateLimitKey? keyOf,
  }) {
    _ensureOpen();
    _rejectBlank(name, 'name');
    if (types != null && types.isEmpty) {
      throw ArgumentError.value(types, 'types', 'Must not be empty');
    }
    if (_sharedLimits.containsKey(name)) {
      throw StateError('Rate limit "$name" has already been added');
    }
    _sharedLimits[name] = _SharedLimit(
      RateLimiter(limit),
      types == null ? null : Set<String>.unmodifiable(types),
      keyOf,
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
  /// [runAt] or [delay] (not both) sets the earliest time the task may
  /// start, for example "send this reminder at 9:00" or "try again in an
  /// hour". The task is stored at once and counts as active (it holds its
  /// deduplication key) while it waits. A time in the past means "now". With
  /// [dependsOn], the task starts when both its dependencies have completed
  /// and its time has come.
  ///
  /// [expiresAt] or [expiresAfter] (not both) sets when the task stops being
  /// worth running, for example a "typing" notice or a one-time code. A task
  /// that has not started by then is cancelled instead of started: it emits
  /// [TaskCancelled], its [StoredTask.lastFailure] records a
  /// [TaskExpiredException], and tasks that depend on it settle as for any
  /// cancellation. A running attempt is not interrupted, but a retry is not
  /// started after the expiry. [retry] clears an expiry that has passed.
  ///
  /// Throws [UnknownTaskTypeException] if [task]'s type is not registered,
  /// [TaskNotFoundException] if a [dependsOn] id is not stored, and
  /// [ArgumentError] if the payload is not JSON-encodable, a key, group,
  /// or dependency id is empty, both [runAt] and [delay] are given, or
  /// [delay] is negative, both [expiresAt] and [expiresAfter] are given, or
  /// the expiry is not after both now and the start time.
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
    DateTime? runAt,
    Duration? delay,
    DateTime? expiresAt,
    Duration? expiresAfter,
  }) async {
    _ensureOpen();
    final start = _runAt(runAt, delay);
    final request = _prepare(
      task,
      retryPolicy: retryPolicy,
      deduplicationKey: deduplicationKey,
      idempotencyKey: idempotencyKey,
      priority: priority,
      dependsOn: dependsOn,
      onDependencyFailure: onDependencyFailure,
      group: group,
      runAt: start,
      expiresAt: _expiresAt(expiresAt, expiresAfter, start),
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
  /// [runAt] or [delay] sets the earliest start of the first task; the
  /// others follow it as usual. [expiresAt] or [expiresAfter] applies to
  /// every task: steps that have not started by then are cancelled.
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
    DateTime? runAt,
    Duration? delay,
    DateTime? expiresAt,
    Duration? expiresAfter,
  }) async {
    _ensureOpen();
    if (tasks.isEmpty) {
      throw ArgumentError.value(tasks, 'tasks', 'Must not be empty');
    }
    final firstRunAt = _runAt(runAt, delay);
    final expiry = _expiresAt(expiresAt, expiresAfter, firstRunAt);
    final requests = [
      for (var i = 0; i < tasks.length; i++)
        _prepare(
          tasks[i],
          retryPolicy: retryPolicy,
          priority: priority,
          onDependencyFailure: onDependencyFailure,
          group: group,
          dependsOn: i == 0 ? dependsOn : const [],
          runAt: i == 0 ? firstRunAt : null,
          expiresAt: expiry,
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
    DateTime? runAt,
    DateTime? expiresAt,
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
      runAt: runAt,
      expiresAt: expiresAt,
    );
  }

  /// Turns `enqueue(expiresAt:, expiresAfter:)` into an absolute time, or
  /// null. [start] is the task's earliest start, if any.
  DateTime? _expiresAt(
    DateTime? expiresAt,
    Duration? expiresAfter,
    DateTime? start,
  ) {
    if (expiresAt != null && expiresAfter != null) {
      throw ArgumentError('Pass expiresAt or expiresAfter, not both');
    }
    final now = _clock.now();
    final expiry = expiresAfter == null ? expiresAt : now.add(expiresAfter);
    if (expiry == null) return null;
    final name = expiresAfter == null ? 'expiresAt' : 'expiresAfter';
    final value = expiresAfter ?? expiresAt;
    if (!expiry.isAfter(now)) {
      throw ArgumentError.value(value, name, 'Must be in the future');
    }
    if (start != null && !expiry.isAfter(start)) {
      throw ArgumentError.value(value, name, 'Must be after the start time');
    }
    return expiry;
  }

  /// Turns `enqueue(runAt:, delay:)` into an absolute time, or null.
  DateTime? _runAt(DateTime? runAt, Duration? delay) {
    if (runAt != null && delay != null) {
      throw ArgumentError('Pass runAt or delay, not both');
    }
    if (delay != null) {
      if (delay.isNegative) {
        throw ArgumentError.value(delay, 'delay', 'Must not be negative');
      }
      return _clock.now().add(delay);
    }
    return runAt;
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
        nextAttemptAt: request.runAt,
        expiresAt: request.expiresAt,
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
        runAt: request.runAt,
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
  /// retried. Tasks handed off with [TaskContext.handOff] are not
  /// interrupted: they keep running outside the queue and are left alone.
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
      // Batches of 100. Handed-off tasks stay running, so each batch asks
      // for the ones already seen plus 100 more.
      final handedOff = <String>{};
      while (true) {
        final limit = handedOff.length + 100;
        final running = await _storage.getByStatus(
          TaskStatus.running,
          limit: limit,
        );
        var recovered = 0;
        for (final task in running) {
          if (task.handOffReference != null) {
            handedOff.add(task.id);
            continue;
          }
          _emitAll(await _commit(_recoverInterrupted(task), _direct));
          recovered++;
        }
        if (recovered == 0 && running.length < limit) break;
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
        if (task.isHandedOff && !_cancelSignals.containsKey(task.id)) {
          // Nothing in this process runs it; the outside work is told by the
          // event.
          events.addAll(await _commit(_handedOffCancelled(task, now), _direct));
          return true;
        }
        if (!task.cancelRequested) {
          await _storage.update(
            task.copyWith(cancelRequested: true, updatedAt: now),
          );
        }
        _cancelRequested.add(task.id);
        _signalCancel(task.id);
        return true;
      case TaskStatus.completed:
      case TaskStatus.failed:
      case TaskStatus.cancelled:
        return false;
    }
  }

  /// Completes a task that was handed off with [TaskContext.handOff], when
  /// the outside work succeeded. Tasks waiting on it are released.
  ///
  /// Pass the [reference] the handler gave, so an outcome meant for an
  /// earlier attempt is not applied to a later one. Returns false, and
  /// changes nothing, when the task is not handed off (any more) under that
  /// reference: unknown, already finished or cancelled, or running a new
  /// attempt. That makes repeated reports of the same outcome harmless.
  ///
  /// An outcome reported while the handler that handed off is still running
  /// is applied as soon as it returns, unless it throws.
  Future<bool> completeHandOff(String id, {String? reference}) {
    return _resolveHandOff(id, reference, null, null);
  }

  /// Fails a task that was handed off with [TaskContext.handOff], as if its
  /// handler had thrown [error]: the type's `retryIf`, `retryDelay`, and
  /// `deferOn` rules and the retry policy decide whether it is retried
  /// (running the handler again), deferred, or failed for good. A
  /// [TaskCancelledException] cancels it.
  ///
  /// [reference] and the return value work as for [completeHandOff].
  Future<bool> failHandOff(
    String id,
    Object error, {
    StackTrace? stackTrace,
    String? reference,
  }) {
    return _resolveHandOff(
      id,
      reference,
      error,
      stackTrace ?? StackTrace.current,
    );
  }

  Future<bool> _resolveHandOff(
    String id,
    String? reference,
    Object? error,
    StackTrace? stackTrace,
  ) async {
    _ensureOpen();
    final pending = _pendingHandOffs[id];
    if (pending != null) {
      if (reference != null && reference != pending.reference) return false;
      if (pending.outcome != null) return false;
      pending.outcome = _HandOffOutcome(error, stackTrace);
      return true;
    }
    final current = await _lock.synchronized(() => _storage.get(id));
    if (current == null ||
        !current.isHandedOff ||
        _cancelSignals.containsKey(id) ||
        (reference != null && reference != current.handOffReference)) {
      return false;
    }
    final changed = error == null
        ? await _markCompleted(current)
        : await _failHandedOff(
            current,
            _registry.find(current.type),
            error,
            stackTrace ?? StackTrace.empty,
          );
    if (changed && _runState == QueueRunState.running) unawaited(_pump());
    return changed;
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

  /// Returns how many stored tasks are in each status.
  ///
  /// Every [TaskStatus] is a key, with zero for statuses no task is in. Pass
  /// [group] to count only that group's tasks. Useful for a badge such as
  /// "3 uploads waiting" or a health check.
  Future<Map<TaskStatus, int>> counts({String? group}) {
    return _lock.synchronized(() async {
      final result = {for (final status in TaskStatus.values) status: 0};
      if (group != null) {
        for (final task in await _storage.getByGroup(group)) {
          result[task.status] = result[task.status]! + 1;
        }
        return result;
      }
      for (final status in TaskStatus.values) {
        result[status] = (await _storage.getByStatus(status)).length;
      }
      return result;
    });
  }

  /// Waits until the task [id] is [TaskStatus.completed],
  /// [TaskStatus.failed], or [TaskStatus.cancelled], and returns its record.
  ///
  /// Returns at once when the task has already finished. The queue must be
  /// running, or be started, for the task to make progress; this method does
  /// not start it. Check [StoredTask.status] on the result to see how the
  /// task finished, and [StoredTask.lastFailure] for why it did not
  /// complete.
  ///
  /// Throws [TaskNotFoundException] if [id] is not stored, or is deleted
  /// while finishing. Throws [TimeoutException] if [timeout] passes first,
  /// measured on the queue clock, and [StateError] if the queue is closed
  /// before the task finishes.
  Future<StoredTask> waitFor(String id, {Duration? timeout}) async {
    _ensureOpen();
    final done = Completer<StoredTask>();
    var finished = false;
    void finish(StoredTask? task) {
      if (finished || done.isCompleted) return;
      if (task == null) {
        done.completeError(TaskNotFoundException(id));
      } else if (task.status.isTerminal) {
        done.complete(task);
      }
    }

    final subscription = events.listen(
      (event) {
        if (event.taskId != id) return;
        if (event is! TaskCompleted &&
            event is! TaskFailed &&
            event is! TaskCancelled) {
          return;
        }
        getTask(id).then(
          finish,
          onError: (Object error, StackTrace trace) {
            if (!finished && !done.isCompleted) {
              done.completeError(error, trace);
            }
          },
        );
      },
      onDone: () {
        if (!done.isCompleted) {
          done.completeError(
            StateError('The queue was closed before task "$id" finished'),
          );
        }
      },
    );
    QueueDelay? limit;
    if (timeout != null) {
      limit = _clock.delayUntil(_clock.now().add(timeout));
      unawaited(
        limit.future.then((_) {
          if (!finished && !done.isCompleted) {
            done.completeError(
              TimeoutException('Task "$id" did not finish', timeout),
            );
          }
        }),
      );
    }
    try {
      finish(await getTask(id));
      return await done.future;
    } finally {
      finished = true;
      await subscription.cancel();
      limit?.cancel();
    }
  }

  /// Puts a [TaskStatus.failed] or [TaskStatus.cancelled] task back in the
  /// queue and returns its id.
  ///
  /// The task keeps its id, payload, keys, priority, group, dependencies,
  /// `createdAt`, and `sequence`, so it runs in its original order relative
  /// to other stored tasks. It becomes [TaskStatus.pending], or
  /// [TaskStatus.waiting] while a dependency is still active, with a fresh
  /// attempt budget: `attempts` resets to zero. Tasks that were cancelled
  /// because this one failed are not revived; retry them as well.
  /// [StoredTask.lastFailure] is kept for reference until the next attempt
  /// replaces or clears it. An expiry that has already passed is cleared, so
  /// the task can run; a later one is kept. Pass [retryPolicy] to replace
  /// the stored policy.
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
          expiresAt: _isExpired(task, now) ? null : task.expiresAt,
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

  /// Starts the queue, runs it until nothing is left to do now or until
  /// [timeLimit] has passed, then stops it.
  ///
  /// Use it where an app gets a limited time to work, such as a background
  /// job started by the operating system: run what is ready, then return.
  ///
  /// * The queue must be idle, as for [start]. Startup recovery runs first.
  /// * "Nothing left to do" means no handler is running and no task is
  ///   ready to start. Tasks scheduled for later, waiting for a retry, held
  ///   by a closed gate, or waiting for a rate-limit slot do not keep the
  ///   drain going; [DrainResult.nextWakeAt] says when the earliest of them
  ///   is due. With [waitForScheduled], the drain also waits for timed work
  ///   that becomes due before [timeLimit] (without a limit: for all timed
  ///   work).
  /// * When [timeLimit] passes, no new task starts. Handlers already running
  ///   are allowed to finish, so the call can return after the limit; leave
  ///   headroom for your longest task.
  ///
  /// Throws [StateError] unless the queue is idle, and [ArgumentError] if
  /// [timeLimit] is not positive. Errors from [start] are passed on.
  Future<DrainResult> drain({
    Duration? timeLimit,
    bool waitForScheduled = false,
  }) async {
    _ensureOpen();
    if (timeLimit != null && timeLimit <= Duration.zero) {
      throw ArgumentError.value(timeLimit, 'timeLimit', 'Must be positive');
    }
    if (_runState != QueueRunState.idle) {
      throw StateError('Cannot drain a queue from $_runState');
    }
    final startedAt = _clock.now();
    final deadline = timeLimit == null ? null : startedAt.add(timeLimit);
    final stats = _drainStats = _DrainStats();
    QueueDelay? limit;
    var timedOut = false;
    var finished = false;
    try {
      await start();
      if (deadline != null) {
        final delay = limit = _clock.delayUntil(deadline);
        unawaited(
          delay.future.then((_) {
            // Cancelling the delay completes it too; that is not a timeout.
            if (finished) return;
            timedOut = true;
            _signalActivity();
          }),
        );
      }
      while (!timedOut) {
        // Take the signal before checking, so activity during the check is
        // not missed.
        final activity = _activity ??= Completer<void>();
        if (await _isDrained(deadline, waitForScheduled)) break;
        if (timedOut) break;
        await activity.future;
      }
    } finally {
      finished = true;
      limit?.cancel();
      await stop();
      _drainStats = null;
    }
    final nextWakeAt = await _lock.synchronized(_storage.getNextWakeAt);
    return DrainResult(
      reason: timedOut ? DrainStopReason.timeLimit : DrainStopReason.idle,
      started: stats.started,
      completed: stats.completed,
      failed: stats.failed,
      retryScheduled: stats.retryScheduled,
      elapsed: _clock.now().difference(startedAt),
      nextWakeAt: nextWakeAt,
    );
  }

  Future<bool> _isDrained(DateTime? deadline, bool waitForScheduled) {
    return _lock.synchronized(() async {
      if (_inFlight > 0 || _pumping) return false;
      if (_runState != QueueRunState.running) return false;
      final now = _clock.now();
      final ready = await _retryStorage(
        () => _storage.getNextReady(now),
        'getNextReady',
      );
      if (ready != null) return false;
      if (!waitForScheduled) return true;
      final wake = await _retryStorage(
        () => _storage.getNextWakeAt(),
        'getNextWakeAt',
      );
      if (wake == null) return true;
      return deadline != null && wake.isAfter(deadline);
    });
  }

  /// Puts every [TaskStatus.failed] task back in the queue, oldest first,
  /// and returns the ids that were retried.
  ///
  /// Failed tasks are this queue's dead-letter queue: they stay stored with
  /// their last error until retried, deleted, or purged. Each task is
  /// retried as by [retry], with a fresh attempt budget and, if given,
  /// [retryPolicy]. Pass [group] to retry only that group. Tasks [retry]
  /// refuses (for example a dependency that did not complete, a
  /// deduplication key held by another active task, or a type that is no
  /// longer registered) are skipped and stay failed.
  Future<List<String>> retryFailed({
    String? group,
    RetryPolicy? retryPolicy,
  }) async {
    _ensureOpen();
    final failed = await getTasks(status: TaskStatus.failed, group: group);
    final retried = <String>[];
    for (final task in failed) {
      try {
        await retry(task.id, retryPolicy: retryPolicy);
        retried.add(task.id);
      } on StateError {
        continue;
      } on UnknownTaskTypeException {
        continue;
      }
    }
    return retried;
  }

  /// Whether a rate limit named [name] exists: one added with
  /// [addRateLimit], or `type:<task type>` for `register(rateLimit:)`.
  bool hasRateLimit(String name) {
    if (_sharedLimits.containsKey(name)) return true;
    return name.startsWith('type:') &&
        _typeLimiters.containsKey(name.substring(5));
  }

  /// Stops rate limit [name] from starting tasks for [duration], for one
  /// [key] of a keyed limit or for the whole limit when [key] is null.
  ///
  /// Use it when a service says "slow down", for example an HTTP 429 with
  /// `Retry-After`: every task in that bucket waits, not only the one that
  /// got the answer. Tasks that already hold a later slot keep it. After the
  /// hold, starts resume at the limit's steady rate (`per / count` apart),
  /// not with a burst.
  ///
  /// Throws [ArgumentError] if no such limit exists or [duration] is
  /// negative.
  void holdRateLimit(String name, {String? key, required Duration duration}) {
    _ensureOpen();
    if (duration.isNegative) {
      throw ArgumentError.value(duration, 'duration', 'Must not be negative');
    }
    final RateLimiter limiter;
    final shared = _sharedLimits[name];
    if (shared != null) {
      limiter = shared.limiter;
    } else if (name.startsWith('type:') &&
        _typeLimiters.containsKey(name.substring(5))) {
      limiter = _typeLimiters[name.substring(5)]!;
    } else {
      throw ArgumentError.value(name, 'name', 'No such rate limit');
    }
    limiter.hold(key, _clock.now().add(duration));
    // Slots reserved before the hold would let tasks through early.
    final bucket = _Bucket(limiter, key, name).id;
    _reservations.removeWhere(
      (_, reservation) => reservation.buckets.contains(bucket),
    );
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
    for (final entry in _gates.values) {
      await entry.subscription?.cancel();
    }
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
          final next = await _lock.synchronized(_claimNext);
          if (next == null) break;
          final deferred = next.deferred;
          if (deferred != null) {
            _emit(deferred);
            continue;
          }
          final expired = next.expired;
          if (expired != null) {
            _emitAll(expired);
            continue;
          }
          final claimed = next.claimed!;
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
    _signalActivity();
  }

  /// Wakes a pending [drain] so it checks whether the queue is done.
  void _signalActivity() {
    final activity = _activity;
    _activity = null;
    if (activity != null && !activity.isCompleted) activity.complete();
  }

  /// Claims the next ready task, or defers it when a gate or rate limit does
  /// not allow it to start yet. Returns null when nothing is ready.
  Future<_Claim?> _claimNext() async {
    if (_runState != QueueRunState.running) return null;
    final task = await _retryStorage(
      () => _storage.getNextReady(_clock.now()),
      'getNextReady',
    );
    if (task == null) return null;
    final now = _clock.now();
    if (_isExpired(task, now)) {
      _reservations.remove(task.id);
      return _Claim.expired(await _commit(_expired(task, now), _retryStorage));
    }
    final deferral = _deferralFor(task, now);
    if (deferral != null) {
      await _retryStorage(
        () => _storage.update(
          task.copyWith(updatedAt: now, nextAttemptAt: deferral.until),
        ),
        'update',
      );
      if (deferral.reason == DeferralReason.gateClosed) {
        _gates[deferral.name]?.deferred[task.id] = deferral.until;
      }
      return _Claim.deferred(
        TaskDeferred(
          taskId: task.id,
          taskType: task.type,
          occurredAt: now,
          reason: deferral.reason,
          name: deferral.name,
          until: deferral.until,
        ),
      );
    }
    // Claim only one due retry rather than promoting the whole backlog.
    final claimed = task.copyWith(
      status: TaskStatus.running,
      attempts: task.attempts + 1,
      updatedAt: now,
      nextAttemptAt: null,
      handOffReference: null,
    );
    await _retryStorage(() => _storage.update(claimed), 'update');
    return _Claim.claimed(claimed);
  }

  static bool _isExpired(StoredTask task, DateTime now) {
    final expiresAt = task.expiresAt;
    return expiresAt != null && !now.isBefore(expiresAt);
  }

  /// The record and event for [task] reaching its expiry before it started.
  _Resolved _expired(StoredTask task, DateTime now) {
    final expiresAt = task.expiresAt!;
    return _Resolved(
      task.copyWith(
        status: TaskStatus.cancelled,
        updatedAt: now,
        nextAttemptAt: null,
        cancelRequested: false,
        lastFailure: TaskFailure(
          error: TaskExpiredException(
            taskId: task.id,
            expiresAt: expiresAt,
          ).toString(),
          stackTrace: null,
          failedAt: now,
          attempt: task.attempts,
        ),
      ),
      TaskCancelled(
        taskId: task.id,
        taskType: task.type,
        occurredAt: now,
        wasRunning: false,
      ),
    );
  }

  /// Decides whether [task] must wait for a gate or a rate limit. Takes a
  /// rate-limit slot when the task may start now or is deferred to a later
  /// slot. Must run inside [_lock].
  _Deferral? _deferralFor(StoredTask task, DateTime now) {
    final registration = _registry.find(task.type);
    // Unknown types fail when claimed; they never start a handler.
    if (registration == null) return null;

    for (final gate in registration.requires) {
      final entry = _gates[gate];
      if (entry != null && !entry.gate.isOpen) {
        // A closed gate wastes any slot the task held.
        _reservations.remove(task.id);
        return _Deferral(
          DeferralReason.gateClosed,
          gate,
          now.add(gateRecheckInterval),
        );
      }
    }

    final buckets = <_Bucket>[];
    final typeLimiter = _typeLimiters[task.type];
    if (typeLimiter != null) {
      buckets.add(_Bucket(typeLimiter, null, 'type:${task.type}'));
    }
    for (final MapEntry(key: name, value: limit) in _sharedLimits.entries) {
      final types = limit.types;
      if (types != null && !types.contains(task.type)) continue;
      String? key;
      final keyOf = limit.keyOf;
      if (keyOf != null) {
        try {
          key = keyOf(task);
        } on Object {
          continue;
        }
        if (key == null) continue;
      }
      buckets.add(_Bucket(limit.limiter, key, name));
    }

    // Slots reserved when the task was deferred are already paid for.
    final reservation = _reservations.remove(task.id);
    final paid = reservation != null && !reservation.slot.isAfter(now)
        ? reservation.buckets
        : const <String>{};
    final unpaid = [
      for (final bucket in buckets)
        if (!paid.contains(bucket.id)) bucket,
    ];
    if (unpaid.isEmpty) return null;

    // Only buckets that are full now are reserved for later. Taking a future
    // slot in a bucket with room would hold back other tasks for no reason.
    var slot = now;
    var limitName = unpaid.first.name;
    final blocking = <_Bucket>[];
    for (final bucket in unpaid) {
      final earliest = bucket.limiter.earliest(bucket.key, now);
      if (!earliest.isAfter(now)) continue;
      blocking.add(bucket);
      if (earliest.isAfter(slot)) {
        slot = earliest;
        limitName = bucket.name;
      }
    }
    _pruneLimits(now);
    if (blocking.isEmpty) {
      for (final bucket in unpaid) {
        bucket.limiter.consume(bucket.key, now);
      }
      return null;
    }
    for (final bucket in blocking) {
      bucket.limiter.consume(bucket.key, slot);
    }
    _reservations[task.id] = (
      slot: slot,
      buckets: {...paid, for (final bucket in blocking) bucket.id},
    );
    return _Deferral(DeferralReason.rateLimit, limitName, slot);
  }

  var _claimsSincePrune = 0;

  /// Keeps limiter keys and stale reservations from growing without bound.
  void _pruneLimits(DateTime now) {
    if (++_claimsSincePrune < 1000) return;
    _claimsSincePrune = 0;
    for (final limiter in _typeLimiters.values) {
      limiter.prune(now);
    }
    for (final limit in _sharedLimits.values) {
      limit.limiter.prune(now);
    }
    // A reservation more than a day overdue belongs to a task that was
    // cancelled, deleted, or claimed by another path.
    final stale = now.subtract(const Duration(days: 1));
    _reservations.removeWhere((_, entry) => entry.slot.isBefore(stale));
  }

  /// Makes every task that [gate] deferred eligible now.
  Future<void> _releaseGate(String gate) async {
    final entry = _gates[gate];
    if (entry == null || entry.deferred.isEmpty) {
      if (_runState == QueueRunState.running) unawaited(_pump());
      return;
    }
    await _lock.synchronized(() async {
      final deferred = Map<String, DateTime>.of(entry.deferred);
      entry.deferred.clear();
      final now = _clock.now();
      for (final MapEntry(key: id, value: until) in deferred.entries) {
        final task = await _retryStorage(() => _storage.get(id), 'get');
        if (task == null || task.nextAttemptAt != until) continue;
        if (task.status != TaskStatus.pending &&
            task.status != TaskStatus.retryScheduled) {
          continue;
        }
        await _retryStorage(
          () => _storage.update(
            task.copyWith(
              updatedAt: now,
              nextAttemptAt: task.status == TaskStatus.pending ? null : now,
            ),
          ),
          'update',
        );
      }
    });
    if (_runState == QueueRunState.running) unawaited(_pump());
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
      _cancelSignals.remove(claimed.id);
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

    var handlerDone = false;
    final cancelSignal = _cancelSignals[claimed.id] = Completer<void>();
    if (_cancelRequested.contains(claimed.id)) cancelSignal.complete();
    final context = TaskContext(
      taskId: claimed.id,
      attempt: claimed.attempts,
      maxAttempts: claimed.retryPolicy.maxAttempts,
      idempotencyKey: claimed.idempotencyKey,
      deduplicationKey: claimed.deduplicationKey,
      isCancellationRequested: () => _cancelRequested.contains(claimed.id),
      whenCancellationRequested: cancelSignal.future,
      onHandOff: (reference) {
        if (handlerDone) {
          throw StateError(
            'The handler for ${claimed.id} has already returned',
          );
        }
        _recordHandOff(claimed, reference);
      },
    );

    try {
      await _withTimeout(
        claimed,
        registration.timeout,
        cancelSignal,
        () => registration.handle(decoded, context),
      );
    } on TaskCancelledException catch (error, stackTrace) {
      await _dropHandOff(claimed.id);
      await _markCancelled(claimed, error, stackTrace);
      return;
    } on Object catch (error, stackTrace) {
      await _dropHandOff(claimed.id);
      if (_cancelRequested.contains(claimed.id)) {
        await _markCancelled(claimed, error, stackTrace);
        return;
      }
      await _handleFailure(claimed, registration, error, stackTrace);
      return;
    } finally {
      handlerDone = true;
    }
    final handOff = _pendingHandOffs[claimed.id];
    if (handOff == null) {
      await _markCompleted(claimed);
      return;
    }
    await _settleHandOff(claimed, registration, handOff);
  }

  /// Runs [handler], failing with [TaskTimeoutException] if it has not
  /// finished within [timeout]. A handler that outlives its timeout is left
  /// running, its result ignored, and [cancelSignal] completes so a handler
  /// that listens for cancellation can stop.
  Future<void> _withTimeout(
    StoredTask claimed,
    Duration? timeout,
    Completer<void> cancelSignal,
    Future<void> Function() handler,
  ) {
    if (timeout == null) return Future<void>.sync(handler);
    final result = Completer<void>();
    final delay = _clock.delayUntil(_clock.now().add(timeout));
    unawaited(
      delay.future.then((_) {
        if (result.isCompleted) return;
        result.completeError(
          TaskTimeoutException(taskId: claimed.id, timeout: timeout),
          StackTrace.current,
        );
        if (!cancelSignal.isCompleted) cancelSignal.complete();
      }),
    );
    Future<void>.sync(handler).then(
      (_) {
        if (!result.isCompleted) result.complete();
        delay.cancel();
      },
      onError: (Object error, StackTrace stackTrace) {
        if (!result.isCompleted) result.completeError(error, stackTrace);
        delay.cancel();
      },
    );
    return result.future;
  }

  /// Records a hand-off made while [claimed]'s handler runs, and saves the
  /// reference right away so a crash before the handler returns does not
  /// start the outside work a second time.
  void _recordHandOff(StoredTask claimed, String reference) {
    if (_pendingHandOffs.containsKey(claimed.id)) {
      throw StateError('Task ${claimed.id} was already handed off');
    }
    final pending = _pendingHandOffs[claimed.id] = _PendingHandOff(reference);
    pending.saved = _lock
        .synchronized(() async {
          final current = await _storage.get(claimed.id);
          if (current == null || current.status != TaskStatus.running) return;
          if (current.attempts != claimed.attempts) return;
          await _storage.update(
            current.copyWith(
              handOffReference: reference,
              updatedAt: _clock.now(),
            ),
          );
        })
        .then<void>((_) {}, onError: (Object _) {});
  }

  /// Forgets a hand-off whose handler then threw. Its early outcome, if any,
  /// is dropped: the throw decides.
  Future<void> _dropHandOff(String id) async {
    final pending = _pendingHandOffs.remove(id);
    if (pending != null) await pending.saved;
  }

  /// After a handler that handed off returned: makes sure the reference is
  /// stored, cancels if cancellation was requested meanwhile, then applies
  /// an outcome that arrived while the handler still ran.
  Future<void> _settleHandOff(
    StoredTask claimed,
    RegisteredTask registration,
    _PendingHandOff handOff,
  ) async {
    await handOff.saved;
    final events = await _lock.synchronized(() async {
      final current = await _retryStorage(
        () => _storage.get(claimed.id),
        'get',
      );
      if (current == null ||
          current.status != TaskStatus.running ||
          current.attempts != claimed.attempts) {
        return const <QueueEvent>[];
      }
      final now = _clock.now();
      final handedOff = current.copyWith(
        handOffReference: handOff.reference,
        updatedAt: now,
      );
      if (current.cancelRequested || _cancelRequested.contains(current.id)) {
        final events = await _commit(
          _handedOffCancelled(handedOff, now),
          _retryStorage,
        );
        _cancelRequested.remove(current.id);
        return events;
      }
      if (current.handOffReference != handOff.reference) {
        await _retryStorage(() => _storage.update(handedOff), 'update');
      }
      return const <QueueEvent>[];
    });
    _pendingHandOffs.remove(claimed.id);
    _emitAll(events);
    final outcome = handOff.outcome;
    if (outcome == null || events.isNotEmpty) return;
    if (outcome.error == null) {
      await _markCompleted(claimed);
    } else {
      await _failHandedOff(
        claimed,
        registration,
        outcome.error!,
        outcome.stackTrace ?? StackTrace.empty,
      );
    }
  }

  _Resolved _handedOffCancelled(StoredTask task, DateTime now) {
    return _Resolved(
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
        wasRunning: true,
      ),
    );
  }

  /// Applies an outside failure to a handed-off task, as if its handler had
  /// thrown [error].
  Future<bool> _failHandedOff(
    StoredTask claimed,
    RegisteredTask? registration,
    Object error,
    StackTrace stackTrace,
  ) async {
    if (error is TaskCancelledException) {
      return _markCancelled(claimed, error, stackTrace);
    }
    if (registration == null) {
      return _failPermanent(claimed, error, stackTrace);
    }
    return _handleFailure(claimed, registration, error, stackTrace);
  }

  Future<bool> _markCompleted(StoredTask claimed) async {
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
    return events.isNotEmpty;
  }

  Future<bool> _markCancelled(
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
    return events.isNotEmpty;
  }

  Future<bool> _failPermanent(
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
    return events.isNotEmpty;
  }

  Future<bool> _handleFailure(
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
        final gate = _gateFor(registration, error, stackTrace);
        if (gate != null) {
          final until = now.add(gateErrorDelay);
          final attempts = current.attempts - 1;
          _gates[gate]?.deferred[current.id] = until;
          _considerWake(until);
          return _Transition(
            current.copyWith(
              status: attempts > 0
                  ? TaskStatus.retryScheduled
                  : TaskStatus.pending,
              attempts: attempts,
              updatedAt: now,
              nextAttemptAt: until,
              lastFailure: failure,
            ),
            TaskDeferred(
              taskId: current.id,
              taskType: current.type,
              occurredAt: now,
              reason: DeferralReason.gateError,
              name: gate,
              until: until,
              error: failure.error,
            ),
          );
        }
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

        final delay =
            _requestedDelay(registration, error, stackTrace) ??
            current.retryPolicy.delayAfter(current.attempts, _nextFraction());
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
    for (final event in events) {
      if (event is TaskDeferred && event.reason == DeferralReason.gateError) {
        _gates[event.name]?.gate.recheck();
      }
    }
    return events.isNotEmpty;
  }

  /// The gate [registration]'s `deferOn` assigns to [error], if it is one of
  /// the type's required gates.
  String? _gateFor(
    RegisteredTask registration,
    Object error,
    StackTrace stackTrace,
  ) {
    final deferOn = registration.deferOn;
    if (deferOn == null) return null;
    final String? gate;
    try {
      gate = deferOn(error, stackTrace);
    } on Object {
      return null;
    }
    if (gate == null || !registration.requires.contains(gate)) return null;
    return gate;
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

  /// The delay an error asks for, clamped to `[0, maxSchedulableDelay]`, or
  /// null to use the retry policy.
  Duration? _requestedDelay(
    RegisteredTask registration,
    Object error,
    StackTrace stackTrace,
  ) {
    final resolver = registration.retryDelay;
    if (resolver == null) return null;
    final Duration? requested;
    try {
      requested = resolver(error, stackTrace);
    } on Object {
      return null;
    }
    if (requested == null) return null;
    if (requested.isNegative) return Duration.zero;
    if (requested > RetryPolicy.maxSchedulableDelay) {
      return RetryPolicy.maxSchedulableDelay;
    }
    return requested;
  }

  void _signalCancel(String id) {
    final signal = _cancelSignals[id];
    if (signal != null && !signal.isCompleted) signal.complete();
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
    _signalActivity();
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
    _drainStats?.count(event);
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

final class _PendingHandOff {
  _PendingHandOff(this.reference);

  final String reference;
  Future<void> saved = Future<void>.value();
  _HandOffOutcome? outcome;
}

final class _HandOffOutcome {
  _HandOffOutcome(this.error, this.stackTrace);

  /// Null for success.
  final Object? error;
  final StackTrace? stackTrace;
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
    this.runAt,
    this.expiresAt,
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
  final DateTime? runAt;
  final DateTime? expiresAt;

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
      runAt: runAt,
      expiresAt: expiresAt,
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

/// A gate added with [DurableQueue.addGate].
final class _GateEntry {
  _GateEntry(this.gate);

  final QueueGate gate;
  StreamSubscription<bool>? subscription;

  /// Tasks this gate deferred in this process, with the time they were
  /// deferred to. Used to release them early when the gate opens.
  final Map<String, DateTime> deferred = {};
}

/// A limit added with [DurableQueue.addRateLimit].
final class _SharedLimit {
  _SharedLimit(this.limiter, this.types, this.keyOf);

  final RateLimiter limiter;
  final Set<String>? types;
  final RateLimitKey? keyOf;
}

/// One rate-limit bucket that applies to a task.
final class _Bucket {
  _Bucket(this.limiter, this.key, this.name);

  final RateLimiter limiter;
  final String? key;
  final String name;

  /// Identifies the bucket across claims.
  String get id => key == null ? name : '$name\u0000$key';
}

/// Why and until when a ready task must wait.
final class _Deferral {
  const _Deferral(this.reason, this.name, this.until);

  final DeferralReason reason;
  final String name;
  final DateTime until;
}

/// Result of one [DurableQueue._claimNext] call.
final class _Claim {
  const _Claim.claimed(StoredTask this.claimed)
    : deferred = null,
      expired = null;
  const _Claim.deferred(TaskDeferred this.deferred)
    : claimed = null,
      expired = null;
  const _Claim.expired(List<QueueEvent> this.expired)
    : claimed = null,
      deferred = null;

  final StoredTask? claimed;
  final TaskDeferred? deferred;

  /// Events from cancelling a task that reached its expiry.
  final List<QueueEvent>? expired;
}

/// Counts events while [DurableQueue.drain] runs.
final class _DrainStats {
  var started = 0;
  var completed = 0;
  var failed = 0;
  var retryScheduled = 0;

  void count(QueueEvent event) {
    switch (event) {
      case TaskStarted():
        started++;
      case TaskCompleted():
        completed++;
      case TaskFailed():
        failed++;
      case TaskRetryScheduled():
        retryScheduled++;
      default:
        break;
    }
  }
}
