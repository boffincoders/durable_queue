import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:math';

import '../clock/queue_clock.dart';
import '../errors.dart';
import '../events/queue_event.dart';
import '../events/task_hold_changed.dart';
import '../events/task_progress.dart';
import '../gates/queue_gate.dart';
import '../internal/async_lock.dart';
import '../internal/rate_limiter.dart';
import '../limits/rate_limit.dart';
import '../registry/task_handler.dart';
import '../registry/task_registry.dart';
import '../retry/retry_policy.dart';
import '../schedule/recurring.dart';
import '../storage/queue_storage.dart';
import '../task/dependency_failure_policy.dart';
import '../task/duplicate_policy.dart';
import '../task/durable_task.dart';
import '../task/stored_task.dart';
import '../task/task_context.dart';
import '../task/task_failure.dart';
import '../task/task_status.dart';
import 'drain_result.dart';
import 'queue_run_state.dart';

part 'durable_queue_limits.dart';
part 'durable_queue_settle.dart';
part 'durable_queue_types.dart';
part 'durable_queue_worker.dart';

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
  /// documentation. [failureHistory] is how many failures each task keeps in
  /// [StoredTask.failures], from 0 (only [StoredTask.lastFailure]) to 20;
  /// the default is 5.
  DurableQueue({
    required QueueStorage storage,
    QueueClock? clock,
    this.maxConcurrentTasks = 1,
    this.storageRetryDelay = const Duration(seconds: 1),
    this.gateRecheckInterval = const Duration(minutes: 5),
    this.gateErrorDelay = const Duration(seconds: 30),
    this.failureHistory = 5,
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
    if (failureHistory < 0 || failureHistory > 20) {
      throw ArgumentError.value(
        failureHistory,
        'failureHistory',
        'Must be from 0 to 20',
      );
    }
  }

  /// How many failures each task keeps in [StoredTask.failures]; 0 keeps
  /// only [StoredTask.lastFailure].
  final int failureHistory;

  /// How long a task stored by a newer app version waits before it is
  /// checked again. See [TaskVersionException].
  static const _newerVersionRecheck = Duration(hours: 1);

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

  /// Whether rate-limit state changed since it was last saved to a
  /// [QueueStateStorage].
  var _limitsChanged = false;

  /// Whether rate-limit state was read from a [QueueStateStorage].
  var _limitsLoaded = false;

  /// The [QueueStateStorage] key of the rate-limit state.
  static const _rateLimitStateKey = 'rate_limits';
  final AsyncLock _lock = AsyncLock();
  final StreamController<QueueEvent> _events =
      StreamController<QueueEvent>.broadcast();
  final StreamController<QueueStorageFailure> _storageErrors =
      StreamController<QueueStorageFailure>.broadcast();
  final Set<String> _cancelRequested = {};
  late final RecurringScheduler _recurring = RecurringScheduler(this);

  /// Running handlers per task type, for `register(maxConcurrent:)`.
  final Map<String, int> _runningByType = {};

  /// Tasks put aside because their type had no free slot, per type, in the
  /// order they were reached, with the time they were put aside until.
  final Map<String, LinkedHashMap<String, DateTime>> _waitingForSlot = {};

  final Map<String, TaskProgress> _progress = {};
  final StreamController<TaskProgress> _progressReports =
      StreamController<TaskProgress>.broadcast();

  /// Running tasks whose handler was asked to stop because of a hold. They
  /// go back to the queue when the handler stops, even if released since.
  final Set<String> _holdInterrupted = {};
  final StreamController<TaskHoldChanged> _holdChanges =
      StreamController<TaskHoldChanged>.broadcast();
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

  /// Reports tasks being held, released, or stopped because of a hold (see
  /// [hold]). Broadcast; it does not replay earlier changes.
  ///
  /// Hold changes are not [QueueEvent]s, so switches over [events] are
  /// unaffected. Code that shows tasks listens to both.
  Stream<TaskHoldChanged> get holdChanges => _holdChanges.stream;

  /// Progress reports of running tasks, from [TaskContext.reportProgress]
  /// and [reportProgress]. Broadcast; it does not replay earlier reports.
  ///
  /// Progress is kept in memory only and is not written to storage, so
  /// reporting often costs no database writes. It is cleared when a task's
  /// attempt ends, and starts again with the next attempt.
  Stream<TaskProgress> get progress => _progressReports.stream;

  /// The last progress reported for task [id] in its current attempt, or
  /// null if none was reported, it is not running, or it was reported
  /// before this queue was created.
  TaskProgress? progressOf(String id) => _progress[id];

  /// Reports progress for task [id], for work that runs outside its
  /// handler, such as a handed-off upload (see [TaskContext.handOff]).
  /// Handlers use [TaskContext.reportProgress] instead.
  ///
  /// [fraction] is clamped to 0 to 1; NaN is ignored. The report is not
  /// checked against storage: report only for a task you know is running.
  void reportProgress(String id, double fraction) {
    if (_closed || fraction.isNaN) return;
    final report = TaskProgress(
      taskId: id,
      fraction: fraction.clamp(0.0, 1.0),
      reportedAt: _clock.now(),
    );
    final previous = _progress[id];
    if (previous != null && previous.fraction == report.fraction) return;
    _progress[id] = report;
    if (!_progressReports.isClosed) _progressReports.add(report);
  }

  /// Worker storage failures, reported before each automatic retry.
  ///
  /// A broadcast stream of values, not uncaught stream errors. Subscribe before
  /// starting the worker. An outage holds serialized storage operations and
  /// may delay pause, stop, enqueue, cancellation, and queries until recovery.
  Stream<QueueStorageFailure> get storageErrors => _storageErrors.stream;

  /// The clock the queue uses for every time it stores or reports.
  QueueClock get clock => _clock;

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
  /// [version] is the version of this type's payload, starting at 1. Raise
  /// it when [decoder] changes in a way that cannot read payloads stored by
  /// an earlier release of the app, and pass [migrate] to convert those:
  /// each task records the version it was enqueued with
  /// ([StoredTask.payloadVersion]), and before an attempt of an older task
  /// the queue passes its payload through [migrate], then [decoder]. A
  /// migration that throws fails the task at once, like a payload that
  /// cannot be decoded. A task stored with a version newer than [version],
  /// by a later release, is not run: it waits, without using attempts, until
  /// an app that knows its version runs the queue (see
  /// [TaskVersionException]).
  ///
  /// [maxConcurrent] limits how many handlers of this type run at once, for
  /// example 2 uploads, within the queue's [maxConcurrentTasks]. Ready tasks
  /// of the type wait for a free slot without using attempts, and tasks of
  /// other types run meanwhile. Time a task spends handed off (see
  /// [TaskContext.handOff]) does not take a slot.
  ///
  /// Throws [ArgumentError] if a gate in [requires] has not been added, if
  /// [deferOn] is given without [requires], if [timeout] is not positive,
  /// or if [version] or [maxConcurrent] is less than 1.
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
    int version = 1,
    TaskMigration? migrate,
    int? maxConcurrent,
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
      version: version,
      migrate: migrate,
      maxConcurrent: maxConcurrent,
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
  /// Pass [onDuplicate] [DuplicatePolicy.replace] to run the new payload
  /// instead, so the latest version wins: a task that has not started gets
  /// the new payload and [idempotencyKey], and keeps its other settings, and
  /// a running one is followed by a task with the new payload. This returns
  /// the id of the task that will run it. See [DuplicatePolicy.replace].
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
  /// the expiry is not after both now and the start time, or [onDuplicate]
  /// is [DuplicatePolicy.replace] without a [deduplicationKey] or the key is
  /// held by a task of another type.
  Future<String> enqueue(
    DurableTask task, {
    RetryPolicy? retryPolicy,
    String? deduplicationKey,
    DuplicatePolicy onDuplicate = DuplicatePolicy.keep,
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
    if (onDuplicate == DuplicatePolicy.replace && deduplicationKey == null) {
      throw ArgumentError.value(
        onDuplicate,
        'onDuplicate',
        'Needs a deduplicationKey',
      );
    }
    final start = _runAt(runAt, delay);
    final request = _prepare(
      task,
      retryPolicy: retryPolicy,
      deduplicationKey: deduplicationKey,
      onDuplicate: onDuplicate,
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

  /// Runs a task every [every], for example a sync every six hours, and
  /// returns the id of the occurrence now stored.
  ///
  /// Call it on every launch, after registering the task's type, like
  /// [register]. One occurrence of [name] is stored at a time: each is a
  /// normal task, built by [task], with [retryPolicy] and [priority]. When
  /// it completes, fails, or is cancelled, the next one is stored for the
  /// next slot. Slots are [every] apart, starting at [firstRunAt], or now
  /// for a schedule that has never run.
  ///
  /// The queue only runs while the app runs. If slots were missed, for
  /// example while the app was closed, the task runs once as soon as
  /// possible and then continues on the original slots; missed runs are
  /// not repeated. A crash between one occurrence finishing and the next
  /// being stored is repaired by the next call to [schedule].
  ///
  /// Occurrences are in the group `schedule:<name>`, so
  /// `getTasks(group: 'schedule:<name>')` shows the history, and their
  /// deduplication key is the same. Each has an idempotency key naming its
  /// slot, `schedule:<name>@<UTC time>`, which a handler can send to a
  /// server. Finished occurrences stay stored until purged; purging them
  /// all makes the next occurrence run at once.
  ///
  /// Throws [ArgumentError] if [name] is blank or [every] is shorter than
  /// one second, [StateError] if [name] was already scheduled by this
  /// queue, and what [enqueue] throws.
  Future<String> schedule(
    String name, {
    required Duration every,
    required DurableTask Function() task,
    DateTime? firstRunAt,
    RetryPolicy? retryPolicy,
    int priority = 0,
  }) {
    _ensureOpen();
    return _recurring.add(
      name,
      every: every,
      task: task,
      firstRunAt: firstRunAt,
      retryPolicy: retryPolicy,
      priority: priority,
    );
  }

  /// Stops the schedule [name]: no further occurrences are stored, and one
  /// that has not started is cancelled. A running occurrence finishes.
  /// Returns whether an occurrence was cancelled.
  ///
  /// Works for a schedule added on an earlier launch too. Remove the
  /// [schedule] call from the app as well, or the next launch starts it
  /// again.
  Future<bool> unschedule(String name) {
    _ensureOpen();
    return _recurring.remove(name);
  }

  /// Names passed to [schedule] on this queue, and not unscheduled.
  Set<String> get schedules => _recurring.names;

  /// Persists [tasks] in one serialized operation and returns their ids in
  /// order.
  ///
  /// Unlike [enqueueChain], the tasks do not depend on each other: they run
  /// in their usual order, at the same time when slots allow. [retryPolicy],
  /// [priority], [group], [dependsOn], [onDependencyFailure], [runAt] or
  /// [delay], and [expiresAt] or [expiresAfter] apply to every task. For a
  /// deduplication or idempotency key per task, call [enqueue] for each.
  ///
  /// Every task is validated before anything is stored, and a running queue
  /// starts work once, after all are stored. A storage error part-way
  /// through can still leave the first tasks stored.
  ///
  /// Throws [ArgumentError] if [tasks] is empty, plus everything [enqueue]
  /// throws.
  Future<List<String>> enqueueAll(
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
    final start = _runAt(runAt, delay);
    final expiry = _expiresAt(expiresAt, expiresAfter, start);
    final requests = [
      for (final task in tasks)
        _prepare(
          task,
          retryPolicy: retryPolicy,
          priority: priority,
          onDependencyFailure: onDependencyFailure,
          group: group,
          dependsOn: dependsOn,
          runAt: start,
          expiresAt: expiry,
        ),
    ];
    final events = <QueueEvent>[];
    final ids = await _lock.synchronized(() async {
      return [for (final request in requests) await _store(request, events)];
    });
    _emitAll(events);
    if (_runState == QueueRunState.running) unawaited(_pump());
    return ids;
  }

  _EnqueueRequest _prepare(
    DurableTask task, {
    RetryPolicy? retryPolicy,
    String? deduplicationKey,
    DuplicatePolicy onDuplicate = DuplicatePolicy.keep,
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
    final registration = _registry.find(task.type);
    if (registration == null) throw _registry.unknownType(task.type);
    return _EnqueueRequest(
      payloadVersion: registration.version,
      type: task.type,
      payload: _jsonPayload(task.toJson()),
      retryPolicy: retryPolicy ?? RetryPolicy.none(),
      deduplicationKey: deduplicationKey,
      onDuplicate: onDuplicate,
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
      if (existing != null) {
        if (request.onDuplicate == DuplicatePolicy.keep) return existing.id;
        return _replace(existing, request, events);
      }
    }
    return _insert(request, events);
  }

  /// Gives the payload of [request] to the active task [existing] holding
  /// its deduplication key, for [DuplicatePolicy.replace]. Must run inside
  /// [_lock].
  Future<String> _replace(
    StoredTask existing,
    _EnqueueRequest request,
    List<QueueEvent> events,
  ) async {
    final key = request.deduplicationKey!;
    // Only one task holds a key, except the follow-up of a running task
    // stored below. It is the one that runs last, so it gets the payload.
    var target = existing;
    for (final dependent in await _storage.getWaitingDependents(existing.id)) {
      if (dependent.deduplicationKey == key) {
        target = dependent;
        break;
      }
    }
    if (target.type != request.type) {
      throw ArgumentError.value(
        request.type,
        'task',
        'Deduplication key "$key" is held by task "${target.id}" of type '
            '"${target.type}"',
      );
    }
    if (target.status != TaskStatus.running) {
      final now = _clock.now();
      await _storage.update(
        target.copyWith(
          payload: request.payload,
          payloadVersion: request.payloadVersion,
          // An earlier attempt may have reached the server under the old
          // key; a server that honours it would never apply this payload.
          idempotencyKey: request.idempotencyKey,
          updatedAt: now,
        ),
      );
      events.add(
        TaskEnqueued(
          taskId: target.id,
          taskType: target.type,
          occurredAt: now,
          deduplicationKey: target.deduplicationKey,
          idempotencyKey: request.idempotencyKey,
          replaced: true,
        ),
      );
      return target.id;
    }
    // The running attempt keeps its payload. The new one runs after it,
    // whether it completes or not, unless the caller chose dependencies and
    // a policy of their own.
    return _insert(
      request.withDependencies(
        [target.id, ...request.dependsOn],
        onDependencyFailure: request.dependsOn.isEmpty
            ? DependencyFailurePolicy.run
            : null,
      ),
      events,
    );
  }

  /// Stores [request] as a new task. Must run inside [_lock].
  Future<String> _insert(
    _EnqueueRequest request,
    List<QueueEvent> events,
  ) async {
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
        payloadVersion: request.payloadVersion,
      ),
      verdict,
      now,
    );
    await _storage.save(_withHistory(stored.task));
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
  /// retried. A running task that was held (see [hold]) goes back to the
  /// queue held, and that attempt does not count. Tasks handed off with
  /// [TaskContext.handOff] are not interrupted: they keep running outside
  /// the queue and are left alone.
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
      await _loadLimits();
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

  /// Holds task [id] until [release] is called, for example when a user
  /// pauses one upload. Returns false, and changes nothing, if the task is
  /// already held, finished, or being cancelled.
  ///
  /// * `pending`, `waiting`, and `retryScheduled` keep their status but do
  ///   not start. The start or retry time is kept for [release]; tasks that
  ///   depend on this one keep waiting.
  /// * `running` is asked to stop: [TaskContext.isCancellationRequested]
  ///   becomes true and [TaskContext.whenCancellationRequested] completes.
  ///   When the handler then throws, including [TaskCancelledException], the
  ///   task goes back to the queue held, and the interrupted attempt does
  ///   not count. If the handler returns normally, the task completes.
  /// * A task handed off with [TaskContext.handOff] stays running. Whoever
  ///   runs the outside work is told on [holdChanges] and pauses it, or
  ///   stops it with [failHandOff], which puts the task back held; until
  ///   then the work continues and its outcome still applies.
  ///   `durable_queue_transfer` does this for uploads and downloads.
  ///
  /// Holding does not stop the task from expiring (see
  /// `enqueue(expiresAt:)`): a task released after its expiry is cancelled.
  /// A held task holds its deduplication key and counts under its status in
  /// [counts]. [cancel] still cancels it.
  ///
  /// Throws [TaskNotFoundException] when [id] is not stored.
  Future<bool> hold(String id) async {
    _ensureOpen();
    final change = await _lock.synchronized(() async {
      final task = await _storage.get(id);
      if (task == null) throw TaskNotFoundException(id);
      if (task.held || task.status.isTerminal || task.cancelRequested) {
        return null;
      }
      if (_cancelRequested.contains(id)) return null;
      final now = _clock.now();
      if (task.status == TaskStatus.running) {
        final held = task.copyWith(held: true, updatedAt: now);
        await _storage.update(held);
        if (_cancelSignals.containsKey(id) && !task.isHandedOff) {
          _holdInterrupted.add(id);
          _signalCancel(id);
        }
        return held;
      }
      _reservations.remove(id);
      for (final gate in _gates.values) {
        gate.deferred.remove(id);
      }
      final held = task.copyWith(
        held: true,
        releaseAt: task.nextAttemptAt,
        nextAttemptAt: StoredTask.heldNextAttemptAt,
        updatedAt: now,
      );
      await _storage.update(held);
      return held;
    });
    if (change == null) return false;
    _emitHold(change);
    return true;
  }

  /// Releases task [id] held by [hold]. Returns false, and changes nothing,
  /// if it is not held.
  ///
  /// A task that was waiting to start may start again at its original start
  /// or retry time, or at once if that has passed. A running task whose
  /// handler was already asked to stop still goes back to the queue when it
  /// stops, without counting the attempt, and starts again. A handed-off
  /// task is reported on [holdChanges] so the outside work can resume.
  ///
  /// Throws [TaskNotFoundException] when [id] is not stored.
  Future<bool> release(String id) async {
    _ensureOpen();
    final change = await _lock.synchronized(() async {
      final task = await _storage.get(id);
      if (task == null) throw TaskNotFoundException(id);
      if (!task.held) return null;
      final now = _clock.now();
      final released = task.status == TaskStatus.running
          ? task.copyWith(held: false, releaseAt: null, updatedAt: now)
          : task.copyWith(
              held: false,
              nextAttemptAt: task.releaseAt,
              releaseAt: null,
              updatedAt: now,
            );
      await _storage.update(released);
      return released;
    });
    if (change == null) return false;
    _emitHold(change);
    if (_runState == QueueRunState.running) unawaited(_pump());
    return true;
  }

  /// Puts a running task back in the queue when it stopped because of a
  /// hold. The attempt does not count. Returns false when no hold applies,
  /// or when the task is being cancelled.
  Future<bool> _requeueForHold(StoredTask claimed) async {
    if (_cancelRequested.contains(claimed.id)) return false;
    final requeued = await _lock.synchronized(() async {
      final current = await _retryStorage(
        () => _storage.get(claimed.id),
        'get',
      );
      if (current == null ||
          current.status != TaskStatus.running ||
          current.attempts != claimed.attempts ||
          current.cancelRequested) {
        return null;
      }
      if (!current.held && !_holdInterrupted.contains(current.id)) return null;
      final now = _clock.now();
      final attempts = current.attempts - 1;
      final next = current.copyWith(
        status: attempts > 0 ? TaskStatus.retryScheduled : TaskStatus.pending,
        attempts: attempts,
        updatedAt: now,
        nextAttemptAt: current.held ? StoredTask.heldNextAttemptAt : now,
        releaseAt: null,
        handOffReference: null,
      );
      await _retryStorage(() => _storage.update(next), 'update');
      return next;
    });
    if (requeued == null) return false;
    _holdInterrupted.remove(claimed.id);
    _emitHold(requeued);
    return true;
  }

  void _emitHold(StoredTask task) {
    if (_holdChanges.isClosed) return;
    _holdChanges.add(
      TaskHoldChanged(
        taskId: task.id,
        taskType: task.type,
        occurredAt: task.updatedAt,
        held: task.held,
        status: task.status,
        isHandedOff: task.isHandedOff,
      ),
    );
  }

  /// Whether a wake-up time only comes from held tasks.
  static bool _isHeldTime(DateTime time) =>
      !time.isBefore(StoredTask.heldNextAttemptAt);

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
          held: false,
          releaseAt: null,
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
    final storedWake = await _lock.synchronized(_storage.getNextWakeAt);
    final nextWakeAt = storedWake == null || _isHeldTime(storedWake)
        ? null
        : storedWake;
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
      if (wake == null || _isHeldTime(wake)) return true;
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
    _limitsChanged = true;
    unawaited(_lock.synchronized(_saveLimits));
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
    await _recurring.close();
    await _events.close();
    await _holdChanges.close();
    await _progressReports.close();
    await _storageErrors.close();
  }

  void _ensureOpen() {
    if (_closed) throw StateError('DurableQueue is closed');
    if (_storage case final QueueStorageCallGuard guard) guard.checkCall();
  }

  static bool _sameFailure(TaskFailure a, TaskFailure b) =>
      a.failedAt == b.failedAt && a.attempt == b.attempt && a.error == b.error;

  /// Finished tasks are never held.
  static StoredTask _unheldIfFinished(StoredTask task) {
    if (!task.status.isTerminal || (!task.held && task.releaseAt == null)) {
      return task;
    }
    return task.copyWith(held: false, releaseAt: null);
  }

  static bool _isExpired(StoredTask task, DateTime now) {
    final expiresAt = task.expiresAt;
    return expiresAt != null && !now.isBefore(expiresAt);
  }

  var _claimsSincePrune = 0;
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
