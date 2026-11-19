part of 'durable_queue.dart';

/// The worker: claims ready tasks, runs their handlers, and records each
/// attempt's outcome.
extension _Worker on DurableQueue {
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
            _freeTypeSlot(claimed.type);
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
    final registered = _registry.find(task.type)?.version;
    if (registered != null && task.payloadVersion > registered) {
      final wait = task.copyWith(
        updatedAt: now,
        nextAttemptAt: now.add(DurableQueue._newerVersionRecheck),
        lastFailure: TaskFailure(
          error: TaskVersionException(
            taskId: task.id,
            taskType: task.type,
            storedVersion: task.payloadVersion,
            registeredVersion: registered,
          ).toString(),
          stackTrace: null,
          failedAt: now,
          attempt: task.attempts,
        ),
      );
      await _retryStorage(() => _storage.update(_withHistory(wait)), 'update');
      return const _Claim.expired([]);
    }
    if (DurableQueue._isExpired(task, now)) {
      _reservations.remove(task.id);
      return _Claim.expired(await _commit(_expired(task, now), _retryStorage));
    }
    final limit = _registry.find(task.type)?.maxConcurrent;
    if (limit != null && (_runningByType[task.type] ?? 0) >= limit) {
      // Put it aside until a slot of its type frees, or the recheck time as
      // a fallback if this process forgets it.
      final until = now.add(gateRecheckInterval);
      await _retryStorage(
        () => _storage.update(
          task.copyWith(updatedAt: now, nextAttemptAt: until),
        ),
        'update',
      );
      _waitingForSlot.putIfAbsent(task.type, LinkedHashMap.new)[task.id] =
          until;
      return const _Claim.expired([]);
    }
    final deferral = _deferralFor(task, now);
    await _saveLimits();
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
    _runningByType.update(task.type, (n) => n + 1, ifAbsent: () => 1);
    return _Claim.claimed(claimed);
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
      _holdInterrupted.remove(claimed.id);
      _cancelSignals.remove(claimed.id);
      _freeTypeSlot(claimed.type);
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
      decoded = registration.decode(_migrated(claimed, registration));
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
      isCancellationRequested: () =>
          _cancelRequested.contains(claimed.id) ||
          _holdInterrupted.contains(claimed.id),
      whenCancellationRequested: cancelSignal.future,
      onHandOff: (reference) {
        if (handlerDone) {
          throw StateError(
            'The handler for ${claimed.id} has already returned',
          );
        }
        _recordHandOff(claimed, reference);
      },
      onProgress: (fraction) => reportProgress(claimed.id, fraction),
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
      if (await _requeueForHold(claimed)) return;
      await _markCancelled(claimed, error, stackTrace);
      return;
    } on Object catch (error, stackTrace) {
      await _dropHandOff(claimed.id);
      if (await _requeueForHold(claimed)) return;
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

  /// [claimed]'s payload in the shape its registered version expects.
  Map<String, dynamic> _migrated(
    StoredTask claimed,
    RegisteredTask registration,
  ) {
    final json = _jsonPayload(claimed.payload);
    final from = claimed.payloadVersion;
    final migrate = registration.migrate;
    if (from >= registration.version || migrate == null) return json;
    try {
      return _jsonPayload(migrate(from, json));
    } on Object catch (error) {
      throw TaskDecodeException(
        'Migrating task "${claimed.id}" of type "${claimed.type}" from '
        'version $from to ${registration.version} failed: $error',
      );
    }
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
    StoredTask? heldHandOff;
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
      if (current.held) heldHandOff = handedOff;
      return const <QueueEvent>[];
    });
    _pendingHandOffs.remove(claimed.id);
    _emitAll(events);
    final outcome = handOff.outcome;
    if (heldHandOff != null && outcome == null) _emitHold(heldHandOff!);
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
    if (await _requeueForHold(claimed)) return true;
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
    if (DurableQueue._isHeldTime(earliest)) return;
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
    switch (event) {
      case TaskStarted() ||
          TaskCompleted() ||
          TaskFailed() ||
          TaskCancelled() ||
          TaskRetryScheduled():
        _progress.remove(event.taskId);
      default:
        break;
    }
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
