part of 'durable_queue.dart';

/// Rate limits, gates, and per-type slots: whether a ready task may start
/// now, and the rate-limit state kept in a [QueueStateStorage].
extension _Limits on DurableQueue {
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
        if (_reservations.remove(task.id) != null) _limitsChanged = true;
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
    if (reservation != null) _limitsChanged = true;
    final paid = reservation != null && !reservation.slot.isAfter(now)
        ? reservation.buckets
        : const <String>{};
    final unpaid = [
      for (final bucket in buckets)
        if (!paid.contains(bucket.id)) bucket,
    ];
    if (unpaid.isEmpty) return null;
    _limitsChanged = true;

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

  /// Frees a slot of [type] and lets the first task that waited for one
  /// become ready again.
  void _freeTypeSlot(String type) {
    final running = (_runningByType[type] ?? 1) - 1;
    if (running <= 0) {
      _runningByType.remove(type);
    } else {
      _runningByType[type] = running;
    }
    final waiting = _waitingForSlot[type];
    if (waiting == null || waiting.isEmpty) return;
    unawaited(
      _lock
          .synchronized(() async {
            while (waiting.isNotEmpty) {
              final MapEntry(key: id, value: until) = waiting.entries.first;
              waiting.remove(id);
              final task = await _retryStorage(() => _storage.get(id), 'get');
              if (task == null || task.nextAttemptAt != until) continue;
              if (task.status != TaskStatus.pending &&
                  task.status != TaskStatus.retryScheduled) {
                continue;
              }
              final now = _clock.now();
              await _retryStorage(
                () => _storage.update(
                  task.copyWith(
                    updatedAt: now,
                    nextAttemptAt: task.status == TaskStatus.pending
                        ? null
                        : now,
                  ),
                ),
                'update',
              );
              return;
            }
          })
          .then((_) {
            if (_runState == QueueRunState.running) unawaited(_pump());
          }),
    );
  }

  /// Writes the rate-limit state to a [QueueStateStorage] if it changed.
  /// Must run inside [_lock].
  Future<void> _saveLimits() async {
    if (!_limitsChanged) return;
    _limitsChanged = false;
    if (_storage case final QueueStateStorage state) {
      final value = _encodeLimits(_clock.now());
      await _retryStorage(
        () => state.writeState(DurableQueue._rateLimitStateKey, value),
        'writeState',
      );
    }
  }

  /// Reads the rate-limit state saved by an earlier queue on the same
  /// storage, once. Must run inside [_lock].
  Future<void> _loadLimits() async {
    if (_limitsLoaded) return;
    if (_typeLimiters.isEmpty && _sharedLimits.isEmpty) return;
    if (_storage case final QueueStateStorage state) {
      final value = await state.readState(DurableQueue._rateLimitStateKey);
      if (value != null) {
        try {
          _restoreLimits(jsonDecode(value) as Map<String, dynamic>);
        } on Object {
          // State that cannot be read is dropped: limits start empty, as
          // they did before state was kept.
        }
      }
    }
    _limitsLoaded = true;
  }

  String? _encodeLimits(DateTime now) {
    Map<String, Object> limits(Map<String, RateLimiter> limiters) => {
      for (final MapEntry(key: name, value: limiter) in limiters.entries)
        if (limiter.activeAt(now) case final active when active.isNotEmpty)
          name: {
            'interval': limiter.interval.inMicroseconds,
            'tolerance': limiter.tolerance.inMicroseconds,
            'keys': [
              for (final MapEntry(:key, :value) in active.entries)
                [key, value.microsecondsSinceEpoch],
            ],
          },
    };
    final types = limits(_typeLimiters);
    final shared = limits({
      for (final MapEntry(:key, :value) in _sharedLimits.entries)
        key: value.limiter,
    });
    if (types.isEmpty && shared.isEmpty && _reservations.isEmpty) return null;
    return jsonEncode({
      'version': 1,
      'types': types,
      'shared': shared,
      'reservations': {
        for (final MapEntry(key: id, value: reservation)
            in _reservations.entries)
          id: {
            'slot': reservation.slot.microsecondsSinceEpoch,
            'buckets': reservation.buckets.toList(),
          },
      },
    });
  }

  void _restoreLimits(Map<String, dynamic> json) {
    if (json['version'] != 1) return;
    DateTime time(Object? micros) =>
        DateTime.fromMicrosecondsSinceEpoch(micros! as int, isUtc: true);
    void restore(RateLimiter? limiter, Object? entry) {
      // A limit that changed since the state was saved starts empty.
      if (limiter == null || entry is! Map) return;
      if (entry['interval'] != limiter.interval.inMicroseconds ||
          entry['tolerance'] != limiter.tolerance.inMicroseconds) {
        return;
      }
      for (final pair in entry['keys'] as List) {
        final [key, micros] = pair as List;
        limiter.restore(key as String?, time(micros));
      }
    }

    (json['types'] as Map).forEach(
      (type, entry) => restore(_typeLimiters[type], entry),
    );
    (json['shared'] as Map).forEach(
      (name, entry) => restore(_sharedLimits[name]?.limiter, entry),
    );
    (json['reservations'] as Map).forEach((id, entry) {
      final reservation = entry as Map;
      _reservations.putIfAbsent(
        id as String,
        () => (
          slot: time(reservation['slot']),
          buckets: {...(reservation['buckets'] as List).cast<String>()},
        ),
      );
    });
  }
}
