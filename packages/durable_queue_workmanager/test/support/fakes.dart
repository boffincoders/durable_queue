import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_workmanager/durable_queue_workmanager.dart';
import 'package:workmanager/workmanager.dart'
    show
        Constraints,
        ExistingPeriodicWorkPolicy,
        ExistingWorkPolicy,
        Workmanager;

final class Note extends DurableTask {
  Note(this.text);

  final String text;

  @override
  String get type => 'note';

  @override
  Map<String, dynamic> toJson() => {'text': text};

  static Note fromJson(Map<String, dynamic> json) =>
      Note(json['text'] as String);
}

/// One lease shared by several [FakeLease] holders.
final class LeaseSlot {
  FakeLease? holder;
}

/// An in-memory lease; [slot] says who holds it.
final class FakeLease implements QueueLease {
  FakeLease(this.slot);

  final LeaseSlot slot;
  int acquireCalls = 0;
  int renewCalls = 0;
  int releaseCalls = 0;
  Object? acquireError;
  Object? renewError;

  @override
  Duration get ttl => const Duration(minutes: 2);

  bool get holds => identical(slot.holder, this);

  @override
  Future<bool> tryAcquire() async {
    acquireCalls++;
    if (acquireError case final error?) throw error;
    if (slot.holder == null || holds) {
      slot.holder = this;
      return true;
    }
    return false;
  }

  @override
  Future<bool> renew() async {
    renewCalls++;
    if (renewError case final error?) throw error;
    return holds;
  }

  @override
  Future<void> release() async {
    releaseCalls++;
    if (holds) slot.holder = null;
  }
}

final class OneOff {
  OneOff(
    this.uniqueName,
    this.taskName,
    this.delay,
    this.policy,
    this.constraints, [
    this.foregroundService,
  ]);

  final String uniqueName;
  final String taskName;
  final Duration? delay;
  final Object? foregroundService;
  final ExistingWorkPolicy? policy;
  final Constraints? constraints;
}

final class Periodic {
  Periodic(
    this.uniqueName,
    this.taskName,
    this.frequency,
    this.initialDelay,
    this.policy,
    this.constraints, [
    this.foregroundService,
  ]);

  final String uniqueName;
  final String taskName;
  final Duration? frequency;
  final Object? foregroundService;
  final Duration? initialDelay;
  final ExistingPeriodicWorkPolicy? policy;
  final Constraints? constraints;
}

/// Records registrations instead of talking to a platform.
final class FakeWorkmanager implements Workmanager {
  final oneOffs = <OneOff>[];
  final periodics = <Periodic>[];
  final cancelled = <String>[];

  @override
  Future<void> registerOneOffTask(
    String uniqueName,
    String taskName, {
    Map<String, dynamic>? inputData,
    Duration? initialDelay,
    Constraints? constraints,
    ExistingWorkPolicy? existingWorkPolicy,
    Object? backoffPolicy,
    Duration? backoffPolicyDelay,
    String? tag,
    Object? outOfQuotaPolicy,
    Object? foregroundServiceConfig,
    bool expedited = false,
  }) async {
    oneOffs.add(
      OneOff(
        uniqueName,
        taskName,
        initialDelay,
        existingWorkPolicy,
        constraints,
        foregroundServiceConfig,
      ),
    );
  }

  @override
  Future<void> registerPeriodicTask(
    String uniqueName,
    String taskName, {
    Duration? frequency,
    Duration? flexInterval,
    Map<String, dynamic>? inputData,
    Duration? initialDelay,
    Constraints? constraints,
    ExistingPeriodicWorkPolicy? existingWorkPolicy,
    Object? backoffPolicy,
    Duration? backoffPolicyDelay,
    String? tag,
    Object? foregroundServiceConfig,
  }) async {
    periodics.add(
      Periodic(
        uniqueName,
        taskName,
        frequency,
        initialDelay,
        existingWorkPolicy,
        constraints,
        foregroundServiceConfig,
      ),
    );
  }

  @override
  Future<void> cancelByUniqueName(String uniqueName) async {
    cancelled.add(uniqueName);
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

/// Waits for [condition], letting microtasks and short timers run.
Future<void> until(bool Function() condition, {int tries = 200}) async {
  for (var i = 0; i < tries; i++) {
    if (condition()) return;
    await Future<void>.delayed(Duration.zero);
  }
  if (!condition()) throw StateError('Condition not reached');
}

DurableQueue newQueue({
  required FakeQueueClock clock,
  QueueStorage? storage,
  List<String>? ran,
  Future<void> Function(Note task)? handler,
}) {
  return DurableQueue(storage: storage ?? MemoryQueueStorage(), clock: clock)
    ..register<Note>(
      type: 'note',
      decoder: Note.fromJson,
      handler: (task, context) async {
        ran?.add(task.text);
        if (handler != null) await handler(task);
      },
    );
}
