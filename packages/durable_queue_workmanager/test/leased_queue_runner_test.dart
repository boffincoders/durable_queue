import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_workmanager/durable_queue_workmanager.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/delegating_storage.dart';
import 'support/fakes.dart';

final class FailingRecovery extends DelegatingStorage {
  var fail = true;

  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit}) {
    if (fail && status == TaskStatus.running) throw StateError('disk');
    return super.getByStatus(status, limit: limit);
  }
}

void main() {
  late FakeQueueClock clock;
  late LeaseSlot slot;
  late FakeLease lease;
  late List<String> ran;
  late DurableQueue queue;

  setUp(() {
    clock = FakeQueueClock(DateTime.utc(2026, 10, 22));
    slot = LeaseSlot();
    lease = FakeLease(slot);
    ran = [];
    queue = newQueue(clock: clock, ran: ran);
  });

  tearDown(() => queue.close());

  LeasedQueueRunner runner({void Function(Object, StackTrace)? onError}) =>
      LeasedQueueRunner(queue, lease, clock: clock, onError: onError);

  test('start takes the lease and starts; stop gives both back', () async {
    final r = runner();
    expect(r.isRunning, isFalse);
    await r.start();
    expect(r.isRunning, isTrue);
    expect(r.isWanted, isTrue);
    expect(slot.holder, same(lease));
    expect(queue.runState, QueueRunState.running);
    await queue.enqueue(Note('a'));
    await until(() => ran.isNotEmpty);

    await r.start();
    expect(lease.acquireCalls, 1, reason: 'already running');

    await r.stop();
    expect(r.isRunning, isFalse);
    expect(r.isWanted, isFalse);
    expect(queue.runState, QueueRunState.idle);
    expect(slot.holder, isNull);
    await r.stop();
    expect(lease.releaseCalls, 1);
  });

  test('waits while the lease is held elsewhere', () async {
    final other = FakeLease(slot);
    slot.holder = other;
    final r = runner();
    var started = false;
    unawaited(r.start().then((_) => started = true));
    await until(() => lease.acquireCalls == 1);
    await queue.enqueue(Note('queued'));
    await pumpEventQueue();
    expect(ran, isEmpty);
    expect(queue.runState, QueueRunState.idle);

    clock.advance(const Duration(seconds: 4));
    await pumpEventQueue();
    expect(lease.acquireCalls, 1);
    clock.advance(const Duration(seconds: 1));
    await until(() => lease.acquireCalls == 2);
    expect(started, isFalse);

    await other.release();
    clock.advance(const Duration(seconds: 5));
    await until(() => started);
    expect(r.isRunning, isTrue);
    await until(() => ran.isNotEmpty);
    expect(ran, ['queued']);
    await r.stop();
  });

  test('stop ends a start that is waiting for the lease', () async {
    slot.holder = FakeLease(slot);
    final r = runner();
    var started = false;
    unawaited(r.start().then((_) => started = true));
    await until(() => lease.acquireCalls == 1);
    await r.stop();
    await until(() => started);
    expect(r.isRunning, isFalse);
    clock.advance(const Duration(minutes: 1));
    await pumpEventQueue();
    expect(lease.acquireCalls, 1);
    expect(queue.runState, QueueRunState.idle);
  });

  test('stop waits for running handlers before releasing', () async {
    final release = Completer<void>();
    await queue.close();
    queue = newQueue(clock: clock, ran: ran, handler: (_) => release.future);
    final r = runner();
    await r.start();
    await queue.enqueue(Note('long'));
    await until(() => ran.isNotEmpty);
    var stopped = false;
    unawaited(r.stop().then((_) => stopped = true));
    await pumpEventQueue();
    expect(stopped, isFalse);
    expect(slot.holder, same(lease));
    release.complete();
    await until(() => stopped);
    expect(slot.holder, isNull);
  });

  test('renews while running and stops renewing after stop', () async {
    final r = runner();
    await r.start();
    clock.advance(const Duration(seconds: 30));
    await until(() => lease.renewCalls == 1);
    await r.stop();
    clock.advance(const Duration(minutes: 5));
    await pumpEventQueue();
    expect(lease.renewCalls, 1);
  });

  test('a lost lease stops the queue and waits for it again', () async {
    final errors = <Object>[];
    final r = runner(onError: (error, _) => errors.add(error));
    await r.start();
    final other = FakeLease(slot);
    slot.holder = other;
    clock.advance(const Duration(seconds: 30));
    await until(() => !r.isRunning);
    expect(queue.runState, QueueRunState.idle);
    expect(lease.releaseCalls, 0);
    expect(r.isWanted, isTrue);
    await until(() => lease.acquireCalls == 2);

    await other.release();
    clock.advance(const Duration(seconds: 5));
    await until(() => r.isRunning);
    expect(queue.runState, QueueRunState.running);
    expect(errors, isEmpty);
    await r.stop();
  });

  test('a stop after a lost lease does not release the other holder', () async {
    final r = runner();
    await r.start();
    final other = FakeLease(slot);
    slot.holder = other;
    clock.advance(const Duration(seconds: 30));
    await until(() => !r.isRunning);
    await r.stop();
    expect(slot.holder, same(other));
  });

  test('errors while restarting after a loss go to onError', () async {
    final errors = <Object>[];
    final r = runner(onError: (error, _) => errors.add(error));
    await r.start();
    slot.holder = FakeLease(slot);
    lease.acquireError = StateError('disk');
    clock.advance(const Duration(seconds: 30));
    await until(() => errors.isNotEmpty);
    expect(errors.single, isA<StateError>());
    expect(r.isRunning, isFalse);
  });

  test('a failed start releases the lease and can be retried', () async {
    await queue.close();
    final storage = FailingRecovery();
    queue = newQueue(clock: clock, storage: storage, ran: ran);
    final r = runner();
    await expectLater(r.start(), throwsStateError);
    expect(r.isRunning, isFalse);
    expect(r.isWanted, isFalse);
    expect(slot.holder, isNull);
    expect(queue.runState, QueueRunState.idle);

    storage.fail = false;
    await r.start();
    expect(r.isRunning, isTrue);
    await r.stop();
  });

  test('an acquire error is returned and the runner keeps working', () async {
    final r = runner();
    lease.acquireError = StateError('disk');
    await expectLater(r.start(), throwsStateError);
    lease.acquireError = null;
    await r.start();
    expect(r.isRunning, isTrue);
    await r.stop();
  });

  test('does not start a closed queue', () async {
    await queue.close();
    final r = runner();
    await r.start();
    expect(r.isRunning, isFalse);
    expect(lease.acquireCalls, 0);
  });

  test('leaves a queue that is already running alone', () async {
    await queue.start();
    final r = runner();
    await r.start();
    expect(r.isRunning, isTrue);
    await r.stop();
    expect(queue.runState, QueueRunState.idle);
  });

  test('after dispose, start does nothing', () async {
    final r = runner();
    await r.start();
    await r.dispose();
    expect(slot.holder, isNull);
    await r.start();
    expect(r.isRunning, isFalse);
    expect(lease.acquireCalls, 1);
  });

  test('retryInterval must be positive', () {
    expect(
      () => LeasedQueueRunner(queue, lease, retryInterval: Duration.zero),
      throwsArgumentError,
    );
  });
}
