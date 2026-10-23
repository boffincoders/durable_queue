import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_workmanager/durable_queue_workmanager.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/fakes.dart';

void main() {
  late FakeQueueClock clock;
  late LeaseSlot slot;
  late FakeLease lease;
  late List<String> ran;

  setUp(() {
    clock = FakeQueueClock(DateTime.utc(2026, 10, 22));
    slot = LeaseSlot();
    lease = FakeLease(slot);
    ran = [];
  });

  test('drains, closes, and releases the lease', () async {
    final storage = MemoryQueueStorage();
    final setup = newQueue(clock: clock, storage: storage);
    await setup.enqueue(Note('a'));
    await setup.enqueue(Note('b'));
    await setup.enqueue(Note('later'), delay: const Duration(hours: 1));
    await setup.close();

    late DurableQueue opened;
    final result = await drainWithLease(
      lease: lease,
      openQueue: () =>
          opened = newQueue(clock: clock, storage: storage, ran: ran),
      clock: clock,
    );
    expect(result.ran, isTrue);
    expect(result.leaseLost, isFalse);
    expect(result.drain!.completed, 2);
    expect(result.nextWakeAt, clock.now().add(const Duration(hours: 1)));
    expect(ran, ['a', 'b']);
    expect(opened.isClosed, isTrue);
    expect(slot.holder, isNull);
    expect(result.toString(), contains('completed: 2'));
  });

  test('skips without opening the queue when the lease is held', () async {
    slot.holder = FakeLease(slot);
    var opened = false;
    final result = await drainWithLease(
      lease: lease,
      openQueue: () {
        opened = true;
        return newQueue(clock: clock);
      },
      clock: clock,
    );
    expect(result.ran, isFalse);
    expect(result.drain, isNull);
    expect(result.nextWakeAt, isNull);
    expect(result.toString(), 'LeasedDrainResult(skipped)');
    expect(opened, isFalse);
    expect(lease.releaseCalls, 0);
  });

  test('passes the time limit and calls closeQueue', () async {
    final release = Completer<void>();
    final storage = MemoryQueueStorage();
    final setup = newQueue(clock: clock, storage: storage);
    await setup.enqueue(Note('long'));
    await setup.enqueue(Note('next'));
    await setup.close();

    DurableQueue? closed;
    final done = drainWithLease(
      lease: lease,
      openQueue: () => newQueue(
        clock: clock,
        storage: storage,
        ran: ran,
        handler: (task) =>
            task.text == 'long' ? release.future : Future.value(),
      ),
      timeLimit: const Duration(seconds: 20),
      closeQueue: (queue) async {
        closed = queue;
        await queue.close();
      },
      clock: clock,
    );
    await until(() => ran.isNotEmpty);
    clock.advance(const Duration(seconds: 20));
    await pumpEventQueue();
    release.complete();
    final result = await done;
    expect(result.drain!.reason, DrainStopReason.timeLimit);
    expect(ran, ['long']);
    expect(closed, isNotNull);
    expect(closed!.isClosed, isTrue);
  });

  test('waitForScheduled is passed on', () async {
    final storage = MemoryQueueStorage();
    final setup = newQueue(clock: clock, storage: storage);
    await setup.enqueue(Note('soon'), delay: const Duration(seconds: 5));
    await setup.close();
    final done = drainWithLease(
      lease: lease,
      openQueue: () => newQueue(clock: clock, storage: storage, ran: ran),
      timeLimit: const Duration(seconds: 20),
      waitForScheduled: true,
      clock: clock,
    );
    await pumpEventQueue();
    clock.advance(const Duration(seconds: 5));
    final result = await done;
    expect(ran, ['soon']);
    expect(result.drain!.reason, DrainStopReason.idle);
  });

  test('renews the lease while the drain runs', () async {
    final release = Completer<void>();
    final storage = MemoryQueueStorage();
    final setup = newQueue(clock: clock, storage: storage);
    await setup.enqueue(Note('long'));
    await setup.close();
    final done = drainWithLease(
      lease: lease,
      openQueue: () => newQueue(
        clock: clock,
        storage: storage,
        ran: ran,
        handler: (_) => release.future,
      ),
      clock: clock,
    );
    await until(() => ran.isNotEmpty);
    expect(lease.renewCalls, 0);
    clock.advance(const Duration(seconds: 30));
    await until(() => lease.renewCalls == 1);
    clock.advance(const Duration(seconds: 29));
    await pumpEventQueue();
    expect(lease.renewCalls, 1);
    clock.advance(const Duration(seconds: 1));
    await until(() => lease.renewCalls == 2);
    release.complete();
    await done;
    clock.advance(const Duration(minutes: 5));
    await pumpEventQueue();
    expect(lease.renewCalls, 2, reason: 'renewal stops with the drain');
  });

  test('a renewal error is skipped', () async {
    final release = Completer<void>();
    final storage = MemoryQueueStorage();
    final setup = newQueue(clock: clock, storage: storage);
    await setup.enqueue(Note('long'));
    await setup.close();
    lease.renewError = StateError('disk');
    final done = drainWithLease(
      lease: lease,
      openQueue: () => newQueue(
        clock: clock,
        storage: storage,
        ran: ran,
        handler: (_) => release.future,
      ),
      clock: clock,
    );
    await until(() => ran.isNotEmpty);
    clock.advance(const Duration(seconds: 30));
    await until(() => lease.renewCalls == 1);
    lease.renewError = null;
    clock.advance(const Duration(seconds: 30));
    await until(() => lease.renewCalls == 2);
    release.complete();
    final result = await done;
    expect(result.leaseLost, isFalse);
    expect(slot.holder, isNull);
  });

  test('reports a lost lease and does not release it', () async {
    final release = Completer<void>();
    final storage = MemoryQueueStorage();
    final setup = newQueue(clock: clock, storage: storage);
    await setup.enqueue(Note('long'));
    await setup.close();
    final done = drainWithLease(
      lease: lease,
      openQueue: () => newQueue(
        clock: clock,
        storage: storage,
        ran: ran,
        handler: (_) => release.future,
      ),
      clock: clock,
    );
    await until(() => ran.isNotEmpty);
    final other = FakeLease(slot);
    slot.holder = other;
    clock.advance(const Duration(seconds: 30));
    await until(() => lease.renewCalls == 1);
    clock.advance(const Duration(minutes: 5));
    await pumpEventQueue();
    expect(lease.renewCalls, 1, reason: 'no renewals after the loss');
    release.complete();
    final result = await done;
    expect(result.leaseLost, isTrue);
    expect(result.toString(), contains('leaseLost: true'));
    expect(lease.releaseCalls, 0);
    expect(slot.holder, same(other));
  });

  test('errors are passed on and the lease is released', () async {
    await expectLater(
      drainWithLease(
        lease: lease,
        openQueue: () => throw StateError('no storage'),
        clock: clock,
      ),
      throwsStateError,
    );
    expect(slot.holder, isNull);

    final running = newQueue(clock: clock);
    await running.start();
    var closed = false;
    await expectLater(
      drainWithLease(
        lease: lease,
        openQueue: () => running,
        closeQueue: (queue) async {
          closed = true;
          await queue.close();
        },
        clock: clock,
      ),
      throwsStateError,
    );
    expect(closed, isTrue);
    expect(slot.holder, isNull);
  });

  test('an acquire error is passed on', () async {
    lease.acquireError = StateError('disk');
    await expectLater(
      drainWithLease(
        lease: lease,
        openQueue: () => newQueue(clock: clock),
        clock: clock,
      ),
      throwsStateError,
    );
  });
}
