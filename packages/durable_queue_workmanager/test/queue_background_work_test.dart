import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_workmanager/durable_queue_workmanager.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:workmanager/workmanager.dart'
    show
        Constraints,
        ForegroundServiceConfig,
        ExistingPeriodicWorkPolicy,
        ExistingWorkPolicy,
        NetworkType;

import 'support/fakes.dart';

void main() {
  late FakeQueueClock clock;
  late LeaseSlot slot;
  late FakeWorkmanager workmanager;
  late MemoryQueueStorage storage;
  late List<String> ran;
  late List<Object> errors;

  setUp(() {
    clock = FakeQueueClock(DateTime.utc(2026, 10, 22));
    slot = LeaseSlot();
    workmanager = FakeWorkmanager();
    storage = MemoryQueueStorage();
    ran = [];
    errors = [];
  });

  QueueBackgroundWork work({
    bool? scheduleWakeUps = true,
    Constraints? constraints,
    Duration? timeLimit,
    Future<void> Function(DurableQueue)? closeQueue,
    DurableQueue Function()? openQueue,
  }) {
    return QueueBackgroundWork(
      openQueue:
          openQueue ?? () => newQueue(clock: clock, storage: storage, ran: ran),
      openLease: () => FakeLease(slot),
      timeLimit: timeLimit,
      closeQueue: closeQueue,
      constraints: constraints,
      scheduleWakeUps: scheduleWakeUps,
      onError: (error, _) => errors.add(error),
      workmanager: workmanager,
      clock: clock,
    );
  }

  Future<void> enqueue(Note note, {Duration? delay}) async {
    final queue = newQueue(clock: clock, storage: storage);
    await queue.enqueue(note, delay: delay);
    await queue.close();
  }

  test('handles its two job names', () {
    final w = work();
    expect(w.handles('durable_queue.drain'), isTrue);
    expect(w.handles('durable_queue.wake'), isTrue);
    expect(w.handles('something else'), isFalse);
    final custom = QueueBackgroundWork(
      openQueue: () => newQueue(clock: clock),
      openLease: () => FakeLease(slot),
      taskName: 'com.example.app.queue',
      wakeTaskName: 'com.example.app.queue.wake',
    );
    expect(custom.handles('com.example.app.queue'), isTrue);
    expect(custom.handles('com.example.app.queue.wake'), isTrue);
    expect(custom.handles('durable_queue.drain'), isFalse);
  });

  test('schedule registers the periodic job', () async {
    final constraints = Constraints(networkType: NetworkType.connected);
    await work(constraints: constraints).schedule();
    final job = workmanager.periodics.single;
    expect(job.uniqueName, 'durable_queue.drain');
    expect(job.taskName, 'durable_queue.drain');
    expect(job.frequency, const Duration(minutes: 15));
    expect(job.initialDelay, isNull);
    expect(job.policy, ExistingPeriodicWorkPolicy.update);
    expect(job.constraints, same(constraints));

    await work().schedule(
      frequency: const Duration(hours: 1),
      initialDelay: const Duration(minutes: 2),
      existingWorkPolicy: ExistingPeriodicWorkPolicy.keep,
    );
    final second = workmanager.periodics.last;
    expect(second.frequency, const Duration(hours: 1));
    expect(second.initialDelay, const Duration(minutes: 2));
    expect(second.policy, ExistingPeriodicWorkPolicy.keep);
  });

  test('cancel cancels both jobs', () async {
    await work().cancel();
    expect(workmanager.cancelled, [
      'durable_queue.drain',
      'durable_queue.wake',
    ]);
  });

  test('run drains and asks to wake for the next due task', () async {
    await enqueue(Note('now'));
    await enqueue(Note('later'), delay: const Duration(minutes: 40));
    final constraints = Constraints(networkType: NetworkType.connected);
    expect(
      await work(constraints: constraints).run('durable_queue.drain'),
      isTrue,
    );
    expect(ran, ['now']);
    final wake = workmanager.oneOffs.single;
    expect(wake.uniqueName, 'durable_queue.wake');
    expect(wake.taskName, 'durable_queue.wake');
    expect(wake.delay, const Duration(minutes: 40));
    expect(wake.policy, ExistingWorkPolicy.replace);
    expect(wake.constraints, same(constraints));
    expect(slot.holder, isNull);
    expect(errors, isEmpty);
  });

  test('inside the wake job the next wake is appended', () async {
    await enqueue(Note('later'), delay: const Duration(minutes: 5));
    expect(await work().run('durable_queue.wake'), isTrue);
    expect(workmanager.oneOffs.single.policy, ExistingWorkPolicy.append);
  });

  test('wake delays have a floor', () async {
    await enqueue(Note('soon'), delay: const Duration(seconds: 10));
    await work().run('durable_queue.drain');
    expect(workmanager.oneOffs.single.delay, const Duration(minutes: 1));
  });

  test('no wake when nothing is due, when disabled, or when skipped', () async {
    expect(await work().run('durable_queue.drain'), isTrue);
    expect(workmanager.oneOffs, isEmpty);

    await enqueue(Note('later'), delay: const Duration(minutes: 30));
    await work(scheduleWakeUps: false).run('durable_queue.drain');
    expect(workmanager.oneOffs, isEmpty);

    slot.holder = FakeLease(slot);
    expect(await work().run('durable_queue.drain'), isTrue);
    expect(workmanager.oneOffs, isEmpty);
  });

  test('by default wakes are scheduled on Android only', () async {
    await enqueue(Note('later'), delay: const Duration(minutes: 30));
    await work(scheduleWakeUps: null).run('durable_queue.drain');
    // These tests run on the host, which is not Android.
    expect(workmanager.oneOffs, isEmpty);
  });

  test('uses the time limit and closeQueue', () async {
    await enqueue(Note('long'));
    await enqueue(Note('next'));
    final release = Completer<void>();
    DurableQueue? closed;
    final done = work(
      timeLimit: const Duration(seconds: 5),
      openQueue: () => newQueue(
        clock: clock,
        storage: storage,
        ran: ran,
        handler: (task) =>
            task.text == 'long' ? release.future : Future<void>.value(),
      ),
      closeQueue: (queue) async {
        closed = queue;
        await queue.close();
      },
    ).run('durable_queue.drain');
    await until(() => ran.isNotEmpty);
    clock.advance(const Duration(seconds: 5));
    await pumpEventQueue();
    release.complete();
    expect(await done, isTrue);
    expect(ran, ['long']);
    expect(closed?.isClosed, isTrue);
    // 'next' is still ready: come back soon.
    expect(workmanager.oneOffs.single.delay, const Duration(minutes: 1));
  });

  test('errors return false and go to onError', () async {
    final ok = await work(openQueue: () => throw StateError('no storage'))
        .run('durable_queue.drain');
    expect(ok, isFalse);
    expect(errors.single, isA<StateError>());
    expect(slot.holder, isNull);
  });

  test('the default time limit fits the platform', () {
    expect(QueueBackgroundWork.defaultTimeLimit, const Duration(minutes: 8));
  });

  test('foregroundService is passed to both jobs', () async {
    final service = ForegroundServiceConfig(notificationTitle: 'Syncing');
    final w = QueueBackgroundWork(
      openQueue: () => newQueue(clock: clock, storage: storage, ran: ran),
      openLease: () => FakeLease(slot),
      scheduleWakeUps: true,
      foregroundService: service,
      workmanager: workmanager,
      clock: clock,
    );
    await w.schedule();
    expect(workmanager.periodics.single.foregroundService, same(service));
    await enqueue(Note('later'), delay: const Duration(minutes: 30));
    await w.run('durable_queue.drain');
    expect(workmanager.oneOffs.single.foregroundService, same(service));
    await work().schedule();
    expect(workmanager.periodics.last.foregroundService, isNull);
  });
}
