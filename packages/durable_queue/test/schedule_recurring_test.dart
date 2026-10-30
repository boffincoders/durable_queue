import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  late QueueHarness h;
  late List<String> keys;

  setUp(() {
    h = QueueHarness();
    keys = [];
    h.register(
      handler: (task, context) async {
        keys.add(context.idempotencyKey!);
        if (task.value == 'bad') throw TemporaryFailure();
      },
    );
  });

  tearDown(() => h.queue.close());

  const hour = Duration(hours: 1);
  final t0 = DateTime.utc(2026, 1, 1);
  String key(DateTime slot) => 'schedule:sync@${slot.toIso8601String()}';

  test('runs now, then every interval', () async {
    final first = await h.queue.schedule(
      'sync',
      every: hour,
      task: () => ValueTask('sync'),
    );
    expect(h.queue.schedules, {'sync'});
    final stored = (await h.queue.getTask(first))!;
    expect(stored.group, 'schedule:sync');
    expect(stored.deduplicationKey, 'schedule:sync');

    await h.queue.start();
    await h.until(() => keys.length == 1);
    expect(keys, [key(t0)]);

    h.clock.advance(const Duration(minutes: 59));
    await pumpEventQueue();
    expect(keys, hasLength(1));
    h.clock.advance(const Duration(minutes: 1));
    await h.until(() => keys.length == 2);
    h.clock.advance(hour);
    await h.until(() => keys.length == 3);
    expect(keys, [key(t0), key(t0.add(hour)), key(t0.add(hour * 2))]);

    final history = await h.queue.getTasks(group: 'schedule:sync');
    expect(history.map((t) => t.status), [
      TaskStatus.completed,
      TaskStatus.completed,
      TaskStatus.completed,
      TaskStatus.pending,
    ]);
  });

  test('firstRunAt sets the first slot', () async {
    final at = t0.add(const Duration(minutes: 30));
    final id = await h.queue.schedule(
      'sync',
      every: hour,
      firstRunAt: at,
      task: () => ValueTask('sync'),
    );
    expect((await h.queue.getTask(id))!.nextAttemptAt, at);
  });

  test('a failed or cancelled occurrence still schedules the next', () async {
    await h.queue.schedule('sync', every: hour, task: () => ValueTask('bad'));
    await h.queue.start();
    await h.until(() => h.of<TaskFailed>().length == 1);
    await pumpEventQueue();
    var active = await h.queue.getTasks(
      group: 'schedule:sync',
      status: TaskStatus.pending,
    );
    expect(active.single.nextAttemptAt, t0.add(hour));

    await h.queue.cancel(active.single.id);
    await pumpEventQueue();
    active = await h.queue.getTasks(
      group: 'schedule:sync',
      status: TaskStatus.pending,
    );
    expect(active.single.nextAttemptAt, t0.add(hour * 2));
  });

  test('a restart keeps the stored occurrence', () async {
    final first = await h.queue.schedule(
      'sync',
      every: hour,
      firstRunAt: t0.add(hour),
      task: () => ValueTask('sync'),
    );
    await h.queue.close();

    final next = QueueHarness(storage: h.storage, clock: h.clock);
    addTearDown(next.queue.close);
    next.register(handler: (task, context) async {});
    final again = await next.queue.schedule(
      'sync',
      every: hour,
      task: () => ValueTask('sync'),
    );
    expect(again, first);
    expect(await next.queue.getTasks(group: 'schedule:sync'), hasLength(1));
  });

  test('after missed slots it runs once, then keeps the slots', () async {
    await h.queue.schedule('sync', every: hour, task: () => ValueTask('sync'));
    await h.queue.start();
    await h.until(() => keys.length == 1);
    await pumpEventQueue();
    await h.queue.close();

    // The app is closed for 5.5 hours.
    h.clock.advance(const Duration(hours: 5, minutes: 30));
    final next = QueueHarness(storage: h.storage, clock: h.clock);
    addTearDown(next.queue.close);
    final ran = <String>[];
    next.register(
      handler: (task, context) async => ran.add(context.idempotencyKey!),
    );
    await next.queue.schedule(
      'sync',
      every: hour,
      task: () => ValueTask('sync'),
    );
    await next.queue.start();
    await next.until(() => ran.length == 1);
    // The 01:00 slot was stored before closing and runs late. 02:00 to
    // 05:00 were missed: 05:00 runs at once, the others not at all.
    await next.until(() => ran.length == 2);
    expect(ran, [key(t0.add(hour)), key(t0.add(hour * 5))]);
    await pumpEventQueue();
    final pending = await next.queue.getTasks(
      group: 'schedule:sync',
      status: TaskStatus.pending,
    );
    expect(pending.single.nextAttemptAt, t0.add(hour * 6));
    expect(ran, hasLength(2));
  });

  test('a crash before the next occurrence was stored is repaired', () async {
    final id = await h.queue.schedule(
      'sync',
      every: hour,
      task: () => ValueTask('sync'),
    );
    await h.queue.close();
    // Simulate: the occurrence completed, the app died before storing the
    // next one.
    final done = (await h.storage.get(id))!;
    await h.storage.update(done.copyWith(status: TaskStatus.completed));

    h.clock.advance(const Duration(minutes: 10));
    final next = QueueHarness(storage: h.storage, clock: h.clock);
    addTearDown(next.queue.close);
    next.register(handler: (task, context) async {});
    final repaired = await next.queue.schedule(
      'sync',
      every: hour,
      task: () => ValueTask('sync'),
    );
    expect(repaired, isNot(id));
    expect((await next.queue.getTask(repaired))!.nextAttemptAt, t0.add(hour));
  });

  test('unschedule cancels the waiting occurrence and stops', () async {
    final id = await h.queue.schedule(
      'sync',
      every: hour,
      firstRunAt: t0.add(hour),
      task: () => ValueTask('sync'),
    );
    expect(await h.queue.unschedule('sync'), isTrue);
    expect(h.queue.schedules, isEmpty);
    expect((await h.queue.getTask(id))!.status, TaskStatus.cancelled);
    await pumpEventQueue();
    expect(
      await h.queue.getTasks(
        group: 'schedule:sync',
        status: TaskStatus.pending,
      ),
      isEmpty,
    );
    expect(await h.queue.unschedule('sync'), isFalse);
  });

  test('unschedule lets a running occurrence finish', () async {
    final gate = <void Function()>[];
    final local = QueueHarness();
    addTearDown(local.queue.close);
    local.register(
      handler: (task, context) {
        final done = Completer<void>();
        gate.add(done.complete);
        return done.future;
      },
    );
    final id = await local.queue.schedule(
      'sync',
      every: hour,
      task: () => ValueTask('sync'),
    );
    await local.queue.start();
    await local.until(() => gate.isNotEmpty);
    expect(await local.queue.unschedule('sync'), isFalse);
    gate.single();
    await local.until(() => local.of<TaskCompleted>().length == 1);
    await pumpEventQueue();
    expect((await local.queue.getTask(id))!.status, TaskStatus.completed);
    expect(await local.queue.getTasks(group: 'schedule:sync'), hasLength(1));
  });

  test('rejects bad arguments', () async {
    expect(
      () => h.queue.schedule(' ', every: hour, task: () => ValueTask('a')),
      throwsA(isA<ArgumentError>()),
    );
    expect(
      () => h.queue.schedule(
        'a',
        every: const Duration(milliseconds: 999),
        task: () => ValueTask('a'),
      ),
      throwsA(isA<ArgumentError>()),
    );
    await h.queue.schedule('a', every: hour, task: () => ValueTask('a'));
    expect(
      () => h.queue.schedule('a', every: hour, task: () => ValueTask('a')),
      throwsA(isA<StateError>()),
    );
  });

  test('an unregistered type is rejected and not kept', () async {
    expect(
      () => h.queue.schedule('x', every: hour, task: RenamedValueTask.new),
      throwsA(isA<UnknownTaskTypeException>()),
    );
    await pumpEventQueue();
    expect(h.queue.schedules, isEmpty);
  });
}
