import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue/testing.dart';
import 'package:test/test.dart';

import 'support/harness.dart';

void main() {
  late MemoryQueueStorage storage;
  late FakeQueueClock clock;

  setUp(() {
    storage = MemoryQueueStorage();
    clock = FakeQueueClock(DateTime.utc(2026, 1, 1));
  });

  QueueHarness open({RateLimit? limit, bool shared = false}) {
    final h = QueueHarness(storage: storage, clock: clock);
    final rateLimit = limit ?? RateLimit(1, per: const Duration(hours: 1));
    if (shared) {
      h.queue.addRateLimit('api', rateLimit);
      h.register();
    } else {
      h.register(rateLimit: rateLimit);
    }
    return h;
  }

  test('a limit used by one queue still holds in the next', () async {
    final first = open();
    await first.queue.enqueue(ValueTask('a'));
    expect((await first.queue.drain()).completed, 1);
    await first.queue.close();

    final second = open();
    await second.queue.enqueue(ValueTask('b'));
    final result = await second.queue.drain();
    expect(result.started, 0);
    expect(result.nextWakeAt, DateTime.utc(2026, 1, 1, 1));

    clock.advance(const Duration(hours: 1));
    expect((await second.queue.drain()).completed, 1);
    await second.queue.close();
  });

  test('a slot reserved for a deferred task is not paid twice', () async {
    final first = open(limit: RateLimit(1, per: const Duration(minutes: 10)));
    await first.queue.enqueue(ValueTask('a'));
    await first.queue.enqueue(ValueTask('b'));
    final result = await first.queue.drain();
    expect(result.completed, 1);
    expect(result.nextWakeAt, DateTime.utc(2026, 1, 1, 0, 10));
    await first.queue.close();

    // b holds the slot at 0:10. The next queue runs it then, not at 0:20.
    clock.advance(const Duration(minutes: 10));
    final second = open(limit: RateLimit(1, per: const Duration(minutes: 10)));
    expect((await second.queue.drain()).completed, 1);
    await second.queue.close();
  });

  test('a hold on a shared limit still holds in the next queue', () async {
    final first = open(shared: true);
    first.queue.holdRateLimit('api', duration: const Duration(minutes: 30));
    // Wait for the hold to be saved.
    await first.queue.getTasks();
    await first.queue.close();

    final second = open(shared: true);
    await second.queue.enqueue(ValueTask('a'));
    final result = await second.queue.drain();
    expect(result.started, 0);
    expect(result.nextWakeAt, DateTime.utc(2026, 1, 1, 0, 30));
    await second.queue.close();
  });

  test('a limit that changed starts empty', () async {
    final first = open();
    await first.queue.enqueue(ValueTask('a'));
    await first.queue.drain();
    await first.queue.close();

    final second = open(limit: RateLimit(2, per: const Duration(hours: 1)));
    await second.queue.enqueue(ValueTask('b'));
    expect((await second.queue.drain()).completed, 1);
    await second.queue.close();
  });

  test('state that cannot be read is ignored', () async {
    await storage.writeState('rate_limits', '{not json');
    final h = open();
    await h.queue.enqueue(ValueTask('a'));
    expect((await h.queue.drain()).completed, 1);
    expect(await storage.readState('rate_limits'), isNotNull);
    await h.queue.close();
  });

  test('without a state storage, limits start empty after a restart', () async {
    final plain = _PlainStorage(storage);
    QueueHarness openPlain() {
      final h = QueueHarness(storage: plain, clock: clock);
      h.register(rateLimit: RateLimit(1, per: const Duration(hours: 1)));
      return h;
    }

    final first = openPlain();
    await first.queue.enqueue(ValueTask('a'));
    await first.queue.drain();
    await first.queue.close();

    final second = openPlain();
    await second.queue.enqueue(ValueTask('b'));
    expect((await second.queue.drain()).completed, 1);
    await second.queue.close();
  });
}

/// A storage without [QueueStateStorage].
final class _PlainStorage implements QueueStorage {
  _PlainStorage(this._inner);

  final QueueStorage _inner;

  @override
  dynamic noSuchMethod(Invocation invocation) => Function.apply(
    _forward(invocation.memberName),
    invocation.positionalArguments,
    invocation.namedArguments,
  );

  Function _forward(Symbol name) => switch (name) {
    #save => _inner.save,
    #get => _inner.get,
    #getPending => _inner.getPending,
    #getByStatus => _inner.getByStatus,
    #getNextReady => _inner.getNextReady,
    #getNextWakeAt => _inner.getNextWakeAt,
    #getMaxSequence => _inner.getMaxSequence,
    #getAll => _inner.getAll,
    #findActiveByDeduplicationKey => _inner.findActiveByDeduplicationKey,
    #getWaitingDependents => _inner.getWaitingDependents,
    #getByGroup => _inner.getByGroup,
    #update => _inner.update,
    #delete => _inner.delete,
    _ => throw NoSuchMethodError.withInvocation(
      this,
      Invocation.method(name, []),
    ),
  };
}
