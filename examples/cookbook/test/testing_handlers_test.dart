// Recipe: test retry behaviour without waiting, with FakeQueueClock.
import 'package:durable_queue/durable_queue.dart';
import 'package:flutter_test/flutter_test.dart';

final class Ping extends DurableTask {
  @override
  String get type => 'ping';

  @override
  Map<String, dynamic> toJson() => {};
}

void main() {
  test('a failed ping is retried after 30 seconds', () async {
    final clock = FakeQueueClock(DateTime.utc(2026, 1, 1));
    final queue = DurableQueue(storage: MemoryQueueStorage(), clock: clock);
    var calls = 0;
    queue.register<Ping>(
      type: 'ping',
      decoder: (_) => Ping(),
      handler: (task, context) async {
        if (++calls == 1) throw StateError('server down');
      },
    );
    final id = await queue.enqueue(
      Ping(),
      retryPolicy: RetryPolicy.fixed(
        maxAttempts: 3,
        delay: const Duration(seconds: 30),
      ),
    );

    // drain runs what is ready and returns; the retry is not due yet.
    var result = await queue.drain();
    expect(calls, 1);
    expect((await queue.getTask(id))!.status, TaskStatus.retryScheduled);
    expect(result.nextWakeAt, clock.now().add(const Duration(seconds: 30)));

    // No real waiting: move the clock.
    clock.advance(const Duration(seconds: 30));
    result = await queue.drain();
    expect(calls, 2);
    expect(result.completed, 1);
    expect((await queue.getTask(id))!.status, TaskStatus.completed);
    await queue.close();
  });
}
