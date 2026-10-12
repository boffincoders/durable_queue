import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

void main() {
  test('fake clock wakes a delay only when time reaches it', () async {
    final clock = FakeQueueClock(DateTime.utc(2026, 1, 1));
    final delay = clock.delayUntil(clock.now().add(const Duration(seconds: 5)));
    var woke = false;
    unawaited(
      delay.future.then((_) {
        woke = true;
      }),
    );

    await Future<void>.delayed(Duration.zero);
    expect(woke, isFalse);

    clock.advance(const Duration(seconds: 4));
    await Future<void>.delayed(Duration.zero);
    expect(woke, isFalse);

    clock.advance(const Duration(seconds: 1));
    await Future<void>.delayed(Duration.zero);
    expect(woke, isTrue);
    expect(clock.now(), DateTime.utc(2026, 1, 1, 0, 0, 5));
  });

  test('a delay that is already due completes immediately', () async {
    final clock = FakeQueueClock(DateTime.utc(2026, 1, 1));
    final delay = clock.delayUntil(clock.now());
    await delay.future;
  });

  test('cancelling a delay completes it', () async {
    final clock = FakeQueueClock(DateTime.utc(2026, 1, 1));
    final delay = clock.delayUntil(clock.now().add(const Duration(hours: 1)));
    delay.cancel();
    await delay.future;
  });

  test('negative advances are rejected', () {
    final clock = FakeQueueClock(DateTime.utc(2026, 1, 1));
    expect(
      () => clock.advance(const Duration(seconds: -1)),
      throwsArgumentError,
    );
  });

  test(
    'system clock reports the current time and completes past delays',
    () async {
      const clock = SystemQueueClock();
      final delta = clock.now().difference(DateTime.now()).abs();
      expect(delta, lessThan(const Duration(seconds: 2)));

      final delay = clock.delayUntil(
        clock.now().subtract(const Duration(seconds: 1)),
      );
      await delay.future;
    },
  );
}
