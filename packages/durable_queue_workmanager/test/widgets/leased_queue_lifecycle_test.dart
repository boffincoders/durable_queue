// Lifecycle tests. They need the Flutter SDK and run in CI with
// `flutter test`.
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_workmanager/durable_queue_workmanager.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';

import '../support/fakes.dart';

const _order = [
  AppLifecycleState.resumed,
  AppLifecycleState.inactive,
  AppLifecycleState.hidden,
  AppLifecycleState.paused,
];

/// Moves the app lifecycle to [target] one legal step at a time.
Future<void> moveTo(WidgetTester tester, AppLifecycleState target) async {
  final current = tester.binding.lifecycleState;
  void set(AppLifecycleState state) {
    // ignore: invalid_use_of_protected_member
    tester.binding.handleAppLifecycleStateChanged(state);
  }

  if (current == null || !_order.contains(current)) {
    set(target);
  } else {
    var i = _order.indexOf(current);
    final j = _order.indexOf(target);
    while (i != j) {
      i += j > i ? 1 : -1;
      set(_order[i]);
    }
  }
  await settle(tester);
}

Future<void> settle(WidgetTester tester) async {
  for (var i = 0; i < 5; i++) {
    await tester.pump();
  }
}

void main() {
  late LeaseSlot slot;
  late FakeLease lease;
  late DurableQueue queue;
  late LeasedQueueRunner runner;

  setUp(() {
    final clock = FakeQueueClock(DateTime.utc(2026, 10, 22));
    slot = LeaseSlot();
    lease = FakeLease(slot);
    queue = newQueue(clock: clock);
    runner = LeasedQueueRunner(queue, lease, clock: clock);
  });

  testWidgets('runs in the foreground and stops in the background', (
    tester,
  ) async {
    await moveTo(tester, AppLifecycleState.resumed);
    final lifecycle = LeasedQueueLifecycle.attach(runner);
    await settle(tester);
    expect(runner.isRunning, isTrue);
    expect(queue.runState, QueueRunState.running);

    await moveTo(tester, AppLifecycleState.inactive);
    expect(runner.isRunning, isTrue);

    await moveTo(tester, AppLifecycleState.hidden);
    expect(runner.isRunning, isFalse);
    expect(slot.holder, isNull);
    expect(queue.runState, QueueRunState.idle);

    await moveTo(tester, AppLifecycleState.paused);
    expect(runner.isRunning, isFalse);

    await moveTo(tester, AppLifecycleState.resumed);
    expect(runner.isRunning, isTrue);

    await lifecycle.dispose();
    await settle(tester);
    expect(runner.isRunning, isFalse);
    expect(slot.holder, isNull);

    await moveTo(tester, AppLifecycleState.hidden);
    await moveTo(tester, AppLifecycleState.resumed);
    expect(runner.isRunning, isFalse, reason: 'no longer listening');
    await queue.close();
  });

  testWidgets('does not start while hidden', (tester) async {
    await moveTo(tester, AppLifecycleState.hidden);
    final lifecycle = LeasedQueueLifecycle.attach(runner);
    await settle(tester);
    expect(runner.isRunning, isFalse);
    expect(lease.acquireCalls, 0);
    await lifecycle.dispose();
    await moveTo(tester, AppLifecycleState.resumed);
    await queue.close();
  });

  testWidgets('errors go to onError', (tester) async {
    await moveTo(tester, AppLifecycleState.resumed);
    lease.acquireError = StateError('disk');
    final errors = <Object>[];
    final lifecycle = LeasedQueueLifecycle.attach(
      runner,
      onError: (error, _) => errors.add(error),
    );
    await settle(tester);
    expect(errors.single, isA<StateError>());
    await lifecycle.dispose();
    await queue.close();
  });
}
