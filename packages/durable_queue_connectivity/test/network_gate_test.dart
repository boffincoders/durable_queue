import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_connectivity/durable_queue_connectivity.dart';
import 'package:test/test.dart';

/// A network source and probe the test controls.
final class Rig {
  Rig({bool up = true, bool withProbe = false}) : _up = up {
    gate = NetworkGate(
      networkChanges: network.stream,
      checkNetwork: () async {
        checks++;
        return _up;
      },
      probe: withProbe ? _probe : null,
      clock: clock,
    );
    gate.changes.listen(seen.add);
  }

  final clock = FakeQueueClock(DateTime.utc(2026, 10, 19));
  final network = StreamController<bool>();
  final seen = <bool>[];
  late final NetworkGate gate;
  bool _up;
  var checks = 0;
  var probes = 0;

  /// Pending probe answers, completed by the test.
  final answers = <Completer<bool>>[];

  Future<bool> _probe() {
    probes++;
    final answer = Completer<bool>();
    answers.add(answer);
    return answer.future;
  }

  void setNetwork(bool up) {
    _up = up;
    network.add(up);
  }

  void answer(bool reachable) => answers.removeAt(0).complete(reachable);
}

Future<void> settle() => pumpEventQueue();

void main() {
  group('without a probe', () {
    test('follows the network', () async {
      final rig = Rig(up: false);
      expect(rig.gate.isOpen, isFalse);
      await rig.gate.start();
      expect(rig.gate.isOpen, isFalse);
      rig.setNetwork(true);
      await settle();
      expect(rig.gate.isOpen, isTrue);
      expect(rig.gate.hasNetwork, isTrue);
      rig.setNetwork(false);
      await settle();
      expect(rig.seen, [true, false]);
      await rig.gate.dispose();
    });

    test('starts open when the network is up', () async {
      final rig = Rig();
      await rig.gate.start();
      expect(rig.gate.isOpen, isTrue);
      expect(rig.checks, 1);
      // start is idempotent.
      await rig.gate.start();
      expect(rig.checks, 1);
      await rig.gate.dispose();
    });

    test(
      'recheck reads the network again, at most every two seconds',
      () async {
        final rig = Rig();
        await rig.gate.start();
        rig.gate.recheck();
        await settle();
        expect(rig.checks, 2);
        rig.gate.recheck();
        await settle();
        expect(rig.checks, 2);
        rig.clock.advance(const Duration(seconds: 2));
        // The network dropped without an event: recheck notices.
        rig._up = false;
        rig.gate.recheck();
        await settle();
        expect(rig.checks, 3);
        expect(rig.gate.isOpen, isFalse);
        await rig.gate.dispose();
      },
    );

    test('a failing check or stream error keeps the last state', () async {
      var fail = true;
      final network = StreamController<bool>();
      final gate = NetworkGate(
        networkChanges: network.stream,
        checkNetwork: () async => fail ? throw StateError('no') : true,
      );
      await gate.start();
      expect(gate.isOpen, isFalse);
      network.addError(StateError('stream'));
      network.add(true);
      await settle();
      expect(gate.isOpen, isTrue);
      fail = true;
      gate.recheck();
      await settle();
      expect(gate.isOpen, isTrue);
      await gate.dispose();
    });
  });

  group('with a probe', () {
    test('opens only after the probe succeeds', () async {
      final rig = Rig(withProbe: true);
      final started = rig.gate.start();
      await settle();
      expect(rig.probes, 1);
      expect(rig.gate.isOpen, isFalse);
      rig.answer(true);
      await started;
      expect(rig.gate.isOpen, isTrue);
      await rig.gate.dispose();
    });

    test(
      'a failing probe keeps it closed and retries on an interval',
      () async {
        final rig = Rig(withProbe: true);
        final started = rig.gate.start();
        await settle();
        rig.answer(false);
        await started;
        expect(rig.gate.isOpen, isFalse);
        expect(rig.gate.hasNetwork, isTrue);

        rig.clock.advance(const Duration(seconds: 29));
        await settle();
        expect(rig.probes, 1);
        rig.clock.advance(const Duration(seconds: 1));
        await settle();
        expect(rig.probes, 2);
        rig.answer(true);
        await settle();
        expect(rig.gate.isOpen, isTrue);
        // No more probing while open.
        rig.clock.advance(const Duration(minutes: 5));
        await settle();
        expect(rig.probes, 2);
        await rig.gate.dispose();
      },
    );

    test('losing the network closes at once and stops retrying', () async {
      final rig = Rig(withProbe: true);
      final started = rig.gate.start();
      await settle();
      rig.answer(false);
      await started;
      rig.setNetwork(false);
      await settle();
      rig.clock.advance(const Duration(minutes: 5));
      await settle();
      expect(rig.probes, 1);
      expect(rig.gate.isOpen, isFalse);
      await rig.gate.dispose();
    });

    test('recheck probes and closes the gate when the probe fails', () async {
      final rig = Rig(withProbe: true);
      final started = rig.gate.start();
      await settle();
      rig.answer(true);
      await started;

      rig.gate.recheck();
      rig.gate.recheck(); // within minRecheckInterval: ignored
      await settle();
      expect(rig.probes, 2);
      rig.answer(false);
      await settle();
      expect(rig.seen, [true, false]);

      // It recovers through the retry interval.
      rig.clock.advance(const Duration(seconds: 30));
      await settle();
      rig.answer(true);
      await settle();
      expect(rig.seen, [true, false, true]);
      await rig.gate.dispose();
    });

    test('a probe result for an older network state is not used', () async {
      final rig = Rig(withProbe: true);
      final started = rig.gate.start();
      await settle();
      // Network flaps while the first probe is running.
      rig.setNetwork(false);
      rig.setNetwork(true);
      await settle();
      rig.answer(false); // stale answer
      await settle();
      expect(rig.gate.isOpen, isFalse);
      expect(rig.probes, 2);
      rig.answer(true);
      await started;
      await settle();
      expect(rig.gate.isOpen, isTrue);
      await rig.gate.dispose();
    });

    test('a probe that throws counts as unreachable', () async {
      final network = StreamController<bool>();
      final gate = NetworkGate(
        networkChanges: network.stream,
        checkNetwork: () async => true,
        probe: () async => throw StateError('boom'),
      );
      await gate.start();
      expect(gate.isOpen, isFalse);
      await gate.dispose();
    });
  });

  test('validates its intervals', () {
    Stream<bool> none() => const Stream.empty();
    Future<bool> up() async => true;
    expect(
      () => NetworkGate(
        networkChanges: none(),
        checkNetwork: up,
        probeRetryInterval: Duration.zero,
      ),
      throwsArgumentError,
    );
    expect(
      () => NetworkGate(
        networkChanges: none(),
        checkNetwork: up,
        minRecheckInterval: Duration.zero,
      ),
      throwsArgumentError,
    );
  });

  test('dispose stops listening and closes changes', () async {
    var listening = false;
    final network = StreamController<bool>(
      onListen: () => listening = true,
      onCancel: () => listening = false,
    );
    final gate = NetworkGate(
      networkChanges: network.stream,
      checkNetwork: () async => true,
    );
    var done = false;
    gate.changes.listen(null, onDone: () => done = true);
    await gate.start();
    expect(listening, isTrue);
    await gate.dispose();
    expect(listening, isFalse);
    expect(done, isTrue);
    network.add(false);
    await settle();
    expect(gate.isOpen, isTrue);
  });
}
