import 'dart:async';

import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:durable_queue_connectivity/durable_queue_connectivity.dart';
import 'package:test/test.dart';

/// Stands in for the platform plugin.
final class FakeConnectivity implements Connectivity {
  FakeConnectivity(this.current);

  List<ConnectivityResult> current;
  final _changes = StreamController<List<ConnectivityResult>>.broadcast();

  void emit(List<ConnectivityResult> results) {
    current = results;
    _changes.add(results);
  }

  @override
  Stream<List<ConnectivityResult>> get onConnectivityChanged => _changes.stream;

  @override
  Future<List<ConnectivityResult>> checkConnectivity() async => current;
}

void main() {
  test('hasUsableConnection', () {
    const none = [ConnectivityResult.none];
    const wifi = [ConnectivityResult.wifi];
    const mobileVpn = [ConnectivityResult.mobile, ConnectivityResult.vpn];
    expect(ConnectivityGate.hasUsableConnection(none), isFalse);
    expect(ConnectivityGate.hasUsableConnection(const []), isFalse);
    expect(ConnectivityGate.hasUsableConnection(wifi), isTrue);
    expect(ConnectivityGate.hasUsableConnection(mobileVpn), isTrue);
    const wifiOnly = {ConnectivityResult.wifi, ConnectivityResult.ethernet};
    expect(
      ConnectivityGate.hasUsableConnection(mobileVpn, accept: wifiOnly),
      isFalse,
    );
    expect(
      ConnectivityGate.hasUsableConnection([
        ConnectivityResult.mobile,
        ConnectivityResult.wifi,
      ], accept: wifiOnly),
      isTrue,
    );
  });

  test('create starts from the current state and follows changes', () async {
    final fake = FakeConnectivity([ConnectivityResult.none]);
    final gate = await ConnectivityGate.create(connectivity: fake);
    final seen = <bool>[];
    gate.changes.listen(seen.add);
    expect(gate.isOpen, isFalse);

    fake.emit([ConnectivityResult.mobile]);
    await pumpEventQueue();
    // Same usable state again: no duplicate event.
    fake.emit([ConnectivityResult.mobile, ConnectivityResult.vpn]);
    await pumpEventQueue();
    fake.emit([ConnectivityResult.none]);
    await pumpEventQueue();
    expect(seen, [true, false]);
    await gate.dispose();
  });

  test('accept limits the connection types', () async {
    final fake = FakeConnectivity([ConnectivityResult.mobile]);
    final gate = await ConnectivityGate.create(
      connectivity: fake,
      accept: {ConnectivityResult.wifi},
    );
    expect(gate.isOpen, isFalse);
    fake.emit([ConnectivityResult.wifi]);
    await pumpEventQueue();
    expect(gate.isOpen, isTrue);
    await gate.dispose();
  });

  test('a probe must pass before the gate opens', () async {
    var reachable = false;
    final fake = FakeConnectivity([ConnectivityResult.wifi]);
    final gate = await ConnectivityGate.create(
      connectivity: fake,
      probe: () async => reachable,
    );
    expect(gate.isOpen, isFalse);
    expect(gate.hasNetwork, isTrue);
    reachable = true;
    fake.emit([ConnectivityResult.none]);
    fake.emit([ConnectivityResult.wifi]);
    await pumpEventQueue();
    expect(gate.isOpen, isTrue);
    await gate.dispose();
  });

  test('a change of connection type alone does not probe again', () async {
    var probes = 0;
    final fake = FakeConnectivity([ConnectivityResult.wifi]);
    final gate = await ConnectivityGate.create(
      connectivity: fake,
      probe: () async {
        probes++;
        return true;
      },
    );
    expect(probes, 1);
    fake.emit([ConnectivityResult.wifi, ConnectivityResult.vpn]);
    fake.emit([ConnectivityResult.mobile]);
    await pumpEventQueue();
    expect(probes, 1);
    fake.emit([ConnectivityResult.none]);
    fake.emit([ConnectivityResult.mobile]);
    await pumpEventQueue();
    expect(probes, 2);
    await gate.dispose();
  });

  test('rejects an accept set that cannot open the gate', () async {
    final fake = FakeConnectivity([ConnectivityResult.wifi]);
    await expectLater(
      ConnectivityGate.create(connectivity: fake, accept: {}),
      throwsArgumentError,
    );
    await expectLater(
      ConnectivityGate.create(
        connectivity: fake,
        accept: {ConnectivityResult.none},
      ),
      throwsArgumentError,
    );
  });
}
